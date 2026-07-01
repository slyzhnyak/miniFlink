(* ============================================================
   Checkpoint_parallel.ml — exactly-once в параллельном режиме

   Интегрирует Chandy-Lamport barrier (barrier_default) в
   data-parallel выполнение. Это roadmap-пункт 1.1.

   Схема:
     dispatcher → [in_chan.0 .. in_chan.N] → workers → sink
                       ↑ barrier инжектируется сюда

   Каждый воркер владеет своим state_backend. Когда по его
   входному каналу приходит Barrier(epoch):
     1. воркер снапшотит свой backend (snapshot : bytes)
     2. отправляет (worker_id, epoch, snapshot) координатору
     3. вызывает worker_ready и продолжает обработку
   Координатор ждёт снапшоты от ВСЕХ воркеров (barrier alignment —
   у нас один вход на воркер, поэтому alignment тривиален), затем
   атомарно коммитит checkpoint(epoch) = массив снапшотов.

   Гарантия: checkpoint(epoch) — согласованный срез. Все события
   до barrier учтены в снапшоте, все после — нет. При восстановлении
   каждый воркер получает свой снапшот обратно.

   Сообщение в канале: Event либо Barrier. Ядро Mf_event не тронуто —
   обёртка живёт только здесь.
   ============================================================ *)

type epoch = int

(* Выполнить [f] под mutex'ом, гарантированно разблокировав даже если
   [f] бросит исключение. Критично для регионов вызывающих
   пользовательский код (cs_persist, publish) — без этого исключение
   в колбэке оставило бы mutex заблокированным навсегда (deadlock). *)
let with_mutex mu f =
  Mutex.lock mu;
  match f () with
  | r -> Mutex.unlock mu; r
  | exception e -> Mutex.unlock mu; raise e

(** Позиция чтения в источнике. Для Kafka — offset партиции,
    для списка — индекс, для файла — байтовое смещение. *)
type offset = int

(** Адресуемый (seekable) источник: помимо чтения событий умеет
    сообщать текущую позицию и перематываться на сохранённую.
    Это то что делает возможным exactly-once на входной стороне —
    после сбоя мы перематываем источник на offset последнего
    закоммиченного checkpoint и переобрабатываем без пропусков. *)
type 'a seekable_source = {
  pull        : unit -> 'a Mf_event.t option;  (* прочитать следующее событие *)
  position    : unit -> offset;                (* текущая позиция чтения *)
  seek        : offset -> unit;                (* перемотать на позицию *)
}

(** Обернуть обычный список в seekable source (для тестов и in-memory). *)
let seekable_of_list (events : 'a Mf_event.t list) : 'a seekable_source =
  let arr = Array.of_list events in
  let pos = ref 0 in
  { pull = (fun () ->
      if !pos >= Array.length arr then None
      else (let e = arr.(!pos) in incr pos; Some e));
    position = (fun () -> !pos);
    seek = (fun o -> pos := o) }

type 'a msg =
  | Event   of 'a Mf_event.t
  | Barrier of epoch

(* Снапшот одного воркера в данном epoch *)
type worker_snapshot = {
  worker    : int;
  epoch     : epoch;
  state     : bytes;
  processed : int;     (* событий обработано воркером к моменту barrier *)
}

(** Полная запись checkpoint: epoch, позиция источника на момент
    barrier, и снапшоты всех воркеров. Source offset — ключевое
    добавление: без него восстановление не знает откуда читать. *)
type checkpoint = {
  cp_epoch     : epoch;
  cp_offset    : offset;                  (* позиция источника на barrier *)
  cp_snapshots : worker_snapshot array;   (* стейт каждого воркера *)
}

(* Хранилище checkpoint-ов *)
type checkpoint_store = {
  mutable committed : checkpoint list;
  cs_mu : Mutex.t;
  cs_persist : (checkpoint -> unit) option;  (* durable-запись, опционально *)
}

let make_store ?persist () =
  { committed = []; cs_mu = Mutex.create (); cs_persist = persist }

let commit store cp =
  with_mutex store.cs_mu (fun () ->
    store.committed <- cp :: store.committed;
    (match store.cs_persist with Some f -> f cp | None -> ()))

let latest_checkpoint store =
  with_mutex store.cs_mu (fun () ->
    match store.committed with [] -> None | x :: _ -> Some x)

let checkpoint_count store =
  with_mutex store.cs_mu (fun () -> List.length store.committed)

(* ── Координатор сбора снапшотов ──────────────────────────── *)

type coordinator = {
  workers   : int;
  store     : checkpoint_store;
  pending   : worker_snapshot option array;
  alive     : bool array;        (* воркер ещё жив? упавших не ждём *)
  co_mu     : Mutex.t;
  co_done   : Condition.t;
  data_positions : (int, offset) Hashtbl.t;
    (* data_count -> позиция в потоке СРАЗУ ПОСЛЕ этого по счёту Data.
       [data_positions.(k)] = source.position() в момент, когда
       координатор разослал k-е Data-событие. cp_offset для чекпойнта
       со снапшотом из N обработанных Data берётся как
       data_positions.(N): это позиция в полном потоке (Data+WM+...),
       соответствующая ровно N обработанным Data, поэтому offset
       СОГЛАСОВАН со снапшотом независимо от того, сколько событий было
       in-flight в момент инжекта барьера (см. §4.1). *)
}

let make_coordinator ~workers ~store = {
  workers;
  store;
  pending = Array.make workers None;
  alive   = Array.make workers true;
  co_mu   = Mutex.create ();
  co_done = Condition.create ();
  data_positions = Hashtbl.create 16;
}

(* Пометить воркера выбывшим (после краха) — определено ниже, после
   try_close, т.к. использует его. *)

(* Воркер сообщает свой снапшот. Когда собраны все N — commit
   вместе с зафиксированным offset источника. *)
(* Закрыть checkpoint если снапшоты пришли от всех ЖИВЫХ воркеров.
   Вызывается под co_mu из submit_snapshot и mark_failed. epoch берём
   из любого присланного снапшота. *)
let try_close c =
  let idxs = Array.init c.workers (fun i -> i) in
  let all_alive_ready =
    Array.for_all (fun i -> not c.alive.(i) || Option.is_some c.pending.(i)) idxs
    && Array.exists (fun i -> c.alive.(i) && Option.is_some c.pending.(i)) idxs in
  if all_alive_ready then begin
    let snapshots =
      Array.to_list c.pending
      |> List.filteri (fun i _ -> c.alive.(i))
      |> List.filter_map (fun x -> x)
      |> Array.of_list in
    let epoch = snapshots.(0).epoch in
    (* snap_processed — число Data, реально вошедших в снапшот (сумма
       processed по воркерам). cp_offset = позиция в потоке СРАЗУ ПОСЛЕ
       snap_processed-го Data (из data_positions). Это согласует offset
       со снапшотом: seek(cp_offset) при recovery встанет ровно туда,
       где обработано snap_processed Data, и replay доиграет остаток без
       потерь и дублей — независимо от того, сколько событий было
       in-flight в момент инжекта барьера и есть ли watermark-и между
       Data (см. §4.1). *)
    let snap_processed =
      Array.fold_left (fun a s -> a + s.processed) 0 snapshots in
    let cp_offset =
      if snap_processed = 0 then 0
      else match Hashtbl.find_opt c.data_positions snap_processed with
        | Some off -> off
        | None -> snap_processed  (* подстраховка (не должно случаться) *)
    in
    (* Очистка: записи для data_count < snap_processed больше не нужны.
       Следующий чекпойнт будет иметь snap_processed не меньше текущего
       (число обработанных Data монотонно растёт), а recover читает
       только latest_checkpoint. Без очистки data_positions рос бы на
       каждое Data — утечка на long-running прогоне (4096 ламп). *)
    Hashtbl.filter_map_inplace
      (fun k v -> if k < snap_processed then None else Some v)
      c.data_positions;
    commit c.store { cp_epoch = epoch;
                     cp_offset;
                     cp_snapshots = snapshots };
    Array.fill c.pending 0 c.workers None;
    Condition.broadcast c.co_done
  end

let submit_snapshot c (snap : worker_snapshot) =
  with_mutex c.co_mu (fun () ->
    c.pending.(snap.worker) <- Some snap;
    try_close c)

(* Пометить воркера выбывшим (после краха). Координатор перестаёт ждать
   его снапшот; если он был последним недостающим — текущий checkpoint
   закрывается по оставшимся живым. *)
let mark_failed c worker =
  with_mutex c.co_mu (fun () ->
    if worker < c.workers then c.alive.(worker) <- false;
    c.pending.(worker) <- None;
    try_close c)

(* Ждать коммита данного epoch (для теста/синхронизации) *)
let wait_committed c ~epoch =
  Mutex.lock c.co_mu;
  while checkpoint_count c.store = 0
        || (Option.get (latest_checkpoint c.store)).cp_epoch < epoch do
    Condition.wait c.co_done c.co_mu
  done;
  Mutex.unlock c.co_mu

(* ── Hash-шардирование (как в parallel) ──────────────────── *)

(* Шардирование по ключу — общий djb2 из модуля Hash (см. 4.6: раньше
   эта функция дублировалась здесь и в parallel_v4/v5). Оставлена как
   тонкий алиас, т.к. входит в публичный API (используется в тестах). *)
let hash_key = Hash.key

(* ── Транзакционный sink (2PC поверх barrier) ─────────────── *)

(** Sink с поддержкой two-phase commit, привязанного к checkpoint.
    Между barrier результаты пишутся в pre-commit (невидимы наружу);
    когда checkpoint зафиксирован — [commit] делает их видимыми;
    при сбое до commit — [abort] откатывает.

    Это замыкает exactly-once на выходной стороне: переобработка
    после восстановления не создаёт дублей, потому что незакоммиченные
    результаты откатываются.

    Для sink не умеющих транзакции используйте идемпотентный путь:
    {!idempotent_sink} — детерминированный ключ + upsert. *)
type 'b transactional_sink = {
  ts_write   : epoch -> 'b -> unit;    (* записать в pre-commit для epoch *)
  ts_commit  : epoch -> unit;          (* сделать видимым (checkpoint прошёл) *)
  ts_abort   : epoch -> unit;          (* откатить (сбой до commit) *)
  ts_flush   : unit -> unit;           (* опубликовать всё оставшееся при штатном завершении *)
}

(** Идемпотентный sink: оборачивает обычную upsert-функцию.
    Commit/abort — no-op, потому что повторная запись по тому же
    детерминированному ключу безвредна (upsert перезапишет тем же).
    Самый дешёвый путь к корректному выходу, работает для любого
    sink с ключевой записью. *)
let idempotent_sink (upsert : 'b -> unit) : 'b transactional_sink = {
  ts_write  = (fun _epoch v -> upsert v);
  ts_commit = (fun _ -> ());
  ts_abort  = (fun _ -> ());
  ts_flush  = (fun () -> ());
}

(** Буферизующий транзакционный sink: накапливает результаты epoch,
    публикует пачкой на commit, отбрасывает на abort. Эмулирует 2PC
    для sink без нативных транзакций (in-memory pre-commit буфер). *)
let buffered_sink (publish : 'b list -> unit) : 'b transactional_sink =
  let buffers : (epoch, 'b list ref) Hashtbl.t = Hashtbl.create 8 in
  let mu = Mutex.create () in
  let buf epoch =
    match Hashtbl.find_opt buffers epoch with
    | Some r -> r
    | None -> let r = ref [] in Hashtbl.replace buffers epoch r; r in
  { ts_write = (fun epoch v ->
      with_mutex mu (fun () -> let r = buf epoch in r := v :: !r));
    ts_commit = (fun epoch ->
      with_mutex mu (fun () ->
        match Hashtbl.find_opt buffers epoch with
        | Some r -> publish (List.rev !r); Hashtbl.remove buffers epoch
        | None -> ()));
    ts_abort = (fun epoch ->
      (* R7 (внешний bug-report) — НЕ баг: abort семантически верен.
         Между barrier'ами результаты epoch'и копятся в buffers и
         наружу НЕ публикуются (publish зовётся только в ts_commit).
         abort просто выбрасывает незакоммиченный буфер этой epoch'и —
         поэтому при переобработке после сбоя они не доедут до publish
         (exactly-once на выходной стороне). Удаление буфера ДРУГОЙ,
         уже закоммиченной epoch'и невозможно: ts_commit удаляет свой
         буфер сразу после publish, так что abort не может «отменить»
         опубликованное. См. test/test_2pc_sink_abort.ml. *)
      with_mutex mu (fun () -> Hashtbl.remove buffers epoch));
    ts_flush = (fun () ->
      (* Штатное завершение: опубликовать все оставшиеся epoch по порядку.
         Это хвостовые события после последнего checkpoint — не сбой. *)
      with_mutex mu (fun () ->
        let epochs = Hashtbl.fold (fun e _ acc -> e :: acc) buffers [] in
        List.iter (fun e ->
          match Hashtbl.find_opt buffers e with
          | Some r -> publish (List.rev !r); Hashtbl.remove buffers e
          | None -> ()) (List.sort compare epochs))) }

(* ── Параллельный прогон с checkpoint + offset + 2PC ──────── *)

(**
   [run_exactly_once] — data-parallel выполнение с полным end-to-end
   exactly-once:
   - source offset фиксируется в каждом checkpoint (входная сторона);
   - транзакционный sink коммитит результаты epoch только после того
     как checkpoint этого epoch зафиксирован (выходная сторона).

   Параметры:
   - [workers], [capacity], [checkpoint_every] — как раньше;
   - [key_of] — ключ шардирования;
   - [make_state] — создать backend воркера (вызывается N раз);
   - [process backend epoch ev] — обработать событие, вернуть выходы;
   - [source] — {!seekable_source} (умеет position/seek);
   - [sink] — {!transactional_sink};
   - [store] — куда коммитить checkpoint-ы.

   Воркер пишет выходы через [ts_write epoch]. Когда barrier(epoch)
   собран со всех воркеров и checkpoint зафиксирован, координатор
   вызывает [ts_commit epoch] — результаты становятся видимыми
   атомарно вместе с фиксацией стейта и offset. *)
let run_exactly_once
    ~(workers : int)
    ~(capacity : int)
    ~(checkpoint_every : int)
    ~(key_of : 'a -> string)
    ~(make_state : unit -> State_backend_memory.t)
    ~(process : State_backend_memory.t -> 'a Mf_event.t -> 'b list)
    ~(source : 'a seekable_source)
    ~(sink : 'b transactional_sink)
    ~(store : checkpoint_store)
    () =

  let in_chans = Array.init workers (fun _ -> Channel.make_bounded capacity) in
  let coord = make_coordinator ~workers ~store in
  (* failed[i] пишется из воркера i и читается drive-потоком (dispatcher).
     На OCaml 5 это разные домены без общего lock → обычный bool array дал
     бы data race (UB): dispatcher мог бы читать устаревшее значение и
     слать события/барьеры мёртвому воркеру, теряя их. Atomic даёт
     happens-before и корректную видимость (как current_epoch рядом). *)
  let failed = Array.init workers (fun _ -> Atomic.make false) in

  (* Текущий epoch виден воркерам для маршрутизации выходов в pre-commit.
     Atomic чтобы воркеры и dispatcher видели согласованно. *)
  let current_epoch = Atomic.make 0 in

  (* Коммит/abort транзакционного sink при фиксации checkpoint.
     Координатор зовёт это когда checkpoint(epoch) собран. *)
  let on_committed epoch = sink.ts_commit epoch in

  (* ── Воркеры ──────────────────────────────────────────── *)
  let worker_threads = Array.init workers (fun i ->
    Thread.create (fun () ->
      (try
         let backend = make_state () in
         let processed = ref 0 in
         let rec loop () =
           match Channel.pop in_chans.(i) with
           | None -> ()
           | Some (Event ev) ->
             let e = Atomic.get current_epoch in
             let outs = process backend ev in
             (match ev with Mf_event.Data _ -> incr processed | _ -> ());
             List.iter (fun o -> sink.ts_write e o) outs;
             loop ()
           | Some (Barrier epoch) ->
             let state = State_backend_memory.snapshot backend in
             submit_snapshot coord
               { worker = i; epoch; state; processed = !processed };
             loop ()
         in loop ()
       with e ->
         Atomic.set failed.(i) true;
         (* сообщаем координатору что воркер выбыл — иначе он будет
            ждать снапшот мёртвого и checkpoint залипнет (R1) *)
         mark_failed coord i;
         Channel.close in_chans.(i);
         (* Откатываем незакоммиченные выходы текущего epoch этого воркера *)
         sink.ts_abort (Atomic.get current_epoch);
         Log.error ~fields:[("worker", string_of_int i);
                            ("error", Printexc.to_string e)]
           "checkpoint worker crashed")
    ) ()
  ) in

  (* Поток-коммиттер: ждёт фиксации каждого epoch и коммитит sink.
     Развязывает submit_snapshot (под co_mu) от пользовательского
     ts_commit чтобы не держать мьютекс координатора. *)
  let committed_upto = ref 0 in
  let committer_stop = ref false in
  (* committer_stop разрывает ожидание ниже даже когда НИ ОДИН
     checkpoint не зафиксирован — это и закрывает сценарий внешнего
     R2 («all workers die before barrier»). Если все воркеры упали до
     barrier'а, latest_checkpoint так и останется пустым, но финальная
     секция run_exactly_once ставит committer_stop := true и делает
     broadcast co_done; цикл while завершается по `not committer_stop`,
     committer доделывает пустой for-коммит и Thread.join возвращается.
     Поэтому R2 — НЕ дедлок; см. test/test_checkpoint_all_die.ml. *)
  let committer = Thread.create (fun () ->
    let rec loop () =
      Mutex.lock coord.co_mu;
      while (checkpoint_count store = 0
             || (Option.get (latest_checkpoint store)).cp_epoch <= !committed_upto)
            && not !committer_stop do
        Condition.wait coord.co_done coord.co_mu
      done;
      let latest = if checkpoint_count store > 0
        then (Option.get (latest_checkpoint store)).cp_epoch else !committed_upto in
      Mutex.unlock coord.co_mu;
      (* Коммитим все epoch от committed_upto+1 до latest *)
      for e = !committed_upto + 1 to latest do on_committed e done;
      committed_upto := latest;
      if not !committer_stop then loop ()
    in loop ()
  ) () in

  (* ── Dispatcher: шардирует + инжектирует barrier с offset ── *)
  let epoch = ref 0 in
  let since_checkpoint = ref 0 in
  let inject_barrier () =
    incr epoch;
    let e = !epoch in
    Atomic.set current_epoch e;
    Array.iteri (fun i ch ->
      if not (Atomic.get failed.(i)) then ignore (Channel.try_push ch (Barrier e))) in_chans
  in
  (* счётчик разосланных Data и запись data_count -> позиция в потоке.
     Ведём здесь (главный drive-поток), читаем в try_close под co_mu. *)
  let data_count = ref 0 in
  let record_data_position () =
    incr data_count;
    Mutex.lock coord.co_mu;
    Hashtbl.replace coord.data_positions !data_count (source.position ());
    Mutex.unlock coord.co_mu
  in
  (* Блокирующая отправка живому воркеру: для exactly-once нельзя
     терять события (try_push роняет при полном канале). push даёт
     backpressure. Проверка failed чтобы не зависнуть на упавшем. *)
  let send_to i ch msg =
    if not (Atomic.get failed.(i)) then
      if Channel.try_push ch msg then ()
      else if not (Atomic.get failed.(i)) then Channel.push ch msg  (* блокируем = backpressure *)
  in
  let rec drive () =
    match source.pull () with
    | None -> ()
    | Some ev ->
      (match ev with
       | Mf_event.Data (v, _) ->
         let shard = hash_key (key_of v) workers in
         send_to shard in_chans.(shard) (Event ev);
         record_data_position ();
         incr since_checkpoint;
         if !since_checkpoint >= checkpoint_every then begin
           inject_barrier (); since_checkpoint := 0
         end
       | Mf_event.Watermark _ ->
         Array.iteri (fun i ch -> send_to i ch (Event ev)) in_chans
       | Mf_event.Retract (v, _) ->
         (* ретракт шардируем по ключу как Data — иначе он терялся бы
            (R2) и retract-семантика в EO-пути расходилась бы с обычным
            Pipe-путём *)
         let shard = hash_key (key_of v) workers in
         send_to shard in_chans.(shard) (Event ev)
       | Mf_event.Update { new_value = v; _ } ->
         (* Update шардируем по ключу new_value, как Data *)
         let shard = hash_key (key_of v) workers in
         send_to shard in_chans.(shard) (Event ev);
         record_data_position ();
         incr since_checkpoint;
         if !since_checkpoint >= checkpoint_every then begin
           inject_barrier (); since_checkpoint := 0
         end);
      drive ()
  in
  drive ();

  (* Финальный barrier — закоммитить последний срез *)
  inject_barrier ();

  Array.iter Channel.close in_chans;
  Array.iter Thread.join worker_threads;

  (* Дождаться коммита всех epoch, остановить committer *)
  Mutex.lock coord.co_mu;
  committer_stop := true;
  Condition.broadcast coord.co_done;
  Mutex.unlock coord.co_mu;
  Thread.join committer;
  (* Финальный коммит на случай если committer не успел последний epoch *)
  let final = if checkpoint_count store > 0
    then (Option.get (latest_checkpoint store)).cp_epoch else 0 in
  for e = !committed_upto + 1 to final do on_committed e done;
  (* Штатное завершение: опубликовать хвост (события после последнего
     checkpoint). Для идемпотентного sink — no-op. *)
  sink.ts_flush ()

(* ── Восстановление ───────────────────────────────────────── *)

(** Раздать снапшоты последнего checkpoint воркерам.
    [backends.(i)] восстанавливается из snapshot воркера [i].
    Возвращает offset с которого нужно перечитывать источник,
    либо [None] если checkpoint-ов нет. *)
let restore_latest store (backends : State_backend_memory.t array) : offset option =
  match latest_checkpoint store with
  | None -> None
  | Some cp ->
    Array.iter (fun snap ->
      if snap.worker < Array.length backends then
        State_backend_memory.restore backends.(snap.worker) snap.state
    ) cp.cp_snapshots;
    Some cp.cp_offset

(** Протокол холодного старта после сбоя.

    Единый путь восстановления:
    1. прочитать последний валидный checkpoint из store;
    2. восстановить стейт всех воркеров из снапшотов;
    3. перемотать источник на сохранённый offset;
    4. вернуть [backends] готовые к продолжению обработки.

    Если checkpoint-ов нет (первый запуск) — backends пустые,
    источник остаётся в начале. *)
let recover ~workers ~make_state ~(source : 'a seekable_source) store
    : State_backend_memory.t array =
  let backends = Array.init workers (fun _ -> make_state ()) in
  (match restore_latest store backends with
   | Some off ->
     source.seek off;
     Log.info ~fields:[
       ("epoch", string_of_int (match latest_checkpoint store with Some c -> c.cp_epoch | None -> 0));
       ("offset", string_of_int off)]
       "recovered from checkpoint, source seeked"
   | None ->
     Log.info "no checkpoint, cold start from offset 0");
  backends

(* ── Durable checkpoint storage ──────────────────────────── *)

(** Сериализация checkpoint в bytes (Marshal). worker_snapshot.state
    уже bytes, поэтому Marshal записи безопасен. *)
let serialize_checkpoint (cp : checkpoint) : bytes =
  Marshal.to_bytes cp []

let deserialize_checkpoint (b : bytes) : checkpoint =
  Marshal.from_bytes b 0

(* Открыть файл, выполнить [f] над каналом, ГАРАНТИРОВАННО закрыть
   даже если [f] бросит. Без этого исключение при записи (диск полон,
   IO error) или при чтении (битый файл) оставило бы file descriptor
   открытым — на persist, который вызывается на каждом checkpoint, это
   привело бы к исчерпанию дескрипторов. *)
let with_out_channel open_fn path f =
  let oc = open_fn path in
  match f oc with
  | r -> close_out oc; r
  | exception e -> (try close_out oc with _ -> ()); raise e

let with_in_channel open_fn path f =
  let ic = open_fn path in
  match f ic with
  | r -> close_in ic; r
  | exception e -> (try close_in ic with _ -> ()); raise e

(** Создать store с durable-записью на диск. Каждый коммит пишет
    checkpoint в файл [dir/checkpoint_<epoch>.cp] плюс обновляет
    [dir/LATEST] с номером последнего epoch. Переживает рестарт
    процесса и (если dir на сетевом томе) смерть локального диска. *)
let durable_store ~dir : checkpoint_store =
  (try Unix.mkdir dir 0o755
   with Unix.Unix_error (Unix.EEXIST,_,_) -> () | _ -> ());
  let persist cp =
    let path = Printf.sprintf "%s/checkpoint_%d.cp" dir cp.cp_epoch in
    with_out_channel open_out_bin path (fun oc ->
      output_bytes oc (serialize_checkpoint cp));
    (* атомарное обновление указателя LATEST через rename *)
    let tmp = Printf.sprintf "%s/LATEST.tmp" dir in
    with_out_channel open_out tmp (fun oc ->
      output_string oc (string_of_int cp.cp_epoch));
    Sys.rename tmp (Printf.sprintf "%s/LATEST" dir)
  in
  make_store ~persist ()

(** Загрузить последний durable checkpoint с диска в новый store
    (после рестарта процесса). Читает [dir/LATEST], затем
    соответствующий [checkpoint_<epoch>.cp]. *)
let load_durable ~dir : checkpoint_store =
  let store = durable_store ~dir in
  (try
     let epoch =
       with_in_channel open_in (Printf.sprintf "%s/LATEST" dir)
         (fun ic -> int_of_string (input_line ic)) in
     let path = Printf.sprintf "%s/checkpoint_%d.cp" dir epoch in
     let cp =
       with_in_channel open_in_bin path (fun ic ->
         let len = in_channel_length ic in
         let b = Bytes.create len in
         really_input ic b 0 len;
         deserialize_checkpoint b) in
     (* кладём напрямую в committed, минуя persist (он уже на диске) *)
     store.committed <- [cp]
   with _ -> ());   (* нет LATEST — пустой store, холодный старт *)
  store
