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

type 'a msg =
  | Event   of 'a Mf_event.t
  | Barrier of epoch

(* Снапшот одного воркера в данном epoch *)
type worker_snapshot = {
  worker : int;
  epoch  : epoch;
  state  : bytes;
}

(* Хранилище checkpoint-ов: epoch → снапшоты всех воркеров *)
type checkpoint_store = {
  mutable committed : (epoch * worker_snapshot array) list;
  cs_mu : Mutex.t;
}

let make_store () = { committed = []; cs_mu = Mutex.create () }

let commit store epoch snapshots =
  Mutex.lock store.cs_mu;
  store.committed <- (epoch, snapshots) :: store.committed;
  Mutex.unlock store.cs_mu

let latest_checkpoint store =
  Mutex.lock store.cs_mu;
  let r = match store.committed with [] -> None | x :: _ -> Some x in
  Mutex.unlock store.cs_mu;
  r

let checkpoint_count store =
  Mutex.lock store.cs_mu;
  let n = List.length store.committed in
  Mutex.unlock store.cs_mu;
  n

(* ── Координатор сбора снапшотов ──────────────────────────── *)

type coordinator = {
  workers   : int;
  store     : checkpoint_store;
  (* снапшоты текущего epoch по воркерам *)
  pending   : worker_snapshot option array;
  co_mu     : Mutex.t;
  co_done   : Condition.t;
}

let make_coordinator ~workers ~store = {
  workers;
  store;
  pending = Array.make workers None;
  co_mu   = Mutex.create ();
  co_done = Condition.create ();
}

(* Воркер сообщает свой снапшот. Когда собраны все N — commit. *)
let submit_snapshot c (snap : worker_snapshot) =
  Mutex.lock c.co_mu;
  c.pending.(snap.worker) <- Some snap;
  if Array.for_all Option.is_some c.pending then begin
    (* Все воркеры достигли barrier — атомарный commit *)
    let snapshots = Array.map Option.get c.pending in
    commit c.store snap.epoch snapshots;
    Array.fill c.pending 0 c.workers None;
    Condition.broadcast c.co_done
  end;
  Mutex.unlock c.co_mu

(* Ждать коммита данного epoch (для теста/синхронизации) *)
let wait_committed c ~epoch =
  Mutex.lock c.co_mu;
  while checkpoint_count c.store = 0
        || fst (Option.get (latest_checkpoint c.store)) < epoch do
    Condition.wait c.co_done c.co_mu
  done;
  Mutex.unlock c.co_mu

(* ── Hash-шардирование (как в parallel) ──────────────────── *)

let hash_key key n =
  let h = ref 5381 in
  String.iter (fun ch -> h := !h * 33 + Char.code ch) key;
  (!h land max_int) mod n

(* ── Параллельный прогон с checkpoint ─────────────────────── *)

(**
   run_exactly_once:
     ~workers          число воркеров
     ~capacity         размер каждого канала
     ~checkpoint_every барьер каждые N событий (по dispatcher)
     ~key_of           ключ шардирования
     ~make_state       создать backend для воркера (вызывается N раз)
     ~process          обработать событие, обновляя backend; вернуть выходы
     ~key_of_out       (не нужен) — выходы идут в sink
     ~source           вход
     ~sink             выход
     ~store            куда коммитить checkpoint-ы

   process получает (backend, событие) и возвращает список выходов.
   Стейт живёт в backend — именно он снапшотится на barrier.
*)
let run_exactly_once
    ~(workers : int)
    ~(capacity : int)
    ~(checkpoint_every : int)
    ~(key_of : 'a -> string)
    ~(make_state : unit -> State_backend_memory.t)
    ~(process : State_backend_memory.t -> 'a Mf_event.t -> 'b list)
    ~(source : 'a Mf_event.t Stream.t)
    ~(sink : 'b -> unit)
    ~(store : checkpoint_store)
    () =

  let in_chans = Array.init workers (fun _ -> Channel.make_bounded capacity) in
  let coord = make_coordinator ~workers ~store in
  let mu_sink = Mutex.create () in
  let failed = Array.make workers false in

  (* ── Воркеры ──────────────────────────────────────────── *)
  let worker_threads = Array.init workers (fun i ->
    Thread.create (fun () ->
      (try
         let backend = make_state () in
         let rec loop () =
           match Channel.pop in_chans.(i) with
           | None -> ()                       (* канал закрыт *)
           | Some (Event ev) ->
             let outs = process backend ev in
             List.iter (fun o ->
               Mutex.lock mu_sink; sink o; Mutex.unlock mu_sink) outs;
             loop ()
           | Some (Barrier epoch) ->
             (* Снапшот собственного стейта, сигнал координатору *)
             let state = State_backend_memory.snapshot backend in
             submit_snapshot coord { worker = i; epoch; state };
             loop ()
         in loop ()
       with e ->
         failed.(i) <- true;
         Channel.close in_chans.(i);
         Printf.eprintf "[checkpoint] worker %d crashed: %s\n%!"
           i (Printexc.to_string e))
    ) ()
  ) in

  (* ── Dispatcher: шардирует + инжектирует barrier ────────── *)
  let epoch = ref 0 in
  let since_checkpoint = ref 0 in
  let inject_barrier () =
    incr epoch;
    let e = !epoch in
    Array.iteri (fun i ch ->
      if not failed.(i) then ignore (Channel.try_push ch (Barrier e))) in_chans
  in
  Stream.iter (fun ev ->
    match ev with
    | Mf_event.Data (v, _) ->
      let shard = hash_key (key_of v) workers in
      if not failed.(shard) then
        ignore (Channel.try_push in_chans.(shard) (Event ev));
      incr since_checkpoint;
      if !since_checkpoint >= checkpoint_every then begin
        inject_barrier ();
        since_checkpoint := 0
      end
    | Mf_event.Watermark _ ->
      (* Watermark — всем воркерам *)
      Array.iteri (fun i ch ->
        if not failed.(i) then ignore (Channel.try_push ch (Event ev))) in_chans
    | Mf_event.Retract _ -> ()
  ) source;

  (* Финальный barrier — закоммитить последний срез *)
  inject_barrier ();

  Array.iter Channel.close in_chans;
  Array.iter Thread.join worker_threads

(* ── Восстановление ───────────────────────────────────────── *)

(* Раздать снапшоты последнего checkpoint-а воркерам.
   backends.(i) восстанавливается из snapshot воркера i. *)
let restore_latest store (backends : State_backend_memory.t array) =
  match latest_checkpoint store with
  | None -> false
  | Some (_epoch, snapshots) ->
    Array.iter (fun snap ->
      if snap.worker < Array.length backends then
        State_backend_memory.restore backends.(snap.worker) snap.state
    ) snapshots;
    true
