(* ============================================================
   Parallel.ml — data-parallel выполнение pipeline

   Схема:
     Source → dispatcher → [worker_0 .. worker_N] → collector → Sink

   dispatcher: читает из source, шардирует по hash(key),
               пишет в input_channel[shard]

   worker_i:   читает из input_channel[i],
               прогоняет через pipeline(fn),
               пишет в output_channel[i]

   collector:  читает из всех output_channel,
               передаёт в sink

   Ключевое свойство: все события с одним device_id
   всегда идут в один shard → корректная window семантика.
   ============================================================ *)

(* ── Хэш-шардирование ────────────────────────────────────── *)

(* Djb2 hash для строк — быстрый и равномерный *)
let hash_key key n_shards =
  if n_shards <= 0 then
    invalid_arg "hash_key: число шардов должно быть > 0";
  let h = ref 5381 in
  String.iter (fun c -> h := !h * 33 + Char.code c) key;
  (!h land max_int) mod n_shards

(* ── Статистика worker-а ─────────────────────────────────── *)

type worker_stats = {
  mutable processed : int;
  mutable emitted   : int;
  mutable wm_seen   : int;
}

let make_stats () = { processed = 0; emitted = 0; wm_seen = 0 }

(* ── Parallel run ─────────────────────────────────────────── *)

(**
   run_parallel
     ~workers      : число воркеров (обычно = число CPU)
     ~capacity     : размер каждого bounded channel
     ~key_of       : как достать ключ шардирования из события
     ~pipeline     : функция 'a Mf_event.t Stream.t → 'b Mf_event.t Stream.t
     ~source       : входной поток
     ~sink         : функция для обработки выходных событий
*)
let run_parallel
    ~(workers  : int)
    ~(capacity : int)
    ~(key_of   : 'a -> string)
    ~(pipeline : 'a Mf_event.t Stream.t -> 'b Mf_event.t Stream.t)
    ~(source   : 'a Mf_event.t Stream.t)
    ~(sink     : 'b -> unit)
    () =

  (* Каждый worker получает свой input и output channel *)
  let in_chans  = Array.init workers (fun _ -> Channel.make_bounded capacity) in
  let out_chans = Array.init workers (fun _ -> Channel.make_bounded capacity) in
  let stats     = Array.init workers (fun _ -> make_stats ()) in

  (* ── Workers ────────────────────────────────────────────── *)
  let worker_threads = Array.init workers (fun i ->
    Thread.create (fun () ->
      (try
         let st   = stats.(i) in
         let src  = Channel.to_stream in_chans.(i) in
         let out  = pipeline src in
         Stream.iter (fun ev ->
           (match ev with
            | Mf_event.Data _      -> st.emitted <- st.emitted + 1
            | Mf_event.Update _    -> st.emitted <- st.emitted + 1
            | Mf_event.Watermark _ -> st.wm_seen <- st.wm_seen + 1
            | Mf_event.Retract _   -> ());
           Channel.push out_chans.(i) ev
         ) out
       with e ->
         Printf.eprintf "[parallel] worker %d crashed: %s\n%!"
           i (Printexc.to_string e));
      (* Всегда закрываем out — иначе collector зависнет на pop *)
      Channel.close out_chans.(i)
    ) ()
  ) in

  (* ── Collector ──────────────────────────────────────────── *)
  (* Один collector-поток на каждый output канал.
     Блокирующий pop: нет busy-wait, нет преждевременного завершения.
     pop возвращает None только когда канал закрыт И пуст → воркер кончил. *)
  let mu_sink = Mutex.create () in
  (* sink — пользовательский callback; вызывается из N collector-потоков
     под общим mu_sink. Если sink бросит, mutex обязан освободиться,
     иначе остальные collector-потоки навсегда зависнут на нём
     (deadlock). Совпадает с защитой в parallel_v5. *)
  let safe_sink v =
    Mutex.lock mu_sink;
    (match sink v with
     | () -> Mutex.unlock mu_sink
     | exception e -> Mutex.unlock mu_sink; raise e)
  in
  let collector_threads = Array.init workers (fun i ->
    Thread.create (fun () ->
      let rec loop () =
        match Channel.pop out_chans.(i) with
        | None -> ()                       (* канал закрыт и пуст *)
        | Some (Mf_event.Data (v, _)) ->
          safe_sink v; loop ()
        | Some (Mf_event.Update { new_value = v; _ }) ->
          safe_sink v; loop ()
        | Some (Mf_event.Watermark _) | Some (Mf_event.Retract _) ->
          loop ()
      in loop ()
    ) ()
  ) in

  (* ── Dispatcher ─────────────────────────────────────────── *)
  (* Основной поток: читает source и шардирует *)
  let wm_count = ref 0 in
  Stream.iter (fun ev ->
    match ev with
    | Mf_event.Data (v, _) ->
      let shard = hash_key (key_of v) workers in
      stats.(shard).processed <- stats.(shard).processed + 1;
      Channel.push in_chans.(shard) ev
    | Mf_event.Watermark _ ->
      incr wm_count;
      (* Watermark рассылаем всем воркерам *)
      Array.iter (fun ch -> Channel.push ch ev) in_chans
    | Mf_event.Retract (v, _) ->
      (* Ретракции: шардируем по ключу как Data *)
      let shard = hash_key (key_of v) workers in
      Channel.push in_chans.(shard) ev
    | Mf_event.Update { new_value = v; _ } ->
      (* Update — шардируем по ключу new_value. Если key функция
         даёт разные ключи для old и new — будет cross-shard
         transfer (редкий случай); для обычного use case oba ключа
         одинаковы и Update остаётся на том же shard. *)
      let shard = hash_key (key_of v) workers in
      stats.(shard).processed <- stats.(shard).processed + 1;
      Channel.push in_chans.(shard) ev
  ) source;

  (* Закрываем все входные каналы *)
  Array.iter Channel.close in_chans;

  (* Ждём завершения *)
  Array.iter Thread.join worker_threads;
  Array.iter Thread.join collector_threads;

  (* Возвращаем статистику *)
  stats

(* ── Упрощённая версия: без collector, sink вызывается из воркеров ── *)

let run_parallel_simple
    ?(sink_factory : (int -> ('b -> unit)) option)
    ?(on_queue_depth : (int array -> unit) option)
    ~(workers  : int)
    ~(capacity : int)
    ~(key_of   : 'a -> string)
    ~(pipeline : 'a Mf_event.t Stream.t -> 'b Mf_event.t Stream.t)
    ~(source   : 'a Mf_event.t Stream.t)
    ~(sink     : 'b -> unit)
    () =

  let in_chans = Array.init workers (fun _ -> Channel.make_bounded capacity) in
  let failed   = Array.make workers false in  (* упал ли воркер i *)

  (* Per-worker sink:
     - sink_factory задан → каждый воркер пишет в свой sink, без мьютекса
       (например, своя Kafka партиция). Нет contention.
     - иначе → общий sink под mu_sink (безопасно, но contention при
       высоком parallelism). *)
  let mu_sink = Mutex.create () in
  let worker_sink = match sink_factory with
    | Some factory -> (fun i -> factory i)              (* свой на воркера *)
    | None -> (fun _ -> fun v ->
        Mutex.lock mu_sink;
        (try sink v with e -> Mutex.unlock mu_sink; raise e);
        Mutex.unlock mu_sink)
  in

  let worker_threads = Array.init workers (fun i ->
    let my_sink = worker_sink i in
    Thread.create (fun () ->
      (try
         let src = Channel.to_stream in_chans.(i) in
         pipeline src |> Pipe.sink my_sink
       with e ->
         (* Воркер упал: помечаем, закрываем входной канал.
            Dispatcher через try_push узнает что писать некуда. *)
         failed.(i) <- true;
         Channel.close in_chans.(i);
         Printf.eprintf "[parallel] worker %d crashed: %s\n%!"
           i (Printexc.to_string e))
    ) ()
  ) in

  let report_depth = match on_queue_depth with
    | None -> (fun () -> ())
    | Some f ->
      let n = ref 0 in
      (fun () ->
        incr n;
        (* прореживаем: раз в 64 события, чтобы не звать на каждом *)
        if !n land 63 = 0 then
          f (Array.map Channel.length in_chans))
  in
  Stream.iter (fun ev ->
    match ev with
    | Mf_event.Data (v,_) ->
      let shard = hash_key (key_of v) workers in
      if not failed.(shard) then
        ignore (Channel.try_push in_chans.(shard) ev);
      report_depth ()
    | Mf_event.Watermark _ ->
      Array.iteri (fun i ch ->
        if not failed.(i) then ignore (Channel.try_push ch ev)) in_chans
    | Mf_event.Retract (v,_) ->
      let shard = hash_key (key_of v) workers in
      if not failed.(shard) then ignore (Channel.try_push in_chans.(shard) ev)
    | Mf_event.Update { new_value = v; _ } ->
      let shard = hash_key (key_of v) workers in
      if not failed.(shard) then ignore (Channel.try_push in_chans.(shard) ev);
      report_depth ()
  ) source;

  Array.iter Channel.close in_chans;
  Array.iter Thread.join worker_threads
