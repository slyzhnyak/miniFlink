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
  let h = ref 5381 in
  String.iter (fun c -> h := !h * 33 + Char.code c) key;
  (abs !h) mod n_shards

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
      let st   = stats.(i) in
      let src  = Channel.to_stream in_chans.(i) in
      let out  = pipeline src in
      Stream.iter (fun ev ->
        (match ev with
         | Mf_event.Data _    -> st.emitted <- st.emitted + 1
         | Mf_event.Watermark _ -> st.wm_seen <- st.wm_seen + 1
         | Mf_event.Retract _ -> ());
        Channel.push out_chans.(i) ev
      ) out;
      Channel.close out_chans.(i)
    ) ()
  ) in

  (* ── Collector ──────────────────────────────────────────── *)
  (* Round-robin по output channels, пока хотя бы один открыт *)
  let collector_thread = Thread.create (fun () ->
    let active = Array.make workers true in
    let running = ref true in
    while !running do
      let any_active = ref false in
      Array.iteri (fun i ch ->
        if active.(i) then
          match Channel.try_pop ch with
          | None     -> any_active := true  (* ещё живой но пустой *)
          | Some (Mf_event.Data (v,_)) ->
            any_active := true;
            sink v
          | Some _ ->
            any_active := true              (* watermark/retract *)
      ) out_chans;
      if not !any_active then running := false
      else if not (Array.exists Fun.id active) then running := false
    done
  ) () in

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
    | Mf_event.Retract _ ->
      (* Ретракции: шардируем по ключу как Data *)
      ()  (* упрощение: ретракции пока не шардируем *)
  ) source;

  (* Закрываем все входные каналы *)
  Array.iter Channel.close in_chans;

  (* Ждём завершения *)
  Array.iter Thread.join worker_threads;
  Thread.join collector_thread;

  (* Возвращаем статистику *)
  stats

(* ── Упрощённая версия: без collector, sink вызывается из воркеров ── *)

let run_parallel_simple
    ~(workers  : int)
    ~(capacity : int)
    ~(key_of   : 'a -> string)
    ~(pipeline : 'a Mf_event.t Stream.t -> 'b Mf_event.t Stream.t)
    ~(source   : 'a Mf_event.t Stream.t)
    ~(sink     : 'b -> unit)
    () =

  let in_chans = Array.init workers (fun _ -> Channel.make_bounded capacity) in
  let mu_sink  = Mutex.create () in   (* защищаем sink от race *)
  let failed   = Array.make workers false in  (* упал ли воркер i *)

  let worker_threads = Array.init workers (fun i ->
    Thread.create (fun () ->
      (try
         let src = Channel.to_stream in_chans.(i) in
         pipeline src
         |> Pipe.sink (fun v ->
              Mutex.lock mu_sink;
              (try sink v with e ->
                 Mutex.unlock mu_sink; raise e);
              Mutex.unlock mu_sink)
       with e ->
         (* Воркер упал: помечаем, закрываем входной канал.
            Dispatcher через try_push узнает что писать некуда. *)
         failed.(i) <- true;
         Channel.close in_chans.(i);
         Printf.eprintf "[parallel] worker %d crashed: %s\n%!"
           i (Printexc.to_string e))
    ) ()
  ) in

  Stream.iter (fun ev ->
    match ev with
    | Mf_event.Data (v,_) ->
      let shard = hash_key (key_of v) workers in
      if not failed.(shard) then
        ignore (Channel.try_push in_chans.(shard) ev)
    | Mf_event.Watermark _ ->
      Array.iteri (fun i ch ->
        if not failed.(i) then ignore (Channel.try_push ch ev)) in_chans
    | Mf_event.Retract _ -> ()
  ) source;

  Array.iter Channel.close in_chans;
  Array.iter Thread.join worker_threads
