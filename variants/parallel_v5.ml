(* ============================================================
   Parallel_v5.ml — параллельное выполнение через Domain (OCaml 5)

   Единственное отличие от v4:
     Thread.create → Domain.spawn
     Thread.join   → Domain.join

   Всё остальное идентично.

   В OCaml 5 Domain.spawn запускает настоящий OS thread
   без GIL — все Domain-ы работают параллельно на разных ядрах.

   Важно: sink вызывается из разных Domain-ов параллельно.
   Если sink не thread-safe (например Printf.printf) —
   нужен mutex. Если sink пишет в Kafka с per-partition producer
   и каждый воркер пишет в свою партицию — mutex не нужен.
   ============================================================ *)

type worker_stats = {
  mutable processed : int;
  mutable emitted   : int;
  mutable wm_seen   : int;
}

let make_stats () = { processed = 0; emitted = 0; wm_seen = 0 }

let hash_key key n =
  if n <= 0 then
    invalid_arg "hash_key: число шардов должно быть > 0";
  let h = ref 5381 in
  String.iter (fun c -> h := !h * 33 + Char.code c) key;
  (!h land max_int) mod n

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
  let failed   = Array.make workers false in

  let mu_sink = Mutex.create () in
  let worker_sink = match sink_factory with
    | Some factory -> (fun i -> factory i)
    | None -> (fun _ -> fun v ->
        Mutex.lock mu_sink;
        (try sink v with e -> Mutex.unlock mu_sink; raise e);
        Mutex.unlock mu_sink)
  in

  (* ── Workers: Domain вместо Thread ────────────────────── *)
  let worker_domains = Array.init workers (fun i ->
    let my_sink = worker_sink i in
    Domain.spawn (fun () ->
      (try
         let src = Channel.to_stream in_chans.(i) in
         pipeline src |> Pipe.sink my_sink
       with e ->
         failed.(i) <- true;
         Channel.close in_chans.(i);
         Printf.eprintf "[parallel] worker %d crashed: %s\n%!"
           i (Printexc.to_string e))
    )
  ) in

  (* ── Dispatcher: основной Domain ──────────────────────── *)
  let report_depth = match on_queue_depth with
    | None -> (fun () -> ())
    | Some f ->
      let n = ref 0 in
      (fun () ->
        incr n;
        if !n land 63 = 0 then f (Array.map Channel.length in_chans))
  in
  Stream.iter (fun ev ->
    match ev with
    | Mf_event.Data (v,_) ->
      let shard = hash_key (key_of v) workers in
      if not failed.(shard) then begin
        (* try_push спинит при полном канале (backpressure) и вернёт
           false только если воркер УПАЛ (канал закрыт) — тогда событие
           его шарда обработать некем; логируем, а не глотаем молча *)
        if not (Channel.try_push in_chans.(shard) ev) then
          Printf.eprintf "[parallel] dropped event for dead worker %d\n%!" shard
      end;
      report_depth ()
    | Mf_event.Watermark _ ->
      Array.iteri (fun i ch ->
        if not failed.(i) then ignore (Channel.try_push ch ev)) in_chans
    | Mf_event.Retract (v,_) ->
      (* ретракт шардируем по ключу как Data — иначе он терялся бы и
         retract-семантика расходилась бы с однопоточным путём *)
      let shard = hash_key (key_of v) workers in
      if not failed.(shard) then
        ignore (Channel.try_push in_chans.(shard) ev)
    | Mf_event.Update { new_value = v; _ } ->
      (* Update шардируем по ключу new_value, как Data. *)
      let shard = hash_key (key_of v) workers in
      if not failed.(shard) then
        ignore (Channel.try_push in_chans.(shard) ev);
      report_depth ()
  ) source;

  Array.iter Channel.close in_chans;

  (* Domain.join вместо Thread.join *)
  Array.iter Domain.join worker_domains
