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
  let h = ref 5381 in
  String.iter (fun c -> h := !h * 33 + Char.code c) key;
  (abs !h) mod n

let run_parallel_simple
    ~(workers  : int)
    ~(capacity : int)
    ~(key_of   : 'a -> string)
    ~(pipeline : 'a Mf_event.t Stream.t -> 'b Mf_event.t Stream.t)
    ~(source   : 'a Mf_event.t Stream.t)
    ~(sink     : 'b -> unit)
    () =

  let in_chans = Array.init workers (fun _ -> Channel.make_bounded capacity) in
  let mu_sink  = Mutex.create () in

  (* ── Workers: Domain вместо Thread ────────────────────── *)
  let worker_domains = Array.init workers (fun i ->
    Domain.spawn (fun () ->
      let src = Channel.to_stream in_chans.(i) in
      pipeline src
      |> Pipe.sink (fun v ->
           (* В продакшне: каждый воркер пишет в свою Kafka партицию — без mutex *)
           (* Здесь: общий sink защищаем мьютексом *)
           Mutex.lock mu_sink;
           sink v;
           Mutex.unlock mu_sink)
    )
  ) in

  (* ── Dispatcher: основной Domain ──────────────────────── *)
  Stream.iter (fun ev ->
    match ev with
    | Mf_event.Data (v,_) ->
      Channel.push in_chans.(hash_key (key_of v) workers) ev
    | Mf_event.Watermark _ ->
      Array.iter (fun ch -> Channel.push ch ev) in_chans
    | Mf_event.Retract _ -> ()
  ) source;

  Array.iter Channel.close in_chans;

  (* Domain.join вместо Thread.join *)
  Array.iter Domain.join worker_domains
