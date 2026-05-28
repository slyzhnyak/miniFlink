(* ============================================================
   Runtime.ml — конфигурация и запуск pipeline

   Простой подход: тип config хранит имя режима,
   run выбирает реализации по нему.
   Новый режим = новый конструктор + ветка в run.
   ============================================================ *)

type mode =
  | Noop      (* разработка и тесты: всё в noop *)
  | Log       (* метрики в stderr, shutdown handler *)
  | Parallel  (* многопоточный, noop метрики *)
  | Prod      (* всё включено *)

type config = {
  mode        : mode;
  parallelism : int;
  capacity    : int;
}

let noop     = { mode = Noop;     parallelism = 1; capacity = 1024 }
let log_cfg  = { mode = Log;      parallelism = 1; capacity = 1024 }
let parallel = { mode = Parallel; parallelism = 4; capacity = 4096 }
let prod     = { mode = Prod;     parallelism = 4; capacity = 8192 }

(* ── Helpers ─────────────────────────────────────────────── *)

let with_shutdown cfg f =
  match cfg.mode with
  | Noop | Parallel -> f ()
  | Log | Prod ->
    Shutdown_default.register ~on_shutdown:(fun () -> ());
    f ()

(* ── Run ─────────────────────────────────────────────────── *)

let run cfg
    ~(key_of   : 'a -> string)
    ~(source   : 'a Mf_event.t Stream.t)
    ~(pipeline : 'a Mf_event.t Stream.t -> 'b Mf_event.t Stream.t)
    ~(sink     : 'b -> unit)
    () =

  (* Shutdown *)
  let running = ref true in
  (match cfg.mode with
   | Log | Prod ->
     Shutdown_default.register ~on_shutdown:(fun () -> running := false)
   | _ -> ());

  (* Метрики *)
  let incr_in, incr_out, start_rep =
    match cfg.mode with
    | Log | Prod ->
      let c_in  = Metrics_log.counter
                    ~name:"miniflink_events_in"
                    ~labels:[("mode", (match cfg.mode with Prod -> "prod" | _ -> "log"))] in
      let c_out = Metrics_log.counter
                    ~name:"miniflink_events_out"
                    ~labels:[("mode", (match cfg.mode with Prod -> "prod" | _ -> "log"))] in
      (fun () -> Metrics_log.incr c_in),
      (fun () -> Metrics_log.incr c_out),
      (fun () -> Metrics_log.start_reporter ~interval_s:30)
    | _ ->
      (fun () -> ()), (fun () -> ()), (fun () -> ())
  in
  start_rep ();

  (* DLQ *)
  let dlq = match cfg.mode with
    | Log | Prod -> Dlq_noop.create ()  (* dlq_log used via dlq_send below *)
    | _          -> Dlq_noop.create ()
  in
  ignore dlq;

  let guarded =
    fun () ->
      if Shutdown_noop.is_requested () && cfg.mode = Noop then None
      else if (match cfg.mode with Log | Prod -> Shutdown_default.is_requested () | _ -> false)
      then None
      else
        match source () with
        | Some (Mf_event.Data _ as ev) -> incr_in (); Some ev
        | other -> other
  in

  if cfg.parallelism <= 1 then
    pipeline guarded
    |> Pipe.sink (fun v -> incr_out (); sink v)
  else
    Parallel.run_parallel_simple
      ~workers:cfg.parallelism
      ~capacity:cfg.capacity
      ~key_of
      ~pipeline
      ~source:guarded
      ~sink:(fun v -> incr_out (); sink v)
      ()

let ignore_running = ignore
