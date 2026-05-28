(* ============================================================
   Runtime.ml — конфигурация и запуск pipeline

   Каждый mode выбирает конкретные реализации:
   - Noop    : всё в заглушках, для тестов
   - Log     : метрики в stderr + graceful shutdown
   - Parallel: параллельный, noop метрики
   - Prod    : всё включено, параллельный

   DLQ встроен в safe_decode — невалидные payload не роняют pipeline.
   Shutdown встроен в source guard — SIGTERM дочитывает буфер.
   ============================================================ *)

type mode = Noop | Log | Parallel | Prod

type config = {
  mode        : mode;
  parallelism : int;
  capacity    : int;
}

let noop     = { mode = Noop;     parallelism = 1; capacity = 1024 }
let log_cfg  = { mode = Log;      parallelism = 1; capacity = 1024 }
let parallel = { mode = Parallel; parallelism = 4; capacity = 4096 }
let prod     = { mode = Prod;     parallelism = 4; capacity = 8192 }

(* ── DLQ helpers ─────────────────────────────────────────── *)

(* Создать DLQ нужного типа *)
let make_dlq mode =
  match mode with
  | Log | Prod ->
    let dlq = Dlq_log.create () in
    (fun entry -> Dlq_log.send dlq entry),
    (fun ()    -> Dlq_log.flush dlq),
    (fun ()    -> Dlq_log.count dlq)
  | _ ->
    let dlq = Dlq_noop.create () in
    (fun entry -> Dlq_noop.send dlq entry),
    (fun ()    -> Dlq_noop.flush dlq),
    (fun ()    -> Dlq_noop.count dlq)

(* safe_decode: ошибки декодирования → DLQ, не падение *)
let safe_decode ~send_dlq ~topic ~codec ~attempt raw =
  match codec raw with
  | Ok v    -> Some v
  | Error e ->
    send_dlq {
      Dlq_noop.topic;
      payload = raw;
      error   = e;
      ts      = int_of_float (Unix.gettimeofday () *. 1000.0);
      attempt;
    };
    None

(* ── Metrics helpers ─────────────────────────────────────── *)

let make_metrics mode name =
  match mode with
  | Log | Prod ->
    let label = match mode with Prod -> "prod" | _ -> "log" in
    let c_in  = Metrics_log.counter ~name:(name ^ "_in")
                  ~labels:[("mode", label)] in
    let c_out = Metrics_log.counter ~name:(name ^ "_out")
                  ~labels:[("mode", label)] in
    let c_dlq = Metrics_log.counter ~name:(name ^ "_dlq")
                  ~labels:[("mode", label)] in
    let g_lag = Metrics_log.gauge ~name:(name ^ "_lag")
                  ~labels:[("mode", label)] in
    (fun () -> Metrics_log.incr c_in),
    (fun () -> Metrics_log.incr c_out),
    (fun () -> Metrics_log.incr c_dlq),
    (fun n  -> Metrics_log.set_gauge g_lag (float_of_int n)),
    (fun () -> Metrics_log.start_reporter ~interval_s:30)
  | _ ->
    (fun () -> ()), (fun () -> ()), (fun () -> ()),
    (fun _  -> ()), (fun () -> ())

(* ── Shutdown helpers ────────────────────────────────────── *)

(* Возвращает (running_ref, register_fn, is_requested_fn) *)
let make_shutdown mode =
  let running = ref true in
  (match mode with
   | Log | Prod ->
     Shutdown_default.register ~on_shutdown:(fun () ->
       Printf.eprintf "[runtime] shutdown requested, draining...\n%!";
       running := false)
   | _ -> ());
  running,
  (match mode with
   | Log | Prod -> Shutdown_default.is_requested
   | _          -> Shutdown_noop.is_requested)

(* ── Source guard ────────────────────────────────────────── *)

(* Оборачивает source: останавливается при shutdown *)
let guarded_source ~running ~is_requested ~incr_in source =
  fun () ->
    if not !running || is_requested () then None
    else
      match source () with
      | Some (Mf_event.Data _ as ev) -> incr_in (); Some ev
      | other -> other

(* ── Run ─────────────────────────────────────────────────── *)

let run cfg
    ~(key_of   : 'a -> string)
    ~(source   : 'a Mf_event.t Stream.t)
    ~(pipeline : 'a Mf_event.t Stream.t -> 'b Mf_event.t Stream.t)
    ~(sink     : 'b -> unit)
    () =

  let (send_dlq, flush_dlq, count_dlq) = make_dlq cfg.mode in
  ignore (send_dlq, flush_dlq, count_dlq);  (* используются в safe_decode *)

  let (incr_in, incr_out, _incr_dlq, _set_lag, start_rep) =
    make_metrics cfg.mode "miniflink" in
  start_rep ();

  let (running, is_requested) = make_shutdown cfg.mode in

  let src = guarded_source ~running ~is_requested ~incr_in source in

  let wrapped_sink v =
    incr_out ();
    sink v
  in

  if cfg.parallelism <= 1 then
    pipeline src
    |> Pipe.sink wrapped_sink
  else
    Parallel.run_parallel_simple
      ~workers:cfg.parallelism
      ~capacity:cfg.capacity
      ~key_of
      ~pipeline
      ~source:src
      ~sink:wrapped_sink
      ();

  (* Финальная статистика *)
  (match cfg.mode with
   | Log | Prod ->
     let n = count_dlq () in
     if n > 0 then
       Printf.eprintf "[runtime] done. DLQ: %d messages\n%!" n
   | _ -> ())

(* ── Codec-aware source wrap ─────────────────────────────── *)

(** Обернуть raw bytes source в типизированный stream с DLQ *)
let make_stream_with_dlq cfg ~topic ~codec ~ts_of raw_source =
  let (send_dlq, _, _) = make_dlq cfg.mode in
  fun () ->
    match raw_source () with
    | None -> None
    | Some (topic_actual, payload) ->
      (match safe_decode ~send_dlq
               ~topic:(if topic_actual = "" then topic else topic_actual)
               ~codec ~attempt:1 payload with
       | None   -> None
       | Some v -> Some (Mf_event.data v (ts_of v)))
