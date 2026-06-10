(* ============================================================
   Runtime.ml — конфигурация и запуск pipeline

   mode:
   Noop    — всё в заглушках
   Log     — метрики stderr + shutdown + DLQ log
   Parallel— параллельный + noop метрики
   Prod    — всё включено + Prometheus HTTP :9090
   ============================================================ *)

type mode = Noop | Log | Parallel | Prod

type config = {
  mode         : mode;
  parallelism  : int;
  capacity     : int;
  metrics_port : int;   (* порт Prometheus endpoint, 0 = выключено *)
}

let noop     = { mode=Noop;     parallelism=1; capacity=1024; metrics_port=0 }
let log_cfg  = { mode=Log;      parallelism=1; capacity=1024; metrics_port=0 }
let parallel = { mode=Parallel; parallelism=4; capacity=4096; metrics_port=0 }
let prod     = { mode=Prod;     parallelism=4; capacity=8192; metrics_port=9090 }

(* ── DLQ ─────────────────────────────────────────────────── *)

let make_dlq mode =
  match mode with
  | Log | Prod ->
    let dlq = Dlq_log.create () in
    (fun e -> Dlq_log.send dlq e),
    (fun () -> Dlq_log.flush dlq),
    (fun () -> Dlq_log.count dlq)
  | _ ->
    let dlq = Dlq_noop.create () in
    (fun e -> Dlq_noop.send dlq e),
    (fun () -> Dlq_noop.flush dlq),
    (fun () -> Dlq_noop.count dlq)

let safe_decode ~send_dlq ~topic ~codec ~attempt raw =
  match codec raw with
  | Ok v    -> Some v
  | Error e ->
    send_dlq { Dlq_noop.topic; payload=raw; error=e;
               ts=int_of_float (Unix.gettimeofday () *. 1000.); attempt };
    None

(* ── Metrics ─────────────────────────────────────────────── *)

type metrics = {
  events_in     : Metrics_log.counter;
  events_out    : Metrics_log.counter;
  events_dlq    : Metrics_log.counter;
  watermarks    : Metrics_log.counter;   (* сколько watermark прошло *)
  wm_lag_ms     : Metrics_log.gauge;      (* max_event_ts - last_watermark *)
  last_wm       : Metrics_log.gauge;      (* последний watermark (event-time) *)
  max_event_ts  : Metrics_log.gauge;      (* максимальный виденный event-time *)
}

let make_metrics _mode label =
  let mk_c n = Metrics_log.counter
    ~name:("miniflink_" ^ n) ~labels:[("pipeline", label)] in
  let mk_g n = Metrics_log.gauge
    ~name:("miniflink_" ^ n) ~labels:[("pipeline", label)] in
  { events_in    = mk_c "events_in";
    events_out   = mk_c "events_out";
    events_dlq   = mk_c "events_dlq";
    watermarks   = mk_c "watermarks_total";
    wm_lag_ms    = mk_g "watermark_lag_ms";
    last_wm      = mk_g "watermark_last_ms";
    max_event_ts = mk_g "max_event_ts_ms" }

(* noop метрики — не регистрируются в реестре *)
let noop_metrics _ = ()


(* ── Shutdown ────────────────────────────────────────────── *)

let make_shutdown mode =
  let running = ref true in
  (match mode with
   | Log | Prod ->
     Shutdown_default.register ~on_shutdown:(fun () ->
       Log.info "shutdown requested, draining";
       running := false)
   | _ -> ());
  running,
  (match mode with
   | Log | Prod -> Shutdown_default.is_requested
   | _          -> Shutdown_noop.is_requested)

(* ── Source guard ────────────────────────────────────────── *)

(* on_data: вызывается на Data; on_wm: на Watermark (для метрик lag) *)
let guarded_source ~running ~is_requested ~on_data ~on_wm source =
  fun () ->
    if not !running || is_requested () then None
    else match source () with
    | Some (Mf_event.Data (_, t) as ev) -> on_data t; Some ev
    | Some (Mf_event.Watermark t as ev) -> on_wm t; Some ev
    | other -> other

(* ── Run ─────────────────────────────────────────────────── *)

let run ?(label="default") cfg
    ~(key_of   : 'a -> string)
    ~(source   : 'a Mf_event.t Stream.t)
    ~(pipeline : 'a Mf_event.t Stream.t -> 'b Mf_event.t Stream.t)
    ~(sink     : 'b -> unit)
    () =

  let (send_dlq, flush_dlq, count_dlq) = make_dlq cfg.mode in

  (* Метрики *)
  let (incr_in, incr_out, on_data, on_wm, start_rep, start_srv) =
    match cfg.mode with
    | Log | Prod ->
      let m = make_metrics cfg.mode label in
      let max_ts = ref min_int in
      (fun () -> Metrics_log.incr m.events_in),
      (fun () -> Metrics_log.incr m.events_out),
      (fun t ->   (* on_data: обновляем max_event_ts *)
        if t > !max_ts then begin
          max_ts := t;
          Metrics_log.set_gauge m.max_event_ts (float_of_int t)
        end),
      (fun t ->   (* on_wm: считаем watermark, обновляем lag *)
        Metrics_log.incr m.watermarks;
        Metrics_log.set_gauge m.last_wm (float_of_int t);
        let lag = !max_ts - t in
        if !max_ts > min_int then
          Metrics_log.set_gauge m.wm_lag_ms (float_of_int (max 0 lag))),
      (fun () -> Metrics_log.start_reporter
                   ~stop:Shutdown_default.is_requested ~interval_s:30 ()),
      (fun () ->
        if cfg.metrics_port > 0 then
          Metrics_log.start_server
            ~stop:Shutdown_default.is_requested ~port:cfg.metrics_port ())
    | _ ->
      noop_metrics, noop_metrics,
      (fun _ -> ()), (fun _ -> ()),
      (fun () -> ()), (fun () -> ())
  in
  start_rep ();
  start_srv ();

  let (running, is_requested) = make_shutdown cfg.mode in
  let src = guarded_source ~running ~is_requested
              ~on_data:(fun t -> incr_in (); on_data t)
              ~on_wm source in

  let wrapped_sink v = incr_out (); sink v in

  if cfg.parallelism <= 1 then
    pipeline src |> Pipe.sink wrapped_sink
  else
    Parallel.run_parallel_simple
      ~workers:cfg.parallelism ~capacity:cfg.capacity
      ~key_of ~pipeline ~source:src ~sink:wrapped_sink ();

  flush_dlq ();
  let n = count_dlq () in
  if n > 0 then
    Log.info ~fields:[("pipeline", label); ("dlq_messages", string_of_int n)] "pipeline done"

(* ── Codec-aware source with DLQ ────────────────────────── *)

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

(* мост из Config.t (расширенная запись приложения) в runtime-конфиг:
   workers→parallelism, capacity→capacity; mode задаётся параметром
   (Config.t описывает РЕСУРСЫ, mode — РЕЖИМ исполнения). *)
let of_config ?(mode = Prod) (c : Config.t) : config =
  { mode; parallelism = c.Config.workers; capacity = c.Config.capacity;
    metrics_port = (if c.Config.metrics_interval_s > 0 then 9090 else 0) }
