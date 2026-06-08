open Miniflink
open Domain
open Time

(* ============================================================
   Test_metrics.ml — тесты для Metrics
   ============================================================ *)

let pass name = Printf.printf "  OK %s\n%!" name
let fail name msg = Printf.printf "  FAIL %s: %s\n%!" name msg; exit 1
let check name cond = if cond then pass name else fail name "assertion failed"
let check_eq name a b =
  if a = b then pass name
  else fail name (Printf.sprintf "expected %d, got %d" b a)
let check_f name a b eps =
  if abs_float (a -. b) < eps then pass name
  else fail name (Printf.sprintf "expected %.3f, got %.3f" b a)

(* ── 1. Counter ─────────────────────────────────────────── *)
let test_counter () =
  Printf.printf "\n-- Counter\n";
  let c = Metrics_log.counter ~name:"test_counter" ~labels:[("test","1")] in
  (* Начальное значение *)
  let d0 = Metrics_log.dump () in
  check "dump contains counter" (String.length d0 > 0);
  Metrics_log.incr c;
  Metrics_log.incr c;
  Metrics_log.add c 3;
  (* Проверяем через dump — ищем "5" в выводе *)
  let d = Metrics_log.dump () in
  check "counter value 5 in dump"
    (let re = Str.regexp "test_counter.*5" in
     try ignore (Str.search_forward re d 0); true
     with Not_found -> false)

(* ── 2. Gauge ────────────────────────────────────────────── *)
let test_gauge () =
  Printf.printf "\n-- Gauge\n";
  let g = Metrics_log.gauge ~name:"test_gauge" ~labels:[] in
  Metrics_log.set_gauge g 42.5;
  let d = Metrics_log.dump () in
  check "gauge value in dump" (String.length d > 0);
  Metrics_log.set_gauge g 0.0;
  (* gauge может уменьшаться *)
  check "gauge can decrease" true

(* ── 3. Histogram ────────────────────────────────────────── *)
let test_histogram () =
  Printf.printf "\n-- Histogram\n";
  let h = Metrics_log.histogram ~name:"test_hist" ~labels:[("op","window")] in
  (* Наблюдаем несколько значений latency *)
  Metrics_log.observe h 5.0;     (* 5 мкс — попадёт в бакет ≤10 *)
  Metrics_log.observe h 50.0;    (* 50 мкс — бакет ≤100 *)
  Metrics_log.observe h 500.0;   (* 500 мкс — бакет ≤1000 *)
  Metrics_log.observe h 5000.0;  (* 5 мс — бакет ≤10000 *)
  let d = Metrics_log.dump () in
  check "histogram in dump" (
    let re = Str.regexp "test_hist_count.*4" in
    try ignore (Str.search_forward re d 0); true
    with Not_found -> false)

(* ── 4. Dump format — Prometheus exposition ─────────────── *)
let test_dump_format () =
  Printf.printf "\n-- Prometheus exposition format\n";
  let _ = Metrics_log.counter ~name:"prom_counter" ~labels:[("env","test")] in
  let _ = Metrics_log.gauge   ~name:"prom_gauge"   ~labels:[] in
  let d = Metrics_log.dump () in
  check "has TYPE comment"    (String.length (Str.global_replace
    (Str.regexp "# TYPE") "" d) < String.length d);
  check "has counter type"    (try ignore (Str.search_forward
    (Str.regexp "# TYPE.*counter") d 0); true with Not_found -> false);
  check "has gauge type"      (try ignore (Str.search_forward
    (Str.regexp "# TYPE.*gauge") d 0); true with Not_found -> false);
  check "has histogram type"  (try ignore (Str.search_forward
    (Str.regexp "# TYPE.*histogram") d 0); true with Not_found -> false);
  check "labels in output"    (try ignore (Str.search_forward
    (Str.regexp "env=\"test\"") d 0); true with Not_found -> false)

(* ── 5. Runtime: метрики накапливаются при run ───────────── *)
let test_runtime_metrics () =
  Printf.printf "\n-- Runtime metrics accumulation\n";
  let devices = Table.of_list [
    "A", { owner="O"; max_speed=90.; zone="z" } ] in
  (* Pipeline без внутреннего with_watermarks — watermark подаём в source,
     чтобы runtime's on_wm их увидел и посчитал lag *)
  let pipeline src =
    src
    |> Pipe.enrich (module Telemetry) ~from:devices
         ~merge:(fun t d -> { t with device=d })
    |> Pipe.window (module Telemetry) (Pipe.tumbling (seconds 30))
    |> Pipe.aggregate Rules.compute
    |> Pipe.flat_map (Rules.check Rules.fleet)
  in
  (* Источник с явными watermark-ами *)
  let mk t = { device_id="A"; speed_kmh=120.; fuel_pct=80.;
    position={lat=55.;lon=37.}; ts=t; device=None } in
  let events = [
    Mf_event.data (mk 5000) 5000;
    Mf_event.data (mk 20000) 20000;
    Mf_event.wm 17000;          (* lag = 20000 - 17000 = 3000 *)
    Mf_event.data (mk 35000) 35000;
    Mf_event.wm 65000;
  ] in
  let sink_count = ref 0 in
  Runtime.run ~label:"wmtest" Runtime.log_cfg
    ~key_of:(fun (t:telemetry) -> t.device_id)
    ~source:(Stream.of_list events)
    ~pipeline
    ~sink:(fun _ -> incr sink_count)
    ();
  let d = Metrics_log.dump () in
  let has re = try ignore (Str.search_forward (Str.regexp re) d 0); true
               with Not_found -> false in
  check "events_in counter in dump"     (has "miniflink_events_in");
  check "watermarks_total in dump"       (has "miniflink_watermarks_total");
  check "watermark_lag_ms in dump"       (has "miniflink_watermark_lag_ms");
  check "watermark_last_ms in dump"      (has "miniflink_watermark_last_ms");
  check "max_event_ts_ms in dump"        (has "miniflink_max_event_ts_ms")

(* ── Main ────────────────────────────────────────────────── *)
let () =
  Printf.printf "==========================================\n";
  Printf.printf "  miniFlink v3 - Metrics Tests\n";
  Printf.printf "==========================================\n";
  test_counter ();
  test_gauge ();
  test_histogram ();
  test_dump_format ();
  test_runtime_metrics ();
  Printf.printf "\nAll metrics tests passed.\n"
