open Miniflink
(* ============================================================
   Bench.ml — нагрузочный тест miniFlink v3

   Измеряем:
   1. Raw stream throughput (map + filter)
   2. Window throughput (tumbling window + aggregate)
   3. Full pipeline (enrich + window + rules + dedup)
   4. Масштабирование по числу устройств
   5. Масштабирование по размеру окна
   6. GC давление — сколько слов аллоцировано на событие

   Метрики:
   - events/sec (throughput)
   - ns/event   (latency per event)
   - words/event (GC pressure)
   ============================================================ *)

open Domain
open Time

(* ── Генератор событий ────────────────────────────────────── *)

let make_event ?(speed=60.) ?(fuel=80.) device_id ts =
  Mf_event.data
    { device_id; speed_kmh = speed; fuel_pct = fuel;
      position = { lat = 55.75; lon = 37.61 };
      ts; device = None }
    ts

(* Генерировать N событий от D устройств, интервал step_ms *)
let gen_events ~n_events ~n_devices ~step_ms =
  Array.init n_events (fun i ->
    let device_id = Printf.sprintf "device_%04d" (i mod n_devices) in
    let ts        = i * step_ms in
    (* каждое 10-е устройство превышает скорость для алертов *)
    let speed     = if i mod 10 = 0 then 130. else 60. in
    let fuel      = if i mod 7 = 0 then 15. else 80. in
    make_event device_id ts ~speed ~fuel)

(* ── Измерение времени ────────────────────────────────────── *)

let now_ns () =
  let t = Unix.gettimeofday () in
  Int64.of_float (t *. 1e9)

let measure ~label ~n f =
  (* прогрев *)
  ignore (f ());
  (* GC статистика *)
  Gc.compact ();
  let gc_before = Gc.stat () in
  let t0 = now_ns () in
  let result = f () in
  let t1 = now_ns () in
  let gc_after = Gc.stat () in
  let elapsed_ns = Int64.to_float (Int64.sub t1 t0) in
  let elapsed_s  = elapsed_ns /. 1e9 in
  let events_sec = float_of_int n /. elapsed_s in
  let ns_per_ev  = elapsed_ns /. float_of_int n in
  let words_alloc = gc_after.Gc.minor_words -. gc_before.Gc.minor_words
                    +. gc_after.Gc.major_words -. gc_before.Gc.major_words in
  let words_per_ev = words_alloc /. float_of_int n in
  Printf.printf "  %-35s %8.0f ev/s  %6.1f ns/ev  %6.1f words/ev\n%!"
    label events_sec ns_per_ev words_per_ev;
  result

(* ── Справочник для enrich ────────────────────────────────── *)

let make_device_table n_devices =
  Table.of_list (
    List.init n_devices (fun i ->
      let id = Printf.sprintf "device_%04d" i in
      (id, { owner = "Owner"; max_speed = 90.; zone = "north" }))
  )

(* ── Bench 1: Raw stream throughput ──────────────────────── *)

let bench_raw events =
  let n = Array.length events in
  let src () = Stream.of_list (Array.to_list events) in
  measure ~label:"raw: map + filter" ~n (fun () ->
    src ()
    |> Pipe.map (fun (t:telemetry) -> t.speed_kmh *. 2.0)
    |> Pipe.filter (fun s -> s > 50.0)
    |> Stream.fold (fun acc _ -> acc + 1) 0)

(* ── Bench 2: Window throughput ──────────────────────────── *)

let bench_window events window_size_s =
  let n = Array.length events in
  let src () = Stream.of_list (Array.to_list events)
               |> Mf_event.with_watermarks ~latency:(seconds 3) in
  measure ~label:(Printf.sprintf "window: tumbling %ds" window_size_s) ~n (fun () ->
    src ()
    |> Pipe.window (module Telemetry) (Pipe.tumbling (seconds window_size_s))
    |> Pipe.aggregate Rules.compute
    |> Stream.fold (fun acc _ -> acc + 1) 0)

(* ── Bench 3: Full pipeline ───────────────────────────────── *)

let bench_full events n_devices =
  let n = Array.length events in
  let devices = make_device_table n_devices in
  let src () = Stream.of_list (Array.to_list events)
               |> Mf_event.with_watermarks ~latency:(seconds 3) in
  measure ~label:"full pipeline" ~n (fun () ->
    src ()
    |> Pipe.enrich (module Telemetry)
         ~from:devices
         ~merge:(fun t dev -> { t with device = dev })
    |> Pipe.window (module Telemetry) (Pipe.tumbling (seconds 30))
    |> Pipe.aggregate Rules.compute
    |> Pipe.flat_map (Rules.check Rules.fleet)
    |> Pipe.dedup (module Alert)
         ~rule:(fun a -> a.rule)
         ~cooldown:(minutes 5)
    |> Stream.fold (fun acc _ -> acc + 1) 0)

(* ── Bench 4: Масштабирование по числу устройств ────────────*)

let bench_scale_devices n_events =
  Printf.printf "\n  Масштабирование по числу устройств (n_events=%d):\n" n_events;
  [1; 10; 100; 1000; 10_000] |> List.iter (fun n_devices ->
    let events = gen_events ~n_events ~n_devices ~step_ms:100 in
    let devices = make_device_table n_devices in
    let src () = Stream.of_list (Array.to_list events)
                 |> Mf_event.with_watermarks ~latency:(seconds 3) in
    ignore (measure
      ~label:(Printf.sprintf "  %5d devices" n_devices)
      ~n:n_events
      (fun () ->
        src ()
        |> Pipe.enrich (module Telemetry)
             ~from:devices
             ~merge:(fun t dev -> { t with device = dev })
        |> Pipe.window (module Telemetry) (Pipe.tumbling (seconds 30))
        |> Pipe.aggregate Rules.compute
        |> Pipe.flat_map (Rules.check Rules.fleet)
        |> Stream.fold (fun acc _ -> acc + 1) 0)))

(* ── Bench 5: Масштабирование по размеру окна ────────────── *)

let bench_scale_window events =
  Printf.printf "\n  Масштабирование по размеру окна (n=%d):\n"
    (Array.length events);
  [1; 5; 30; 60; 300] |> List.iter (fun size_s ->
    ignore (bench_window events size_s))

(* ── Bench 6: Сравнение с baseline ──────────────────────────*)

(* Baseline: то же самое через чистый List без фреймворка *)
let bench_baseline_list events =
  let n = Array.length events in
  let lst = Array.to_list events in
  measure ~label:"baseline: List.filter_map (no framework)" ~n (fun () ->
    lst
    |> List.filter_map (function
        | Mf_event.Data (t,_) -> Some t | _ -> None)
    |> List.fold_left (fun acc (t:telemetry) ->
        if t.speed_kmh > 90. then acc + 1 else acc) 0)

(* Baseline: Seq (lazy list) *)
let bench_baseline_seq events =
  let n = Array.length events in
  let arr = events in
  measure ~label:"baseline: Array.fold (bare metal)" ~n (fun () ->
    Array.fold_left (fun acc ev ->
      match ev with
      | Mf_event.Data (t,_) when t.speed_kmh > 90. -> acc + 1
      | _ -> acc
    ) 0 arr)

(* ── Bench 7: Latency distribution ──────────────────────── *)

let bench_latency events =
  Printf.printf "\n  Latency distribution (full pipeline, 1000 samples):\n";
  let n = min 1000 (Array.length events) in
  let latencies = Array.make n 0 in
  let devices = make_device_table 100 in
  for i = 0 to n - 1 do
    let single = [| events.(i) |] in
    let t0 = now_ns () in
    let src () = Stream.of_list (Array.to_list single)
                 |> Mf_event.with_watermarks ~latency:(seconds 3) in
    ignore (
      src ()
      |> Pipe.enrich (module Telemetry)
           ~from:devices
           ~merge:(fun t dev -> { t with device = dev })
      |> Pipe.window (module Telemetry) (Pipe.tumbling (seconds 30))
      |> Pipe.aggregate Rules.compute
      |> Stream.fold (fun acc _ -> acc + 1) 0
    );
    let t1 = now_ns () in
    latencies.(i) <- Int64.to_int (Int64.sub t1 t0)
  done;
  Array.sort compare latencies;
  Printf.printf "  p50  = %5d ns\n%!" latencies.(n/2);
  Printf.printf "  p95  = %5d ns\n%!" latencies.(n*95/100);
  Printf.printf "  p99  = %5d ns\n%!" latencies.(n*99/100);
  Printf.printf "  p999 = %5d ns\n%!" latencies.(n*999/1000)

(* ── Main ────────────────────────────────────────────────── *)

let () =
  let n_small  = 100_000 in
  let n_medium = 500_000 in
  let n_large  = 1_000_000 in

  Printf.printf "\n============================================\n";
  Printf.printf "  miniFlink v3 — Performance Benchmark\n";
  Printf.printf "  OCaml %s · %d CPU\n"
    Sys.ocaml_version
    (try int_of_string (Unix.getenv "NPROC") with _ -> 1);
  Printf.printf "============================================\n\n";

  let events_small  = gen_events ~n_events:n_small  ~n_devices:100  ~step_ms:100 in
  let events_medium = gen_events ~n_events:n_medium ~n_devices:1000 ~step_ms:100 in
  let events_large  = gen_events ~n_events:n_large  ~n_devices:1000 ~step_ms:100 in

  Printf.printf "── 1. Baseline (без фреймворка) ──\n";
  ignore (bench_baseline_seq    events_medium);
  ignore (bench_baseline_list   events_medium);

  Printf.printf "\n── 2. Raw stream (map + filter) ──\n";
  ignore (bench_raw events_small);
  ignore (bench_raw events_medium);
  ignore (bench_raw events_large);

  Printf.printf "\n── 3. Window throughput ──\n";
  ignore (bench_window events_medium 30);
  ignore (bench_window events_medium 60);
  ignore (bench_window events_medium 300);

  Printf.printf "\n── 4. Full pipeline ──\n";
  ignore (bench_full events_small  100);
  ignore (bench_full events_medium 1000);
  ignore (bench_full events_large  1000);

  Printf.printf "\n── 5. Scaling by device count ──";
  bench_scale_devices n_medium;

  Printf.printf "\n── 6. Scaling by window size ──";
  bench_scale_window events_medium;

  bench_latency events_medium;

  Printf.printf "\n── 7. GC summary ──\n";
  let gc = Gc.stat () in
  Printf.printf "  minor_collections: %d\n"    gc.Gc.minor_collections;
  Printf.printf "  major_collections: %d\n"    gc.Gc.major_collections;
  Printf.printf "  heap_words:        %d KB\n" (gc.Gc.heap_words * 8 / 1024);
  Printf.printf "  live_words:        %d KB\n" (gc.Gc.live_words * 8 / 1024);

  Printf.printf "\nDone.\n"
