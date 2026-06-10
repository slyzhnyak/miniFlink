open Miniflink
(* ============================================================
   Bench_parallel.ml — сравнение sequential vs parallel

   Измеряем speedup при N воркерах на full pipeline.
   ============================================================ *)

open Test_support.Domain
open Time

(* ── Setup ───────────────────────────────────────────────── *)

let make_events ~n ~n_devices ~step_ms =
  Array.init n (fun i ->
    let id    = Printf.sprintf "dev_%04d" (i mod n_devices) in
    let ts    = i * step_ms in
    let speed = if i mod 10 = 0 then 130. else 60. in
    let fuel  = if i mod 7  = 0 then 15.  else 80. in
    Mf_event.data
      { device_id = id; speed_kmh = speed; fuel_pct = fuel;
        position = { lat = 55.75; lon = 37.61 }; ts; device = None }
      ts)

let devices n =
  Table.of_list (List.init n (fun i ->
    Printf.sprintf "dev_%04d" i,
    { owner = "O"; max_speed = 90.; zone = "z" }))

let make_pipeline devs =
  fun source ->
    source
    |> Mf_event.with_watermarks ~latency:(seconds 3)
    |> Pipe.enrich (module Telemetry)
         ~from:devs
         ~merge:(fun t d -> { t with device = d })
    |> Pipe.window (module Telemetry) (Pipe.tumbling (seconds 30))
    |> Pipe.aggregate Test_support.Rules.compute
    |> Pipe.flat_map (Test_support.Rules.check Test_support.Rules.fleet)
    |> Pipe.dedup (module Alert)
         ~rule:(fun a -> a.rule)
         ~cooldown:(minutes 5)

(* ── Measure ─────────────────────────────────────────────── *)

let now_s () = Unix.gettimeofday ()

let run_sequential events devs =
  let t0      = now_s () in
  let count   = ref 0 in
  Stream.of_list (Array.to_list events)
  |> (make_pipeline devs)
  |> Pipe.sink (fun _ -> incr count);
  let elapsed = now_s () -. t0 in
  (!count, elapsed)

let run_parallel_n n_workers events devs =
  let t0    = now_s () in
  let count = ref 0 in
  let mu    = Mutex.create () in
  Parallel.run_parallel_simple
    ~workers:n_workers
    ~capacity:4096
    ~key_of:(fun (t:telemetry) -> t.device_id)
    ~pipeline:(make_pipeline devs)
    ~source:(Stream.of_list (Array.to_list events))
    ~sink:(fun _ ->
      Mutex.lock mu; incr count; Mutex.unlock mu)
    ();
  let elapsed = now_s () -. t0 in
  (!count, elapsed)

(* ── Main ─────────────────────────────────────────────────── *)

let () =
  let n_events  = 1_000_000 in
  let n_devices = 1_000 in
  let events    = make_events ~n:n_events ~n_devices ~step_ms:100 in
  let devs      = devices n_devices in

  Printf.printf "\n==============================================\n";
  Printf.printf "  miniFlink v3 — Parallel Benchmark\n";
  Printf.printf "  %d events · %d devices · tumbling 30s\n"
    n_events n_devices;
  Printf.printf "==============================================\n\n";

  Printf.printf "%-12s %10s %10s %10s %12s\n"
    "mode" "alerts" "time(s)" "ev/s" "speedup";
  Printf.printf "%s\n" (String.make 60 '-');

  (* Sequential baseline *)
  let (cnt_seq, t_seq) = run_sequential events devs in
  let evps_seq = float_of_int n_events /. t_seq in
  Printf.printf "%-12s %10d %10.3f %10.0f %12s\n"
    "sequential" cnt_seq t_seq evps_seq "1.0x";

  (* Parallel: 1..8 workers *)
  [1; 2; 4; 8] |> List.iter (fun w ->
    let label = Printf.sprintf "%d workers" w in
    let (cnt, t) = run_parallel_n w events devs in
    let evps    = float_of_int n_events /. t in
    let speedup = t_seq /. t in
    Printf.printf "%-12s %10d %10.3f %10.0f %11.2fx\n"
      label cnt t evps speedup
  );

  Printf.printf "\n── Channel overhead ──\n";
  (* Измерим стоимость самого channel без pipeline *)
  let n_ch = 1_000_000 in
  let ch   = Channel.make_bounded 4096 in
  let t0   = now_s () in
  let producer = Thread.create (fun () ->
    for i = 1 to n_ch do Channel.push ch i done;
    Channel.close ch
  ) () in
  let consumed = ref 0 in
  let consumer = Thread.create (fun () ->
    let rec go () =
      match Channel.pop ch with
      | None   -> ()
      | Some _ -> incr consumed; go ()
    in go ()
  ) () in
  Thread.join producer;
  Thread.join consumer;
  let t_ch = now_s () -. t0 in
  Printf.printf "  Bounded channel: %.0f msg/s (%.1f ns/msg)\n"
    (float_of_int n_ch /. t_ch)
    (t_ch *. 1e9 /. float_of_int n_ch);

  Printf.printf "\n── Scaling: разное число устройств ──\n";
  Printf.printf "%-12s %8s %8s %8s\n" "devices" "seq ev/s" "4w ev/s" "speedup";
  Printf.printf "%s\n" (String.make 40 '-');
  [100; 1000; 10000] |> List.iter (fun nd ->
    let ev2 = make_events ~n:500_000 ~n_devices:nd ~step_ms:100 in
    let dv2 = devices nd in
    let (_,ts) = run_sequential     ev2 dv2 in
    let (_,tp) = run_parallel_n 4   ev2 dv2 in
    Printf.printf "%-12d %8.0f %8.0f %7.2fx\n"
      nd
      (500_000. /. ts)
      (500_000. /. tp)
      (ts /. tp)
  );

  Printf.printf "\nDone.\n"
