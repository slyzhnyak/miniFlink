(** Глубокий профайлер triangulation pipeline.

    Цель: разбить median_rssi на компоненты и атрибутировать
    стоимость каждому шагу:
      1. dedup
      2. flat_map (reading expansion)
      3. event_time
      4. window_agg_keyed (sliding 60s/5s) — главный кандидат
      5. build_loc (triangulation per closure)

    Замеряем CUMULATIVE: каждый шаг = pipeline до этой точки. Это
    даёт точную атрибуцию marginal cost. *)

open Miniflink
open Ex07_location_lib

let scale = match Sys.getenv_opt "SCALE" with
  | Some "small"  -> `Small
  | Some "medium" -> `Medium
  | _ -> `Small

let bench_config = match scale with
  | `Small ->
    { Mock_source.Large_mine.default_config with
      horizons = 4; beacons_per_horizon = 16;
      miners_per_horizon = 32; simulation_minutes = 15 }
  | `Medium ->
    { Mock_source.Large_mine.default_config with
      horizons = 8; beacons_per_horizon = 32;
      miners_per_horizon = 64; simulation_minutes = 30 }

let n_runs = match scale with `Small -> 5 | `Medium -> 3

let find_beacon_ref : (string -> (float * float * int) option) ref =
  ref (fun _ -> None)

let prepare_events () =
  let module Src = Mock_source.Large_mine.Make (struct
    let config = bench_config
  end) in
  let packets = Src.read () |> Stream.to_list in
  let beacons = Mock_source.Large_mine.beacons_table bench_config in
  find_beacon_ref := (fun b -> Hashtbl.find_opt beacons b);
  packets

type measurement = { wall_s : float; alloc_mb : float; emitted : int }

let measure f =
  Gc.compact ();
  let s0 = Gc.quick_stat () in
  let t0 = Unix.gettimeofday () in
  let emitted = f () in
  let t1 = Unix.gettimeofday () in
  let s1 = Gc.quick_stat () in
  let bpw = Sys.word_size / 8 in
  let aw = (s1.minor_words +. s1.major_words -. s1.promoted_words)
        -. (s0.minor_words +. s0.major_words -. s0.promoted_words) in
  { wall_s = t1 -. t0;
    alloc_mb = aw *. float bpw /. 1024. /. 1024.;
    emitted }

let bench name ~runs f =
  Printf.printf "\n=== %s (%d runs) ===\n%!" name runs;
  let ms = List.init runs (fun _ -> measure f) in
  List.iteri (fun i m ->
    Printf.printf "  run %d: wall=%.3fs alloc=%.0fMB → %d emit\n%!"
      (i+1) m.wall_s m.alloc_mb m.emitted) ms;
  let walls = List.sort compare (List.map (fun m -> m.wall_s) ms) in
  let allocs = List.sort compare (List.map (fun m -> m.alloc_mb) ms) in
  let med lst = List.nth lst (runs / 2) in
  let wall = med walls in
  let alloc = med allocs in
  let emit = (List.hd ms).emitted in
  Printf.printf "  → median wall=%.3fs alloc=%.0fMB emit=%d\n%!"
    wall alloc emit;
  (wall, alloc, emit)

let drain s =
  let n = ref 0 in
  Pipe.iter_events (fun _ -> incr n) s;
  !n

(* Из ex07_location_lib.Pipelines копируем internals по частям. *)

module ByLamp = Keyed.Make (struct
  type t = Domain.packet
  let key (p : Domain.packet) = p.lamp
end)

(* Stage 1: только source + dedup *)
let stage1_dedup packets () =
  packets |> Stream.of_list
  |> Pipe.dedup (module ByLamp)
       ~rule:(fun (p : Domain.packet) -> string_of_int p.ts)
       ~cooldown:(Time.seconds 120)
  |> drain

(* Stage 2: + flat_map readings *)
let stage2_flatmap packets () =
  packets |> Stream.of_list
  |> Pipe.dedup (module ByLamp)
       ~rule:(fun (p : Domain.packet) -> string_of_int p.ts)
       ~cooldown:(Time.seconds 120)
  |> Pipe.flat_map (fun (p : Domain.packet) ->
       List.map (fun (b, rssi) ->
         { Domain.r_lamp = p.lamp; r_beacon = b; r_rssi = rssi; r_ts = p.ts })
         p.readings)
  |> drain

(* Stage 3: + event_time *)
let stage3_eventtime packets () =
  packets |> Stream.of_list
  |> Pipe.dedup (module ByLamp)
       ~rule:(fun p -> string_of_int p.Domain.ts)
       ~cooldown:(Time.seconds 120)
  |> Pipe.flat_map (fun (p : Domain.packet) ->
       List.map (fun (b, rssi) ->
         { Domain.r_lamp = p.lamp; r_beacon = b; r_rssi = rssi; r_ts = p.ts })
         p.readings)
  |> Pipe.event_time ~lateness:(Time.seconds 1)
  |> drain

(* Stage 4: + window_agg_keyed (full thing) — ГЛАВНЫЙ КАНДИДАТ *)
let stage4_window packets () =
  packets |> Stream.of_list
  |> Pipe.dedup (module ByLamp)
       ~rule:(fun p -> string_of_int p.Domain.ts)
       ~cooldown:(Time.seconds 120)
  |> Pipe.flat_map (fun (p : Domain.packet) ->
       List.map (fun (b, rssi) ->
         { Domain.r_lamp = p.lamp; r_beacon = b; r_rssi = rssi; r_ts = p.ts })
         p.readings)
  |> Pipe.event_time ~lateness:(Time.seconds 1)
  |> Pipe.window_agg_keyed ~by:(fun r -> r.Domain.r_lamp)
       ~allowed_lateness:(Time.seconds 60)
       (Pipe.sliding (Time.seconds 60) (Time.seconds 5))
       Agg.(
         group_by
           ~key:(fun r -> r.Domain.r_beacon)
           ~inner:(median (fun (r : Domain.reading) -> r.r_rssi))
         |> map (fun per_beacon ->
              per_beacon
              |> List.filter_map (fun (b, med) ->
                   match med with Some m -> Some (b, m) | None -> None)
              |> Agg.run (top_k_by 2 ~by:snd)))
  |> drain

(* Stage 4 ALT: то же но с approx_median (P² algorithm, O(1) state). *)
let stage4_window_approx packets () =
  packets |> Stream.of_list
  |> Pipe.dedup (module ByLamp)
       ~rule:(fun p -> string_of_int p.Domain.ts)
       ~cooldown:(Time.seconds 120)
  |> Pipe.flat_map (fun (p : Domain.packet) ->
       List.map (fun (b, rssi) ->
         { Domain.r_lamp = p.lamp; r_beacon = b; r_rssi = rssi; r_ts = p.ts })
         p.readings)
  |> Pipe.event_time ~lateness:(Time.seconds 1)
  |> Pipe.window_agg_keyed ~by:(fun r -> r.Domain.r_lamp)
       ~allowed_lateness:(Time.seconds 60)
       (Pipe.sliding (Time.seconds 60) (Time.seconds 5))
       Agg.(
         group_by
           ~key:(fun r -> r.Domain.r_beacon)
           ~inner:(approx_median (fun (r : Domain.reading) -> r.r_rssi))
         |> map (fun per_beacon ->
              per_beacon
              |> List.filter_map (fun (b, med) ->
                   match med with Some m -> Some (b, m) | None -> None)
              |> Agg.run (top_k_by 2 ~by:snd)))
  |> drain

(* Stage 4 ALT 2: mean вместо median (O(1) state, sum + count). *)
let stage4_window_mean packets () =
  packets |> Stream.of_list
  |> Pipe.dedup (module ByLamp)
       ~rule:(fun p -> string_of_int p.Domain.ts)
       ~cooldown:(Time.seconds 120)
  |> Pipe.flat_map (fun (p : Domain.packet) ->
       List.map (fun (b, rssi) ->
         { Domain.r_lamp = p.lamp; r_beacon = b; r_rssi = rssi; r_ts = p.ts })
         p.readings)
  |> Pipe.event_time ~lateness:(Time.seconds 1)
  |> Pipe.window_agg_keyed ~by:(fun r -> r.Domain.r_lamp)
       ~allowed_lateness:(Time.seconds 60)
       (Pipe.sliding (Time.seconds 60) (Time.seconds 5))
       Agg.(
         group_by
           ~key:(fun r -> r.Domain.r_beacon)
           ~inner:(mean (fun (r : Domain.reading) -> r.r_rssi))
         |> map (fun per_beacon ->
              per_beacon
              |> List.filter_map (fun (b, m) ->
                   match m with Some x -> Some (b, x) | None -> None)
              |> Agg.run (top_k_by 2 ~by:snd)))
  |> drain

(* Stage 5: полный median_rssi с триангуляцией *)
let stage5_full packets () =
  packets |> Stream.of_list
  |> Pipelines.median_rssi ~find_beacon:!find_beacon_ref
  |> drain

(* Дополнительные изоляции: только window БЕЗ aggregation *)
let stage_window_only packets () =
  packets |> Stream.of_list
  |> Pipe.flat_map (fun (p : Domain.packet) ->
       List.map (fun (b, rssi) ->
         { Domain.r_lamp = p.lamp; r_beacon = b; r_rssi = rssi; r_ts = p.ts })
         p.readings)
  |> Pipe.event_time ~lateness:(Time.seconds 1)
  |> Pipe.window_agg_keyed ~by:(fun r -> r.Domain.r_lamp)
       ~allowed_lateness:(Time.seconds 60)
       (Pipe.sliding (Time.seconds 60) (Time.seconds 5))
       Agg.count  (* trivial: count events *)
  |> drain

(* Только flat_map + dedup, без windowing — base cost reading expansion *)
let stage_readings_only packets () =
  packets |> Stream.of_list
  |> Pipe.flat_map (fun (p : Domain.packet) ->
       List.map (fun (b, rssi) ->
         { Domain.r_lamp = p.lamp; r_beacon = b; r_rssi = rssi; r_ts = p.ts })
         p.readings)
  |> drain

let () =
  Printf.printf "==========================================\n";
  Printf.printf " Triangulation deep profile (scale=%s)\n"
    (match scale with `Small -> "small" | `Medium -> "medium");
  Printf.printf "==========================================\n";
  let packets = prepare_events () in
  Printf.printf "Events: %d packets\n%!" (List.length packets);

  let cumulative = [
    ("stage 1: dedup",                    stage1_dedup packets);
    ("stage 2: + flat_map",               stage2_flatmap packets);
    ("stage 3: + event_time",             stage3_eventtime packets);
    ("stage 4: + window_agg_keyed",       stage4_window packets);
    ("stage 5: + triangulate (full)",     stage5_full packets);
  ] in

  let isolated = [
    ("iso: flat_map only (readings)",     stage_readings_only packets);
    ("iso: window only (trivial agg)",    stage_window_only packets);
    ("alt: stage4 with approx_median",    stage4_window_approx packets);
    ("alt: stage4 with mean",             stage4_window_mean packets);
  ] in

  Printf.printf "\n┌─── CUMULATIVE: marginal cost per stage ───\n";
  let results = List.map (fun (name, f) ->
    let r = bench name ~runs:n_runs f in
    (name, r)
  ) cumulative in

  Printf.printf "\n┌─── ISOLATED: specific component ───\n";
  let iso_results = List.map (fun (name, f) ->
    let r = bench name ~runs:n_runs f in
    (name, r)
  ) isolated in

  Printf.printf "\n========================================\n";
  Printf.printf " SUMMARY (cumulative)\n";
  Printf.printf "========================================\n";
  Printf.printf "%-40s %10s %10s %12s\n"
    "Stage" "wall(s)" "alloc(MB)" "Δalloc(MB)";
  Printf.printf "%s\n" (String.make 80 '-');
  let prev_alloc = ref 0.0 in
  List.iter (fun (name, (wall, alloc, _)) ->
    let delta = alloc -. !prev_alloc in
    Printf.printf "%-40s %10.3f %10.0f %12.0f\n"
      name wall alloc delta;
    prev_alloc := alloc
  ) results;

  Printf.printf "\n========================================\n";
  Printf.printf " SUMMARY (isolated)\n";
  Printf.printf "========================================\n";
  Printf.printf "%-40s %10s %10s\n"
    "Component" "wall(s)" "alloc(MB)";
  Printf.printf "%s\n" (String.make 65 '-');
  List.iter (fun (name, (wall, alloc, _)) ->
    Printf.printf "%-40s %10.3f %10.0f\n" name wall alloc
  ) iso_results
