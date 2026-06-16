(** Per-component profiler.

    Цель: понять где в production-like pipeline (с triangulation +
    gas enrichment) тратится время и память. Запускаем каждый
    компонент в изоляции, замеряем wall + alloc + peak heap.

    Изолированный замер: Stream.iter дренит выход компонента,
    предыдущий stream восстанавливается из листа на каждый прогон.
*)

open Miniflink
open Ex07_location_lib

(* ── Конфиг ───────────────────────────────────────────────── *)

let scale = match Sys.getenv_opt "SCALE" with
  | Some "small"  -> `Small
  | Some "medium" -> `Medium
  | Some "large"  -> `Large
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
  | `Large ->
    Mock_source.Large_mine.default_config

let n_runs = match scale with
  | `Small -> 5 | `Medium -> 3 | `Large -> 2

(* ── Подготовка events ────────────────────────────────────── *)

let find_beacon_ref : (string -> (float * float * int) option) ref =
  ref (fun _ -> None)

let prepare_events () =
  let module Src = Mock_source.Large_mine.Make (struct
    let config = bench_config
  end) in
  let packets = Src.read () |> Stream.to_list in
  let gas = Src.read_gas () |> Stream.to_list in
  let beacons = Mock_source.Large_mine.beacons_table bench_config in
  find_beacon_ref := (fun b -> Hashtbl.find_opt beacons b);
  (packets, gas)

(* ── Замер ─────────────────────────────────────────────────── *)

type measurement = {
  wall_s        : float;
  allocated_mb  : float;
  emitted       : int;
}

let measure name f =
  Gc.compact ();
  let stats0 = Gc.quick_stat () in
  let t0 = Unix.gettimeofday () in
  let emitted = f () in
  let t1 = Unix.gettimeofday () in
  let stats1 = Gc.quick_stat () in
  let bytes_per_word = Sys.word_size / 8 in
  let alloc_words =
    (stats1.minor_words +. stats1.major_words -. stats1.promoted_words)
    -. (stats0.minor_words +. stats0.major_words -. stats0.promoted_words) in
  let alloc_mb = alloc_words *. float bytes_per_word /. 1024. /. 1024. in
  let m = { wall_s = t1 -. t0; allocated_mb = alloc_mb; emitted } in
  Printf.printf "  %s: wall=%.2fs alloc=%.0fMB → %d emit\n%!"
    name m.wall_s m.allocated_mb m.emitted;
  m

let bench_runs name ~runs f =
  Printf.printf "\n=== %s (%d runs) ===\n%!" name runs;
  let ms = List.init runs (fun i ->
    measure (Printf.sprintf "  run %d" (i+1)) f) in
  let walls = List.sort compare (List.map (fun m -> m.wall_s) ms) in
  let allocs = List.sort compare (List.map (fun m -> m.allocated_mb) ms) in
  let median lst = List.nth lst (runs / 2) in
  let wall_med = median walls in
  let alloc_med = median allocs in
  Printf.printf "  → median wall=%.2fs alloc=%.0fMB\n%!"
    wall_med alloc_med;
  (wall_med, alloc_med, (List.hd ms).emitted)

(* ── Компоненты, изолированно ────────────────────────────── *)

(* Pipeline-level helper: дренит весь output stream, считает Data events. *)
let drain stream =
  let n = ref 0 in
  Pipe.iter_data (fun _ -> incr n) stream;
  !n

(* Компонент 0: только parsing — материализация stream и iter без
   pipeline. Базовая стоимость "просто пройти через events". *)
let component_baseline packets gas () =
  let n = ref 0 in
  packets |> Stream.of_list
  |> Pipe.iter_events (fun _ -> incr n);
  gas |> Stream.of_list
  |> Pipe.iter_events (fun _ -> incr n);
  !n

(* Компонент 1: voltage trigger. *)
let component_voltage_trigger packets _gas () =
  let trigger = Trigger.create
    ~name:"low_voltage"
    ~condition:(Trigger.less_than 3.5)
    ~problem_for:(Time.minutes 2)
    ~severity:Trigger.Warning
    ~produce_alert:(fun ~key ~value ~ts -> (key, value, ts))
    ~produce_recovery:(fun ~key ~ts -> (key, 0.0, ts))
    () in
  packets |> Stream.of_list
  |> Pipe.event_time ~lateness:(Time.seconds 1)
  |> Pipe.map (fun (p : Domain.packet) -> (p.lamp, p.voltage))
  |> Trigger.of_stream trigger
  |> drain

(* Компонент 2: CO trigger. *)
let component_co_trigger _packets gas () =
  let trigger = Trigger.create
    ~name:"high_co"
    ~condition:(Trigger.greater_than 50.0)
    ~problem_for:(Time.minutes 1)
    ~severity:Trigger.High
    ~produce_alert:(fun ~key ~value ~ts -> (key, value, ts))
    ~produce_recovery:(fun ~key ~ts -> (key, 0.0, ts))
    () in
  gas |> Stream.of_list
  |> Pipe.event_time ~lateness:(Time.seconds 1)
  |> Pipe.map (fun (g : Domain.gas_packet) ->
      (g.g_lamp, Option.value g.g_co ~default:0.0))
  |> Trigger.of_stream trigger
  |> drain

(* Компонент 3: silence_age. *)
let component_silence packets _gas () =
  packets |> Stream.of_list
  |> Pipe.event_time ~lateness:(Time.seconds 1)
  |> Item.silence_age
       ~by:(fun (p : Domain.packet) -> p.lamp)
       ~tick:(Time.seconds 30)
  |> drain

(* Компонент 4: median_rssi (triangulation). ГЛАВНЫЙ КАНДИДАТ. *)
let component_triangulation packets _gas () =
  packets |> Stream.of_list
  |> Pipelines.median_rssi ~find_beacon:!find_beacon_ref
  |> drain

(* Компонент 5: gas_alerts enrichment (depends on triangulation). *)
let component_gas_enrichment packets gas () =
  let s_rssi = packets |> Stream.of_list in
  let s_loc = packets |> Stream.of_list
    |> Pipelines.median_rssi ~find_beacon:!find_beacon_ref in
  let s_gas = gas |> Stream.of_list in
  Pipelines.gas_alerts ~find_beacon:!find_beacon_ref
    ~rssi:s_rssi ~locations:s_loc ~gas:s_gas ()
  |> drain

(* Компонент 6: combined FSM через keyed_join + process_keyed. *)
type combined_fsm = {
  mutable v : float; mutable c : float; mutable r : float;
  mutable critical : int;
}

let component_combined_fsm packets gas () =
  let voltage =
    packets |> Stream.of_list
    |> Pipe.event_time ~lateness:(Time.seconds 1)
    |> Pipe.map (fun (p : Domain.packet) -> (p.lamp, p.voltage)) in
  let co =
    gas |> Stream.of_list
    |> Pipe.event_time ~lateness:(Time.seconds 1)
    |> Pipe.map (fun (g : Domain.gas_packet) ->
        (g.g_lamp, Option.value g.g_co ~default:0.0)) in
  let rssi =
    packets |> Stream.of_list
    |> Pipe.event_time ~lateness:(Time.seconds 1)
    |> Pipe.map (fun (p : Domain.packet) ->
        let n = List.length p.readings in
        let avg = if n = 0 then -100.0
                  else List.fold_left (fun s (_,r) -> s+.r) 0.0 p.readings
                       /. float n in
        (p.lamp, avg)) in
  let module By_lamp : Keyed.S with type t = string * float = struct
    type t = string * float
    let key (k, _) = k
  end in
  let module Combined : Keyed.S
    with type t = string * (string * float) option list = struct
    type t = string * (string * float) option list
    let key (k, _) = k
  end in
  Pipe.keyed_join (module By_lamp) [voltage; co; rssi]
  |> Pipe.process_keyed (module Combined)
       ~init:(fun () -> { v = 4.0; c = 0.0; r = -50.0; critical = 0 })
       ~on_event:(fun ctx key st (_k, opts) ->
         (match opts with
          | [vo; co; ro] ->
            (match vo with Some (_, v) -> st.v <- v | None -> ());
            (match co with Some (_, c) -> st.c <- c | None -> ());
            (match ro with Some (_, r) -> st.r <- r | None -> ())
          | _ -> ());
         if st.v < 3.5 && st.c > 50.0 && st.r < -75.0 && st.critical = 0
         then begin st.critical <- 1; ctx.emit key end
         else if not (st.v < 3.5 && st.c > 50.0 && st.r < -75.0)
              && st.critical > 0 then st.critical <- 0)
       ~on_timer:(fun _ _ _ _ _ -> ())
  |> drain

(* Компонент 7: window_fold voltage sum. *)
let component_window_fold packets _gas () =
  let module Voltage_keyed : Keyed.S with type t = string * float = struct
    type t = string * float
    let key (k, _) = k
  end in
  packets |> Stream.of_list
  |> Pipe.event_time ~lateness:(Time.seconds 1)
  |> Pipe.map (fun (p : Domain.packet) -> (p.lamp, p.voltage))
  |> Pipe.window_fold (module Voltage_keyed)
       (Pipe.tumbling (Time.minutes 1))
       ~init:(fun () -> 0.0)
       ~add:(fun acc (_, v) -> acc +. v)
  |> drain

(* ── Главный сценарий ────────────────────────────────────── *)

let () =
  Printf.printf "==========================================\n";
  Printf.printf " Per-component profiler (scale=%s)\n"
    (match scale with `Small -> "small" | `Medium -> "medium" | `Large -> "large");
  Printf.printf "==========================================\n";
  Printf.printf "Config: %d horizons × %d miners, %d min sim\n"
    bench_config.horizons bench_config.miners_per_horizon
    bench_config.simulation_minutes;
  Printf.printf "\nPreparing events...\n%!";
  let t0 = Unix.gettimeofday () in
  let packets, gas = prepare_events () in
  let t1 = Unix.gettimeofday () in
  let total_events = List.length packets + List.length gas in
  Printf.printf "  %d packets + %d gas = %d total (%.2fs)\n%!"
    (List.length packets) (List.length gas) total_events (t1 -. t0);

  let components = [
    ("0. baseline iter",     fun () -> component_baseline packets gas ());
    ("1. voltage trigger",   fun () -> component_voltage_trigger packets gas ());
    ("2. co trigger",        fun () -> component_co_trigger packets gas ());
    ("3. silence_age",       fun () -> component_silence packets gas ());
    ("4. triangulation",     fun () -> component_triangulation packets gas ());
    ("5. gas_enrichment",    fun () -> component_gas_enrichment packets gas ());
    ("6. combined FSM",      fun () -> component_combined_fsm packets gas ());
    ("7. window_fold",       fun () -> component_window_fold packets gas ());
  ] in

  let results = List.map (fun (name, f) ->
    let r = bench_runs name ~runs:n_runs f in
    (name, r)
  ) components in

  Printf.printf "\n========================================\n";
  Printf.printf " SUMMARY (scale=%s, %d events total)\n"
    (match scale with `Small -> "small" | `Medium -> "medium" | `Large -> "large")
    total_events;
  Printf.printf "========================================\n\n";
  Printf.printf "%-30s %10s %12s %10s\n"
    "Component" "wall(s)" "alloc(MB)" "thpt(K/s)";
  Printf.printf "%s\n" (String.make 70 '-');
  List.iter (fun (name, (wall, alloc, _emit)) ->
    let thpt = float total_events /. wall /. 1000.0 in
    Printf.printf "%-30s %10.2f %12.0f %10.0f\n"
      name wall alloc thpt
  ) results;

  Printf.printf "\nAllocation per event (median):\n";
  List.iter (fun (name, (_, alloc, _)) ->
    let bytes_per_ev = alloc *. 1024.0 *. 1024.0 /. float total_events in
    Printf.printf "  %-30s %.0f bytes/event\n" name bytes_per_ev
  ) results
