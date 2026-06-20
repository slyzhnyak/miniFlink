(** Benchmark производительности персистентности.

    Замеряет накладные расходы persistence для каждого из четырёх
    stateful операторов в отдельности и всех вместе. Базовый
    workload — пакетный поток + газовые пакеты на масштабе типичной
    шахты (Mock_source.Large_mine).

    Сценарии:
      A: baseline (без persistence)
      B: только Trigger с persistence
      C: только silence_age с persistence
      D: только process_keyed с persistence
      E: только window_fold с persistence
      F: всё включено (full minePASS-like)
      G: восстановление (restore) после фазы 1

    Метрики:
      - wall-time (секунды)
      - throughput (events/sec)
      - allocated bytes
      - heap peak (bytes)
      - backend writes (вызовы set)
      - backend size (записей + bytes)
*)

open Miniflink
open Ex07_location_lib

(* ── Конфигурация: масштаб ─────────────────────────────────── *)

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
  | `Small -> 5
  | `Medium -> 3
  | `Large -> 2

(* ── Backend с метриками ───────────────────────────────────── *)

type counted_backend = {
  inner : Persistence_backend.t;
  mutable gets : int;
  mutable sets : int;
  mutable deletes : int;
  mutable bytes_written : int;
}

let make_counted () : counted_backend =
  let tbl = Hashtbl.create 4096 in
  let inner = Persistence_backend.of_memory tbl in
  let cb = {
    inner;
    gets = 0; sets = 0; deletes = 0; bytes_written = 0;
  } in
  cb

(* Persistence_backend.t с подсчётом операций *)
let as_backend (cb : counted_backend) : Persistence_backend.t = {
  get    = (fun k ->
    cb.gets <- cb.gets + 1;
    cb.inner.get k);
  set    = (fun k v ->
    cb.sets <- cb.sets + 1;
    cb.bytes_written <- cb.bytes_written + Bytes.length v;
    cb.inner.set k v);
  delete = (fun k ->
    cb.deletes <- cb.deletes + 1;
    cb.inner.delete k);
  keys   = cb.inner.keys;
}

let backend_size (cb : counted_backend) : int =
  List.length (cb.inner.keys ())

(* ── Подготовка events ─────────────────────────────────────── *)

(* Mock_source даёт packet stream + gas stream + таблицу beacon'ов
   для триангуляции. Материализуем events в списки чтобы можно было
   прогнать пайплайн многократно. *)

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

(* ── Пайплайны (без и с persistence per operator) ──────────── *)

(* Combined FSM state: реалистичный minePASS use case — FSM держит
   последние значения voltage/co/rssi per шахтёра и срабатывает на
   критическом сочетании. Используется на выходе keyed_join'а.

   Это даёт более точную картину process_keyed overhead с
   нетривиальным state (4 поля + критичность). *)
type combined_fsm = {
  mutable voltage : float;
  mutable co      : float;
  mutable rssi    : float;
  mutable critical_since : Time.t;  (* 0 = не критично *)
}

let combined_to_json s =
  `Assoc [
    ("voltage", `Float s.voltage);
    ("co", `Float s.co);
    ("rssi", `Float s.rssi);
    ("critical_since", `Int s.critical_since);
  ]

let combined_of_json = function
  | `Assoc kv ->
    let f n = match List.assoc n kv with
      | `Float f -> f | `Int i -> float i | _ -> 0.0 in
    { voltage = f "voltage";
      co = f "co";
      rssi = f "rssi";
      critical_since =
        (match List.assoc "critical_since" kv with
         | `Int i -> i | _ -> 0); }
  | _ -> failwith "bad combined"

(* Алерты — общий тип для всех триггеров. *)
type alert =
  | A_low_voltage of string * float * Time.t
  | A_recovery    of string * Time.t
  | A_high_co     of string * float * Time.t
  | A_co_recovery of string * Time.t
  | A_no_packets  of string * Time.t

let alert_to_json = function
  | A_low_voltage (k, v, t) ->
    `Assoc [("tag",`String "lv");("k",`String k);("v",`Float v);("t",`Int t)]
  | A_recovery (k, t) ->
    `Assoc [("tag",`String "vok");("k",`String k);("t",`Int t)]
  | A_high_co (k, v, t) ->
    `Assoc [("tag",`String "co");("k",`String k);("v",`Float v);("t",`Int t)]
  | A_co_recovery (k, t) ->
    `Assoc [("tag",`String "cook");("k",`String k);("t",`Int t)]
  | A_no_packets (k, t) ->
    `Assoc [("tag",`String "np");("k",`String k);("t",`Int t)]

let alert_of_json _ = A_recovery ("?", 0)  (* placeholder for trigger param *)

(* Низкое напряжение trigger spec. С backend если bk = Some, иначе без. *)
let mk_low_voltage_trigger ?backend () =
  let backend_opt = backend in
  Trigger.create
    ~name:"low_voltage_bench"
    ~condition:(Trigger.less_than 3.5)
    ~problem_for:(Time.minutes 2)
    ~severity:Trigger.Warning
    ~produce_alert:(fun ~key ~value ~ts -> A_low_voltage (key, value, ts))
    ~produce_recovery:(fun ~key ~ts -> A_recovery (key, ts))
    ?serialize_key:(if backend_opt <> None
                    then Some (fun k -> `String k) else None)
    ?deserialize_key:(if backend_opt <> None
                      then Some (fun j -> Yojson.Safe.Util.to_string j)
                      else None)
    ?serialize_value:(if backend_opt <> None
                      then Some (fun v -> `Float v) else None)
    ?deserialize_value:(if backend_opt <> None
                        then Some (function `Float v -> v
                                          | _ -> failwith "bad")
                        else None)
    ?serialize_alert:(if backend_opt <> None
                      then Some alert_to_json else None)
    ?deserialize_alert:(if backend_opt <> None
                        then Some alert_of_json else None)
    ()

(* CO trigger spec. *)
let mk_co_trigger ?backend () =
  let backend_opt = backend in
  Trigger.create
    ~name:"high_co_bench"
    ~condition:(Trigger.greater_than 50.0)
    ~problem_for:(Time.minutes 1)
    ~severity:Trigger.High
    ~produce_alert:(fun ~key ~value ~ts -> A_high_co (key, value, ts))
    ~produce_recovery:(fun ~key ~ts -> A_co_recovery (key, ts))
    ?serialize_key:(if backend_opt <> None
                    then Some (fun k -> `String k) else None)
    ?deserialize_key:(if backend_opt <> None
                      then Some (fun j -> Yojson.Safe.Util.to_string j)
                      else None)
    ?serialize_value:(if backend_opt <> None
                      then Some (fun v -> `Float v) else None)
    ?deserialize_value:(if backend_opt <> None
                        then Some (function `Float v -> v
                                          | _ -> failwith "bad")
                        else None)
    ?serialize_alert:(if backend_opt <> None
                      then Some alert_to_json else None)
    ?deserialize_alert:(if backend_opt <> None
                        then Some alert_of_json else None)
    ()

(* Pipeline (РЕАЛИСТИЧНЫЙ minePASS с триангуляцией и enriched gas):

   ┌─→ voltage_stream ──→ Trigger (low_voltage) ──→ voltage_alerts
   │                  ╲
   │                   ╲→ window_fold (voltage sum tumbling 1min)
   │
   ├─→ silence_age ──→ silence_stream
   │
   ├─→ Pipelines.median_rssi ──→ location_stream (с triangulated позицией)
   │                                │
   │                                ↓
   ├──── rssi raw ────────────→ Pipelines.gas_alerts → enriched gas_alerts
   gas─→──────────────────────→     (gas + location)    (с координатами)
   │
   └─→ keyed_join [voltage; co; rssi] ──→ process_keyed (combined FSM)
                                              ↓
                                       evacuation alerts

   Замечания о persistence:
   - Trigger, silence_age, process_keyed, window_fold подключены к
     backend через ?backend параметры
   - median_rssi внутри использует window_agg_keyed (Agg.t existential
     acc) — persistence НЕ поддерживается напрямую (требует Agg.t
     refactor; задокументировано как TODO в expressiveness branch)
   - gas_alerts использует manual Hashtbl для enrichment state —
     тоже без persistence (нестандартная retract семантика)

   То есть в production minePASS реально persisted получаются 4 из 6
   stateful компонентов. Этот benchmark показывает overhead на этих 4. *)
let run_pipeline ?trigger_bk ?silence_bk ?process_bk ?window_bk
    ?find_beacon
    (packets : Domain.packet Mf_event.t list)
    (gas : Domain.gas_packet Mf_event.t list)
  : int =
  let open Ex07_location_lib in
  let find_beacon = match find_beacon with
    | Some f -> f
    | None -> !find_beacon_ref in
  (* Module для keyed-join — values имеют тип (string * float). *)
  let module By_lamp : Keyed.S with type t = string * float = struct
    type t = string * float
    let key (k, _) = k
  end in

  let voltage_stream =
    packets |> Stream.of_list
    |> Pipe.event_time ~lateness:(Time.seconds 1)
    |> Pipe.map (fun (p : Domain.packet) -> (p.lamp, p.voltage)) in

  let co_stream =
    gas |> Stream.of_list
    |> Pipe.event_time ~lateness:(Time.seconds 1)
    |> Pipe.map (fun (g : Domain.gas_packet) ->
        (g.g_lamp, Option.value g.g_co ~default:0.0)) in

  let rssi_stream =
    packets |> Stream.of_list
    |> Pipe.event_time ~lateness:(Time.seconds 1)
    |> Pipe.map (fun (p : Domain.packet) ->
        let n = List.length p.readings in
        let avg = if n = 0 then -100.0
                  else List.fold_left (fun s (_, r) -> s +. r) 0.0 p.readings
                       /. float n in
        (p.lamp, avg)) in

  let packet_keyed =
    packets |> Stream.of_list
    |> Pipe.event_time ~lateness:(Time.seconds 1) in

  let voltage_alerts =
    voltage_stream
    |> Trigger.of_stream ?backend:trigger_bk
         (mk_low_voltage_trigger ?backend:trigger_bk ()) in

  let co_alerts =
    co_stream
    |> Trigger.of_stream ?backend:trigger_bk
         (mk_co_trigger ?backend:trigger_bk ()) in

  let silence_stream =
    packet_keyed
    |> Item.silence_age
         ?persistence:(Option.map (fun bk ->
           { Persistence_backend.backend = bk;
             name = "bench_silence";
             serialize = ((fun k -> `String k));
             deserialize = ((fun j -> Yojson.Safe.Util.to_string j)) }) silence_bk)
         ~by:(fun (p : Domain.packet) -> p.lamp)
         ~tick:(Time.seconds 30) in

  (* ── ТРИАНГУЛЯЦИЯ + ENRICHED GAS ALERTS ─────────────────────
     Используем готовые ex07 pipelines.
     Эти операторы (window_agg_keyed + gas_alerts manual hashtbl)
     persistence не поддерживают — это документированное ограничение. *)
  let packets_for_loc = packets |> Stream.of_list in
  let location_stream =
    Pipelines.median_rssi ~find_beacon packets_for_loc in

  let packets_for_rssi = packets |> Stream.of_list in
  let gas_for_enrich = gas |> Stream.of_list in
  let enriched_gas_alerts =
    Pipelines.gas_alerts ~find_beacon
      ~rssi:packets_for_rssi
      ~locations:location_stream
      ~gas:gas_for_enrich
      () in

  (* ── COMBINED FSM ──────────────────────────────────────────── *)
  let combined_stream =
    Pipe.keyed_join (module By_lamp)
      [voltage_stream; co_stream; rssi_stream] in

  let module Combined_keyed
    : Keyed.S with type t = string * (string * float) option list = struct
    type t = string * (string * float) option list
    let key (k, _) = k
  end in

  let evacuation_alerts =
    combined_stream
    |> Pipe.process_keyed (module Combined_keyed)
         ?persistence:(Option.map (fun bk ->
           { Persistence_backend.backend = bk;
             name = "bench_combined_fsm";
             serialize = combined_to_json;
             deserialize = combined_of_json }) process_bk)
         ~init:(fun () ->
           { voltage = 4.0; co = 0.0; rssi = -50.0; critical_since = 0 })
         ~on_event:(fun ctx key st (_k, opts) ->
           (match opts with
            | [v_opt; c_opt; r_opt] ->
              (match v_opt with Some (_, v) -> st.voltage <- v | None -> ());
              (match c_opt with Some (_, c) -> st.co <- c | None -> ());
              (match r_opt with Some (_, r) -> st.rssi <- r | None -> ())
            | _ -> ());
           let is_critical =
             st.voltage < 3.5 && st.co > 50.0 && st.rssi < -75.0 in
           if is_critical && st.critical_since = 0 then begin
             st.critical_since <- 1;
             ctx.emit (A_no_packets (key, st.critical_since))
           end else if not is_critical && st.critical_since > 0 then
             st.critical_since <- 0)
         ~on_timer:(fun _ _ _ _ _ -> ()) in

  let module Voltage_keyed : Keyed.S with type t = string * float = struct
    type t = string * float
    let key (k, _) = k
  end in

  let voltage_sum =
    voltage_stream
    |> Pipe.window_fold (module Voltage_keyed)
         ?persistence:(Option.map (fun bk ->
           { Persistence_backend.backend = bk;
             name = "bench_vsum";
             serialize = ((fun (f : float) -> `Float f));
             deserialize = ((function `Float f -> f | _ -> 0.0)) }) window_bk)
         (Pipe.tumbling (Time.minutes 1))
         ~init:(fun () -> 0.0)
         ~add:(fun acc (_, v) -> acc +. v) in

  let n = ref 0 in
  Pipe.iter_data (fun _ -> incr n) voltage_alerts;
  Pipe.iter_data (fun _ -> incr n) co_alerts;
  Pipe.iter_data (fun _ -> incr n) silence_stream;
  Pipe.iter_data (fun _ -> incr n) evacuation_alerts;
  Pipe.iter_data (fun _ -> incr n) voltage_sum;
  Pipe.iter_data (fun _ -> incr n) enriched_gas_alerts;
  !n

(* ── Замер: один прогон ────────────────────────────────────── *)

type measurement = {
  wall_s        : float;
  allocated_mb  : float;
  heap_peak_mb  : float;
  events_total  : int;
  result_count  : int;
}

let measure name ~packets ~gas pipeline_fn =
  Gc.compact ();
  let stats_before = Gc.quick_stat () in
  let t0 = Unix.gettimeofday () in

  let result = pipeline_fn ~packets ~gas in

  let t1 = Unix.gettimeofday () in
  let stats_after = Gc.quick_stat () in

  let bytes_per_word = Sys.word_size / 8 in
  let allocated_words =
    (stats_after.minor_words +. stats_after.major_words
     -. stats_after.promoted_words)
    -. (stats_before.minor_words +. stats_before.major_words
        -. stats_before.promoted_words) in
  let allocated_bytes =
    allocated_words *. float_of_int bytes_per_word in
  let heap_peak_bytes = stats_after.heap_words * bytes_per_word in

  let events_total = List.length packets + List.length gas in
  let m = {
    wall_s = t1 -. t0;
    allocated_mb = allocated_bytes /. 1024.0 /. 1024.0;
    heap_peak_mb = float_of_int heap_peak_bytes /. 1024.0 /. 1024.0;
    events_total;
    result_count = result;
  } in
  Printf.printf "  %s: wall=%.2fs alloc=%.0fMB heap=%.0fMB %d events → %d alerts (%.0fK ev/s)\n%!"
    name m.wall_s m.allocated_mb m.heap_peak_mb
    m.events_total m.result_count
    (float events_total /. m.wall_s /. 1000.0);
  m

(* Запустить N раз, вывести median и p95. *)
let bench_runs name ~packets ~gas ~runs pipeline_fn =
  Printf.printf "\n=== %s (%d runs) ===\n%!" name runs;
  let ms = ref [] in
  for i = 1 to runs do
    let label = Printf.sprintf "  run %d" i in
    let m = measure label ~packets ~gas pipeline_fn in
    ignore i;
    ms := m :: !ms
  done;
  let walls = List.map (fun m -> m.wall_s) !ms in
  let sorted = List.sort compare walls in
  let median = List.nth sorted (runs / 2) in
  let min_wall = List.hd sorted in
  let max_wall = List.nth sorted (runs - 1) in
  Printf.printf "  → median=%.2fs min=%.2fs max=%.2fs\n%!" median min_wall max_wall;
  (median, min_wall, max_wall)

(* ── Сценарии ──────────────────────────────────────────────── *)

let scenario_a ~packets ~gas =
  bench_runs "A. Baseline (no persistence)" ~packets ~gas ~runs:n_runs
    (fun ~packets ~gas -> run_pipeline packets gas)

let scenario_b ~packets ~gas =
  let cb = ref (make_counted ()) in
  let result = bench_runs "B. Trigger only persisted" ~packets ~gas ~runs:n_runs
    (fun ~packets ~gas ->
       cb := make_counted ();
       run_pipeline ~trigger_bk:(as_backend !cb) packets gas) in
  Printf.printf "  backend: %d sets, %d records, %.0f KB\n%!"
    !cb.sets (backend_size !cb)
    (float !cb.bytes_written /. 1024.0);
  result

let scenario_c ~packets ~gas =
  let cb = ref (make_counted ()) in
  let result = bench_runs "C. silence_age only persisted" ~packets ~gas ~runs:n_runs
    (fun ~packets ~gas ->
       cb := make_counted ();
       run_pipeline ~silence_bk:(as_backend !cb) packets gas) in
  Printf.printf "  backend: %d sets, %d records, %.0f KB\n%!"
    !cb.sets (backend_size !cb)
    (float !cb.bytes_written /. 1024.0);
  result

let scenario_d ~packets ~gas =
  let cb = ref (make_counted ()) in
  let result = bench_runs "D. process_keyed only persisted" ~packets ~gas ~runs:n_runs
    (fun ~packets ~gas ->
       cb := make_counted ();
       run_pipeline ~process_bk:(as_backend !cb) packets gas) in
  Printf.printf "  backend: %d sets, %d records, %.0f KB\n%!"
    !cb.sets (backend_size !cb)
    (float !cb.bytes_written /. 1024.0);
  result

let scenario_e ~packets ~gas =
  let cb = ref (make_counted ()) in
  let result = bench_runs "E. window_fold only persisted" ~packets ~gas ~runs:n_runs
    (fun ~packets ~gas ->
       cb := make_counted ();
       run_pipeline ~window_bk:(as_backend !cb) packets gas) in
  Printf.printf "  backend: %d sets, %d records, %.0f KB\n%!"
    !cb.sets (backend_size !cb)
    (float !cb.bytes_written /. 1024.0);
  result

let scenario_f ~packets ~gas =
  let cb = ref (make_counted ()) in
  let result = bench_runs "F. All operators persisted (full)" ~packets ~gas ~runs:n_runs
    (fun ~packets ~gas ->
       cb := make_counted ();
       let bk = as_backend !cb in
       run_pipeline
         ~trigger_bk:bk ~silence_bk:bk
         ~process_bk:bk ~window_bk:bk
         packets gas) in
  Printf.printf "  backend (shared): %d sets, %d records, %.0f KB\n%!"
    !cb.sets (backend_size !cb)
    (float !cb.bytes_written /. 1024.0);
  result

let scenario_g ~packets ~gas =
  Printf.printf "\n=== G. Recovery scenario ===\n%!";
  (* Phase 1: первая половина events, заполняем backend *)
  let cb = make_counted () in
  let bk = as_backend cb in
  let half = List.length packets / 2 in
  let p1 = List.filteri (fun i _ -> i < half) packets in
  let g1 = List.filteri (fun i _ -> i < List.length gas / 2) gas in
  let p2 = List.filteri (fun i _ -> i >= half) packets in
  let g2 = List.filteri (fun i _ -> i >= List.length gas / 2) gas in
  let t0 = Unix.gettimeofday () in
  let _ = run_pipeline ~trigger_bk:bk ~silence_bk:bk
            ~process_bk:bk ~window_bk:bk p1 g1 in
  let t1 = Unix.gettimeofday () in
  let phase1_records = backend_size cb in
  let phase1_bytes = cb.bytes_written in
  Printf.printf "  phase 1: %.2fs, backend = %d records, %.0f KB\n%!"
    (t1 -. t0) phase1_records (float phase1_bytes /. 1024.0);

  (* Phase 2: новый pipeline с тем же backend — это и есть recovery.
     restore_all внутри каждого оператора прочитает state из backend
     и продолжит с того же места. *)
  let t2 = Unix.gettimeofday () in
  let _ = run_pipeline ~trigger_bk:bk ~silence_bk:bk
            ~process_bk:bk ~window_bk:bk p2 g2 in
  let t3 = Unix.gettimeofday () in
  Printf.printf "  phase 2 (with restore from backend): %.2fs\n%!" (t3 -. t2);
  Printf.printf "  total events: phase1=%d, phase2=%d\n%!"
    (List.length p1 + List.length g1)
    (List.length p2 + List.length g2)

(* ── Сравнение overhead ────────────────────────────────────── *)

let print_summary scenarios =
  Printf.printf "\n\n========================================\n";
  Printf.printf " SUMMARY (scale=%s, %d runs each)\n"
    (match scale with
     | `Small -> "small" | `Medium -> "medium" | `Large -> "large")
    n_runs;
  Printf.printf "========================================\n\n";
  Printf.printf "%-40s %10s %10s %10s\n"
    "Scenario" "median(s)" "vs A(%)" "thpt(K/s)";
  let (_, (a_med, _, _)) = List.hd scenarios in
  let total_events =
    match scale with
    | `Small  -> 8000
    | `Medium -> 60000
    | `Large  -> 950000
  in
  List.iter (fun (label, (median, _, _)) ->
    let overhead = (median /. a_med -. 1.0) *. 100.0 in
    let thpt = float total_events /. median /. 1000.0 in
    Printf.printf "%-40s %10.2f %10.1f %10.0f\n"
      label median overhead thpt
  ) scenarios;
  Printf.printf "\nA = baseline (no persistence). Other scenarios show relative cost.\n"

(* ── Main ───────────────────────────────────────────────────── *)

let () =
  Printf.printf "==========================================\n";
  Printf.printf " Persistence benchmark (scale=%s)\n"
    (match scale with
     | `Small -> "small" | `Medium -> "medium" | `Large -> "large");
  Printf.printf "==========================================\n";
  Printf.printf "Config: %d horizons × %d miners, %d min simulation\n"
    bench_config.horizons bench_config.miners_per_horizon
    bench_config.simulation_minutes;
  Printf.printf "\nPreparing events...\n%!";
  let t0 = Unix.gettimeofday () in
  let packets, gas = prepare_events () in
  let t1 = Unix.gettimeofday () in
  Printf.printf "  %d packets + %d gas events = %d total (%.2fs to prepare)\n%!"
    (List.length packets) (List.length gas)
    (List.length packets + List.length gas)
    (t1 -. t0);

  let result_a = scenario_a ~packets ~gas in
  let result_b = scenario_b ~packets ~gas in
  let result_c = scenario_c ~packets ~gas in
  let result_d = scenario_d ~packets ~gas in
  let result_e = scenario_e ~packets ~gas in
  let result_f = scenario_f ~packets ~gas in
  scenario_g ~packets ~gas;

  print_summary [
    ("A. Baseline (no persistence)",  result_a);
    ("B. Trigger only persisted",     result_b);
    ("C. silence_age only persisted", result_c);
    ("D. process_keyed only persist", result_d);
    ("E. window_fold only persisted", result_e);
    ("F. All operators persisted",    result_f);
  ];

  Printf.printf "\nDone.\n"
