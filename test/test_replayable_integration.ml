(** Полноценная имитация crash+restart на triggers + replayable_source.

    Отличие от test_trigger_persistence_e2e.ml: тот вручную делил
    events на (before/after cutoff_ms) — это знание ENGINEER'а о
    структуре теста. Здесь имитируем настоящий рестарт:
    1. Source выдаёт events один за другим
    2. Consumer обрабатывает их по одному, периодически коммитит
       свой offset в backend
    3. "Crash" — теряем in-memory state, ostается backend (snapshot
       triggers state + committed offset)
    4. На "рестарте": читаем offset из backend, открываем source с
       этого offset'а, и trigger автоматически восстанавливает
       свой state из backend
    5. Результат после рестарта = результат без рестарта *)

open Miniflink
open Ex09_complex_trigger_lib

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

type alert =
  | Low_voltage of string * float * Time.t
  | Voltage_ok  of string * Time.t

let alert_to_json = function
  | Low_voltage (l, v, t) ->
    `Assoc [("tag", `String "low_voltage"); ("lamp", `String l);
            ("v", `Float v); ("ts", `Int t)]
  | Voltage_ok (l, t) ->
    `Assoc [("tag", `String "voltage_ok"); ("lamp", `String l);
            ("ts", `Int t)]

let alert_of_json = function
  | `Assoc kv ->
    let tag = List.assoc "tag" kv |> Yojson.Safe.Util.to_string in
    (match tag with
     | "low_voltage" ->
       let l = List.assoc "lamp" kv |> Yojson.Safe.Util.to_string in
       let v = List.assoc "v"    kv |> Yojson.Safe.Util.to_number in
       let t = List.assoc "ts"   kv |> Yojson.Safe.Util.to_int in
       Low_voltage (l, v, t)
     | "voltage_ok" ->
       let l = List.assoc "lamp" kv |> Yojson.Safe.Util.to_string in
       let t = List.assoc "ts"   kv |> Yojson.Safe.Util.to_int in
       Voltage_ok (l, t)
     | other -> failwith ("unknown tag " ^ other))
  | _ -> failwith "not assoc"

let low_voltage_spec () =
  Trigger.create
    ~name:"low_voltage_replayable"
    ~condition:(Trigger.less_than 3.5)
    ~problem_for:(Time.minutes 2)
    ~severity:Trigger.Warning
    ~produce_alert:(fun ~key ~value ~ts -> Low_voltage (key, value, ts))
    ~produce_recovery:(fun ~key ~ts -> Voltage_ok (key, ts))
    ~serialize_key:(fun k -> `String k)
    ~deserialize_key:(fun j -> Yojson.Safe.Util.to_string j)
    ~serialize_value:(fun v -> `Float v)
    ~deserialize_value:(fun j -> Yojson.Safe.Util.to_number j)
    ~serialize_alert:alert_to_json
    ~deserialize_alert:alert_of_json
    ()

(* Offset commit/restore via backend.
   Ключ "consumer:offset" хранит string-представление int. *)
let offset_key = "consumer:offset"

let commit_offset (backend : Trigger.backend) (offset : int) =
  backend.set offset_key (Bytes.of_string (string_of_int offset))

let restore_offset (backend : Trigger.backend) : int =
  match backend.get offset_key with
  | None -> 0
  | Some b -> int_of_string (Bytes.to_string b)

(* Прогнать events до конца, считать alerts, периодически
   коммитить offset (каждые N events). *)
let run_with_commits ~commit_every (stream : 'a Mf_event.t Stream.t)
    (get_offset : unit -> int) (backend : Trigger.backend) =
  let alerts = ref 0 in
  let processed = ref 0 in
  let rec loop () = match stream () with
    | None -> ()
    | Some (Mf_event.Data _) -> incr alerts; incr processed;
      if !processed mod commit_every = 0 then
        commit_offset backend (get_offset ());
      loop ()
    | Some _ -> incr processed;
      if !processed mod commit_every = 0 then
        commit_offset backend (get_offset ());
      loop ()
  in loop ();
  commit_offset backend (get_offset ());  (* финальный commit *)
  !alerts

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Trigger + Replayable_source — crash+restart\n";
  Printf.printf "==========================================\n";

  (* Построить packet events Demo_source как list *)
  let pkts = Demo_source.all_rssi_packets () in
  let sorted = List.sort
    (fun (a : Ex07_location_lib.Domain.packet) b -> compare a.ts b.ts) pkts in
  let events =
    List.map
      (fun (p : Ex07_location_lib.Domain.packet) -> Mf_event.data p p.ts)
      sorted
    @ [Mf_event.wm 500_000] in
  Printf.printf "Demo_source: %d events (incl. final watermark)\n"
    (List.length events);

  (* ── Baseline: full run, no crash ──────────────────────────── *)
  Printf.printf "\n-- Baseline: full run\n";

  let baseline_alerts =
    let stream =
      events
      |> Stream.of_list
      |> Pipe.event_time ~lateness:(Time.seconds 1)
      |> Pipe.map (fun (p : Ex07_location_lib.Domain.packet) -> (p.lamp, p.voltage))
      |> Trigger.of_stream (low_voltage_spec ()) in
    let n = ref 0 in
    let rec loop () = match stream () with
      | None -> ()
      | Some (Mf_event.Data _) -> incr n; loop ()
      | Some _ -> loop ()
    in loop ();
    !n in
  Printf.printf "  baseline: %d alerts\n" baseline_alerts;
  check "baseline >= 1" (baseline_alerts >= 1);

  (* ── Crash+restore через replayable_source ─────────────────── *)
  Printf.printf "\n-- Crash+restore: replayable_source + persisted offset\n";

  let src = Replayable_source.of_list events in
  let tbl = Hashtbl.create 64 in
  let backend = Trigger.backend_of_memory tbl in

  (* === PHASE 1 ===
     Читаем половину events, commit'им offset периодически,
     "крашимся". Backend сохраняет: snapshot trigger state +
     committed offset. *)
  let raw_stream, get_offset = Replayable_source.read_from src in
  let crash_after_n = List.length events / 2 in
  (* Wrapped stream: возвращает None после первых crash_after_n events.
     Это имитирует "процесс упал". *)
  let count = ref 0 in
  let bounded_raw () =
    if !count >= crash_after_n then None
    else begin
      incr count;
      raw_stream ()
    end
  in
  let phase1_pipeline =
    bounded_raw
    |> Pipe.event_time ~lateness:(Time.seconds 1)
    |> Pipe.map (fun (p : Ex07_location_lib.Domain.packet) -> (p.lamp, p.voltage))
    |> Trigger.of_stream ~backend (low_voltage_spec ()) in
  let phase1_alerts = run_with_commits ~commit_every:5
                        phase1_pipeline get_offset backend in
  let committed = restore_offset backend in
  Printf.printf "  phase 1: %d alerts emitted, offset committed = %d (of %d)\n"
    phase1_alerts committed (List.length events);
  check "phase 1: committed offset within first half"
    (committed > 0 && committed <= crash_after_n);

  (* === "PROCESS CRASHED" ===
     in-memory state потерян. Backend остаётся (snapshot triggers +
     offset). Симулируем new process: тот же src (как persistent
     topic в Kafka) + тот же backend. *)

  (* === PHASE 2 ===
     Восстанавливаем offset из backend, открываем source с этой
     позиции, новый Trigger.of_stream подтянет state. *)
  let restored_off = restore_offset backend in
  Printf.printf "  recovery: read offset %d from backend\n" restored_off;
  let raw_stream2, get_offset2 = Replayable_source.read_from ~offset:restored_off src in
  let phase2_pipeline =
    raw_stream2
    |> Pipe.event_time ~lateness:(Time.seconds 1)
    |> Pipe.map (fun (p : Ex07_location_lib.Domain.packet) -> (p.lamp, p.voltage))
    |> Trigger.of_stream ~backend (low_voltage_spec ()) in
  let phase2_alerts = run_with_commits ~commit_every:5
                        phase2_pipeline get_offset2 backend in
  Printf.printf "  phase 2: %d alerts emitted, final offset=%d\n"
    phase2_alerts (restore_offset backend);

  let total = phase1_alerts + phase2_alerts in
  check (Printf.sprintf "total with crash = baseline (%d vs %d)"
           total baseline_alerts)
    (total = baseline_alerts);
  check "final offset = events length"
    (restore_offset backend = List.length events);

  Printf.printf "\nReplay+persistence integration test passed.\n"
