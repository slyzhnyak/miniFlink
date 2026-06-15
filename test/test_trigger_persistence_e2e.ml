(** End-to-end integration test триггер-persistence на реальном
    Mock_source.

    Сценарий "crash mid-stream":
    1. Создаём trigger низкого voltage с backend, прогоняем packets
       Demo_source ДО момента t=300с. Voltage M_critical уже < 3.5,
       но debounce 120с не дозрел (problem fire_at ≈ 345с).
       Backend сохраняет state=Pending_problem.

    2. "Killing" the trigger — просто перестаём дёргать его stream.

    3. Создаём НОВЫЙ trigger с тем же backend, прогоняем оставшиеся
       packets от 300с до 480с. Restore подгружает Pending_problem,
       пересоздаёт debounce-timer. В районе t=345с — fire alert.

    Доказательство что persistence работает в реальном пайплайне,
    не только на синтетических unit-tests. *)

open Miniflink
open Ex09_complex_trigger_lib

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

(* Доменный alert (упрощённый, без Domain.alert из ex09 для
   изоляции теста). *)
type alert =
  | Low_voltage of string * float * Time.t
  | Voltage_ok  of string * Time.t

let alert_to_json = function
  | Low_voltage (l, v, t) ->
    `Assoc [("tag", `String "low_voltage");
            ("lamp", `String l); ("v", `Float v); ("ts", `Int t)]
  | Voltage_ok (l, t) ->
    `Assoc [("tag", `String "voltage_ok");
            ("lamp", `String l); ("ts", `Int t)]

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
     | other -> failwith ("alert_of_json: unknown tag " ^ other))
  | _ -> failwith "alert_of_json: not assoc"

let low_voltage_spec () =
  Trigger.create
    ~name:"low_voltage_e2e"
    ~condition:(Trigger.less_than 3.5)
    ~problem_for:(Time.minutes 2)  (* 120s — fire после t=225+120=345с *)
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

(* Прогнать stream до конца, посчитать Data-events.
   Watermark events игнорируем. *)
let count_data stream =
  let n = ref 0 in
  let rec loop () = match stream () with
    | None -> ()
    | Some (Mf_event.Data _) -> incr n; loop ()
    | Some _ -> loop ()
  in loop ();
  !n

(* Извлекаем voltage-item stream из Demo_source. *)
let voltage_stream events =
  events
  |> Stream.of_list
  |> Pipe.event_time ~lateness:(Time.seconds 1)
  |> Pipe.map (fun (p : Ex07_location_lib.Domain.packet) -> (p.lamp, p.voltage))

(* Конструируем packet events от Demo_source. *)
let demo_packet_events () =
  let pkts = Demo_source.all_rssi_packets () in
  let sorted = List.sort
    (fun (a : Ex07_location_lib.Domain.packet) b -> compare a.ts b.ts) pkts in
  List.map
    (fun (p : Ex07_location_lib.Domain.packet) -> Mf_event.data p p.ts)
    sorted

(* Разделить events по ts на (раньше cutoff, позже cutoff) и добавить
   watermark в конце первой части. *)
let split_at_ts events cutoff_ms =
  let before, after = List.partition
    (fun ev -> Mf_event.ts ev < cutoff_ms) events in
  (* В конец первой части добавляем watermark cutoff чтобы все
     persisted state'ы записались до того как мы "крашнем". *)
  (before @ [Mf_event.wm cutoff_ms], after)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  E2E trigger persistence — crash mid-stream\n";
  Printf.printf "==========================================\n";

  let all_events = demo_packet_events () in
  Printf.printf "Demo_source: %d packets total\n" (List.length all_events);

  (* ── Baseline: запуск без crash, считаем alerts ──────────── *)
  Printf.printf "\n-- Baseline: run without crash\n";

  let baseline_stream =
    all_events @ [Mf_event.wm 500_000]  (* финальный wm *)
    |> voltage_stream
    |> Trigger.of_stream (low_voltage_spec ()) in
  let baseline_data = count_data baseline_stream in
  Printf.printf "  baseline produced %d Data alerts\n" baseline_data;
  check "baseline: at least 1 alert from M_critical voltage drop"
    (baseline_data >= 1);

  (* ── Phase 1: подаём events до t=300с, останавливаемся ───── *)
  Printf.printf "\n-- Phase 1: run up to t=300s with backend\n";

  let tbl = Hashtbl.create 64 in
  let backend = Trigger.backend_of_memory tbl in

  let phase1_events, phase2_events = split_at_ts all_events 300_000 in

  let phase1_stream =
    phase1_events
    |> voltage_stream
    |> Trigger.of_stream ~backend (low_voltage_spec ()) in
  let phase1_data = count_data phase1_stream in
  Printf.printf "  phase 1: %d Data alerts\n" phase1_data;
  check "phase 1: no alert yet (debounce not matured)" (phase1_data = 0);

  (* Проверяем что backend содержит запись для M_critical *)
  let bk_critical = "trigger:low_voltage_e2e:\"M_critical\"" in
  (match Hashtbl.find_opt tbl bk_critical with
   | None -> fail "phase 1: backend missing record for M_critical"
   | Some v_bytes ->
     let json = Yojson.Safe.from_string (Bytes.to_string v_bytes) in
     match json with
     | `Assoc kv ->
       (match List.assoc "state" kv with
        | `Assoc skv ->
          let tag = Yojson.Safe.Util.to_string (List.assoc "tag" skv) in
          check (Printf.sprintf "phase 1: state=%s (Pending_problem)" tag)
            (tag = "pending_problem")
        | _ -> fail "state not assoc")
     | _ -> fail "json not assoc");

  Printf.printf "  backend has %d keys total\n" (Hashtbl.length tbl);

  (* ── Phase 2: новый trigger с тем же backend ─────────────── *)
  Printf.printf "\n-- Phase 2: new trigger picks up state from backend\n";

  let phase2_with_wm = phase2_events @ [Mf_event.wm 500_000] in
  let phase2_stream =
    phase2_with_wm
    |> voltage_stream
    |> Trigger.of_stream ~backend (low_voltage_spec ()) in
  let phase2_data = count_data phase2_stream in
  Printf.printf "  phase 2: %d Data alerts\n" phase2_data;
  check (Printf.sprintf "phase 2: at least 1 alert after restore (got %d)"
           phase2_data)
    (phase2_data >= 1);

  (* Итог: phase 1 + phase 2 должны дать тот же результат что и
     baseline. Если суммы совпадают — crash был "невидимым". *)
  let total_with_crash = phase1_data + phase2_data in
  check (Printf.sprintf "total with crash = baseline (%d vs %d)"
           total_with_crash baseline_data)
    (total_with_crash = baseline_data);

  Printf.printf "\nE2E persistence test passed.\n"
