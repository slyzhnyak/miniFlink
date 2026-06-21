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

let low_voltage_spec () =
  Trigger.create
    ~name:"low_voltage_e2e"
    ~condition:(Trigger.less_than 3.5)
    ~problem_for:(Time.minutes 2)  (* 120s — fire после t=225+120=345с *)
    ~severity:Trigger.Warning
    ~produce_alert:(fun ~key ~value ~ts -> Low_voltage (key, value, ts))
    ~produce_recovery:(fun ~key ~ts -> Voltage_ok (key, ts))
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

  (* ── Phase 1: events до t=300с в DURABLE-контексте ────────── *)
  Printf.printf "\n-- Phase 1: run up to t=300s in durable context\n";

  let tbl = Hashtbl.create 64 in
  let backend = Persistence_backend.of_memory tbl in
  let ctx = Runtime_context.durable backend in

  let phase1_events, phase2_events = split_at_ts all_events 300_000 in

  (* Тот же триггерный пайплайн, без изменений — persistence снаружи. *)
  let phase1_data =
    Runtime_context.with_context ctx (fun () ->
      count_data (phase1_events |> voltage_stream
                  |> Trigger.of_stream (low_voltage_spec ()))) in
  Printf.printf "  phase 1: %d Data alerts\n" phase1_data;
  check "phase 1: no alert yet (debounce not matured)" (phase1_data = 0);
  check "phase 1: backend has records" (Hashtbl.length tbl >= 1);
  Printf.printf "  backend has %d keys total\n" (Hashtbl.length tbl);

  (* ── Phase 2: новый trigger, тот же контекст → restore ─────── *)
  Printf.printf "\n-- Phase 2: new trigger picks up state from backend\n";

  let phase2_with_wm = phase2_events @ [Mf_event.wm 500_000] in
  let phase2_data =
    Runtime_context.with_context ctx (fun () ->
      count_data (phase2_with_wm |> voltage_stream
                  |> Trigger.of_stream (low_voltage_spec ()))) in
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
