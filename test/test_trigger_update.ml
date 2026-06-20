(** Тест: Trigger корректно обрабатывает Update events.

    Verifies Phase 3.2 — conservative Update handling в Trigger
    действительно даёт правильное поведение для всех сценариев:

    1. Update с new ABOVE threshold → triggers Problem (если был Ok)
    2. Update с new BELOW threshold → triggers Recovery (если был Problem)
    3. Update с new BETWEEN states → state evolves naturally
    4. Update сам по себе (без предшествующих Data) обрабатывается

    Цель: убедиться что Update от Window (late correction) корректно
    проходит через Trigger как если бы это был просто свежий Data.
    *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

(* Простой type для теста *)
type alert =
  | Problem of string * float * Time.t
  | Recovery of string * Time.t

let alert_lamp = function
  | Problem (l, _, _) | Recovery (l, _) -> l

(* Trigger с порогом 3.5: значения < 3.5 = low voltage problem *)
let low_voltage_trigger () = Trigger.create
  ~name:"low_voltage"
  ~condition:(Trigger.less_than 3.5)
  ~problem_for:0      (* без debounce — мгновенный fire *)
  ~recovery_for:0
  ~severity:Trigger.Warning
  ~produce_alert:(fun ~key ~value ~ts -> Problem (key, value, ts))
  ~produce_recovery:(fun ~key ~ts -> Recovery (key, ts))
  ()

let collect_alerts stream =
  let datas = ref [] in
  let retracts = ref [] in
  let updates = ref [] in
  let rec drain () = match stream () with
    | None -> ()
    | Some (Mf_event.Data (a, _)) -> datas := a :: !datas; drain ()
    | Some (Mf_event.Retract (a, _)) -> retracts := a :: !retracts; drain ()
    | Some (Mf_event.Update { old; new_value; _ }) ->
      updates := (old, new_value) :: !updates; drain ()
    | Some (Mf_event.Watermark _) -> drain ()
  in drain ();
  (List.rev !datas, List.rev !retracts, List.rev !updates)

let () =
  Printf.printf "Test: Trigger conservative Update handling\n%!";

  (* ── 1. Update с new ниже порога → Problem ──────────── *)
  Printf.printf "\n-- 1. Update with new BELOW threshold → fires Problem\n";
  (* Стрим: Data(5.0) → no alert. Затем Update{old=5, new=2}: new ниже
     порога → Trigger переходит в Problem. *)
  let events = [
    Mf_event.data ("M1", 5.0) 0;       (* Ok *)
    Mf_event.wm 100;
    Mf_event.update ("M1", 5.0) ("M1", 2.0) 200;  (* correction → low *)
    Mf_event.wm 300;
  ] in
  let datas, _, _ = events |> Stream.of_list
    |> Trigger.of_stream (low_voltage_trigger ())
    |> collect_alerts in
  Printf.printf "  alerts: %d\n" (List.length datas);
  List.iter (fun a -> Printf.printf "    %s\n" (alert_lamp a)) datas;
  check "1 Problem alert after Update to low value"
    (List.length datas = 1
     && (match List.hd datas with Problem ("M1", 2.0, _) -> true | _ -> false));

  (* ── 2. Update с new выше порога после Problem → Recovery ── *)
  Printf.printf "\n-- 2. Update with new ABOVE threshold (was Problem) → Recovery\n";
  let events = [
    Mf_event.data ("M2", 2.0) 0;        (* low → Problem *)
    Mf_event.wm 100;
    Mf_event.update ("M2", 2.0) ("M2", 5.0) 200;  (* correction → ok *)
    Mf_event.wm 300;
  ] in
  let datas, retracts, _ = events |> Stream.of_list
    |> Trigger.of_stream (low_voltage_trigger ())
    |> collect_alerts in
  Printf.printf "  data: %d, retracts: %d\n"
    (List.length datas) (List.length retracts);
  check "1 Problem + 1 Recovery (Data) and 1 retract of Problem"
    (List.length datas = 2 && List.length retracts = 1);
  check "first alert is Problem"
    (match datas with Problem ("M2", 2.0, _) :: _ -> true | _ -> false);
  check "second alert is Recovery"
    (match List.nth_opt datas 1 with Some (Recovery ("M2", _)) -> true | _ -> false);

  (* ── 3. Update с new в том же state → no new alerts ── *)
  Printf.printf "\n-- 3. Update with new in SAME state → no new alerts\n";
  let events = [
    Mf_event.data ("M3", 5.0) 0;        (* Ok *)
    Mf_event.wm 100;
    Mf_event.update ("M3", 5.0) ("M3", 4.5) 200;  (* still Ok (>3.5) *)
    Mf_event.wm 300;
  ] in
  let datas, _, _ = events |> Stream.of_list
    |> Trigger.of_stream (low_voltage_trigger ())
    |> collect_alerts in
  check "no alerts when Update stays in Ok"
    (List.length datas = 0);

  (* ── 4. Update без предшествующих Data ────────────── *)
  Printf.printf "\n-- 4. Update without prior Data → still triggers on new_value\n";
  let events = [
    Mf_event.update ("M4", 4.0) ("M4", 2.0) 100;  (* first event is Update *)
    Mf_event.wm 200;
  ] in
  let datas, _, _ = events |> Stream.of_list
    |> Trigger.of_stream (low_voltage_trigger ())
    |> collect_alerts in
  check "Update without prior Data triggers Problem on new_value"
    (List.length datas = 1
     && (match List.hd datas with Problem ("M4", 2.0, _) -> true | _ -> false));

  (* ── 5. Update с debounce (problem_for > 0) ─────── *)
  Printf.printf "\n-- 5. Update with debounce — fires only after problem_for\n";
  let trig_debounced = Trigger.create
    ~name:"low_voltage_debounce"
    ~condition:(Trigger.less_than 3.5)
    ~problem_for:1000       (* 1 second debounce *)
    ~recovery_for:0
    ~severity:Trigger.Warning
    ~produce_alert:(fun ~key ~value ~ts -> Problem (key, value, ts))
    ~produce_recovery:(fun ~key ~ts -> Recovery (key, ts))
    () in
  let events = [
    Mf_event.update ("M5", 5.0) ("M5", 2.0) 100;  (* low *)
    Mf_event.wm 500;        (* < 1000 after low → still pending *)
    Mf_event.wm 1200;       (* ≥ 100 + 1000 → debounce satisfied → fires *)
  ] in
  let datas, _, _ = events |> Stream.of_list
    |> Trigger.of_stream trig_debounced
    |> collect_alerts in
  Printf.printf "  alerts: %d\n" (List.length datas);
  check "1 Problem after debounce window"
    (List.length datas = 1);

  Printf.printf "\nAll Trigger Update handling tests passed.\n"
