(** Тесты Trigger.

    Покрываем семантику в изоляции:
    - условия и гистерезис
    - immediate transition (problem_for=0)
    - debounce problem_for, recovery_for
    - retract-семантика на recovery
    - sticky внутри Problem (нет refresh-эмиссий)
    - combine — слияние нескольких триггеров
*)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

(* Простой тестовый alert-тип. *)
type alert =
  | Above   of string * float * Time.t
  | Below   of string * float * Time.t
  | Resolved of string * Time.t

(* Запустить триггер на input-list и получить все события *)
let run_trigger spec inputs =
  let stream =
    inputs
    |> List.map (fun (k, v, t) -> Mf_event.data (k, v) t)
    |> Stream.of_list
    |> Trigger.of_stream spec in
  let acc = ref [] in
  let rec loop () =
    match stream () with
    | None -> ()
    | Some ev -> acc := ev :: !acc; loop ()
  in loop ();
  List.rev !acc

(* Запустить с явными Mf_event (для подачи Watermark'ов) *)
let run_trigger_ev spec events =
  let stream =
    events |> Stream.of_list |> Trigger.of_stream spec in
  let acc = ref [] in
  let rec loop () =
    match stream () with
    | None -> ()
    | Some ev -> acc := ev :: !acc; loop ()
  in loop ();
  List.rev !acc

let only_data_retract evs = List.filter (function
  | Mf_event.Data _ | Mf_event.Retract _ | Mf_event.Update _ -> true
  | Mf_event.Watermark _ -> false) evs

(* Простой триггер above 5.0 без гистерезиса, без debounce *)
let above5 () =
  Trigger.create
    ~name:"above5"
    ~condition:(Trigger.greater_than 5.0)
    ~severity:Trigger.Warning
    ~produce_alert:(fun ~key ~value ~ts -> Above (key, value, ts))
    ~produce_recovery:(fun ~key ~ts -> Resolved (key, ts))
    ()

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Trigger\n";
  Printf.printf "==========================================\n";

  (* ── 1. Базовый порог, без debounce ────────────────────────── *)
  Printf.printf "\n-- 1. Threshold, no debounce\n";

  (* Значение ниже порога → ничего *)
  let evs = run_trigger (above5 ()) [("A", 3.0, 100)] in
  check "below threshold: no events" (only_data_retract evs = []);

  (* Значение выше порога → один Data *)
  let evs = run_trigger (above5 ()) [("A", 7.0, 100)] in
  (match only_data_retract evs with
   | [Mf_event.Data (Above ("A", 7.0, 100), 100)] ->
     pass "above threshold: 1 Data Above"
   | _ -> fail "expected single Data Above");

  (* Sticky: повторное превышение того же ключа без recovery — silent *)
  let evs = run_trigger (above5 ())
    [("A", 7.0, 100); ("A", 8.0, 200); ("A", 9.0, 300)] in
  let n = List.length (only_data_retract evs) in
  check (Printf.sprintf "stay-in-problem: 1 emission (got %d)" n) (n = 1);

  (* ── 2. Recovery без debounce ──────────────────────────────── *)
  Printf.printf "\n-- 2. Recovery (no debounce)\n";

  let evs = run_trigger (above5 ())
    [("A", 7.0, 100); ("A", 3.0, 200)] in
  (match only_data_retract evs with
   | [Mf_event.Data (Above _, _);
      Mf_event.Retract (Above _, _);
      Mf_event.Data (Resolved _, _)] ->
     pass "problem → ok: Data, Retract, Data(Resolved)"
   | other ->
     Printf.printf "  unexpected: %d events\n" (List.length other);
     fail "expected 3 events");

  (* ── 3. Гистерезис ─────────────────────────────────────────── *)
  Printf.printf "\n-- 3. Hysteresis\n";

  let voltage_trigger =
    Trigger.create
      ~name:"low_voltage"
      ~condition:(Trigger.less_than_with_hysteresis
                    ~problem:3.5 ~recovery:3.7)
      ~severity:Trigger.Warning
      ~produce_alert:(fun ~key ~value ~ts -> Below (key, value, ts))
      ~produce_recovery:(fun ~key ~ts -> Resolved (key, ts))
      () in

  (* 3.4 → Problem; 3.6 → ВСЁ ЕЩЁ в Problem (в мёртвой зоне); 3.8 → Recovery *)
  let evs = run_trigger voltage_trigger
    [("M1", 3.4, 100); ("M1", 3.6, 200); ("M1", 3.8, 300)] in
  let n = List.length (only_data_retract evs) in
  check (Printf.sprintf "hysteresis 3.4→3.6→3.8: 3 events (got %d)" n)
    (n = 3);

  (* Проверим что 3.6 НЕ вызвал recovery *)
  (match only_data_retract evs with
   | [Mf_event.Data (Below _, 100);
      Mf_event.Retract (Below _, _);
      Mf_event.Data (Resolved _, 300)] ->
     pass "hysteresis: 3.6 in dead zone, no flapping"
   | _ -> fail "hysteresis: wrong sequence");

  (* ── 4. Problem debounce ───────────────────────────────────── *)
  Printf.printf "\n-- 4. Problem debounce\n";

  let spec_debounce =
    Trigger.create
      ~name:"above5_debounce"
      ~condition:(Trigger.greater_than 5.0)
      ~problem_for:100  (* 100ms *)
      ~produce_alert:(fun ~key ~value ~ts -> Above (key, value, ts))
      ~produce_recovery:(fun ~key ~ts -> Resolved (key, ts))
      () in

  (* Превышение в t=100, но возврат в t=150 (до дозревания) → silence *)
  let evs = run_trigger spec_debounce
    [("A", 7.0, 100); ("A", 3.0, 150)] in
  check "debounce 100ms, return at 150ms: no events"
    (only_data_retract evs = []);

  (* Превышение в t=100, держится до t=300; watermark в 300 → fire *)
  let evs = run_trigger_ev spec_debounce [
    Mf_event.data ("A", 7.0) 100;
    Mf_event.data ("A", 8.0) 150;
    Mf_event.wm 300;
  ] in
  (match only_data_retract evs with
   | [Mf_event.Data (Above ("A", 8.0, _), _)] ->
     pass "debounce fired at watermark 300"
   | _ -> fail "expected single Data Above after debounce");

  (* ── 5. Recovery debounce ──────────────────────────────────── *)
  Printf.printf "\n-- 5. Recovery debounce\n";

  let spec_rec_debounce =
    Trigger.create
      ~name:"above5_rec_debounce"
      ~condition:(Trigger.greater_than 5.0)
      ~recovery_for:100
      ~produce_alert:(fun ~key ~value ~ts -> Above (key, value, ts))
      ~produce_recovery:(fun ~key ~ts -> Resolved (key, ts))
      () in

  (* Problem в t=100, recovery=true в t=200, ещё recovery в t=250
     (debounce 100ms ещё не вышел), watermark 350 → fire recovery *)
  let evs = run_trigger_ev spec_rec_debounce [
    Mf_event.data ("A", 7.0) 100;
    Mf_event.data ("A", 3.0) 200;
    Mf_event.data ("A", 3.0) 250;
    Mf_event.wm 350;
  ] in
  let alerts = only_data_retract evs in
  check (Printf.sprintf "rec debounce: 3 events D,R,D (got %d)"
           (List.length alerts))
    (List.length alerts = 3);

  (* ── 6. Recovery debounce — отмена при возврате в Problem ──── *)
  Printf.printf "\n-- 6. Pending_ok interrupted\n";

  let evs = run_trigger_ev spec_rec_debounce [
    Mf_event.data ("A", 7.0) 100;  (* Problem *)
    Mf_event.data ("A", 3.0) 200;  (* Pending_ok *)
    Mf_event.data ("A", 8.0) 250;  (* откат в Problem *)
    Mf_event.wm 500;
  ] in
  (* Должен быть один Data Above и ничего больше *)
  let n = List.length (only_data_retract evs) in
  check (Printf.sprintf "interrupted recovery: 1 event (got %d)" n) (n = 1);

  (* ── 7. Per-key independence ───────────────────────────────── *)
  Printf.printf "\n-- 7. Per-key independence\n";

  let evs = run_trigger (above5 ())
    [("A", 7.0, 100); ("B", 7.0, 100);
     ("A", 3.0, 200); ("B", 8.0, 200)] in
  (* A: Problem at 100, Recovery at 200
     B: Problem at 100, sticky at 200
     Итого: 4 события (D_A, D_B, R_A+D_resolved_A) = 4 *)
  let alerts = only_data_retract evs in
  check (Printf.sprintf "two keys: 4 events (got %d)" (List.length alerts))
    (List.length alerts = 4);

  (* ── 8. Combine ────────────────────────────────────────────── *)
  Printf.printf "\n-- 8. Combine\n";

  let above3 =
    Trigger.create
      ~name:"above3"
      ~condition:(Trigger.greater_than 3.0)
      ~produce_alert:(fun ~key ~value ~ts -> Above (key, value, ts))
      ~produce_recovery:(fun ~key ~ts -> Resolved (key, ts))
      () in
  let above7 =
    Trigger.create
      ~name:"above7"
      ~condition:(Trigger.greater_than 7.0)
      ~produce_alert:(fun ~key ~value ~ts -> Above (key, value, ts))
      ~produce_recovery:(fun ~key ~ts -> Resolved (key, ts))
      () in

  (* value=8 должен сработать ОБА: above3 и above7 *)
  let stream =
    [("A", 8.0, 100)]
    |> List.map (fun (k, v, t) -> Mf_event.data (k, v) t)
    |> Stream.of_list
    |> Trigger.combine [above3; above7] in
  let acc = ref [] in
  let rec loop () = match stream () with
    | None -> () | Some e -> acc := e :: !acc; loop () in
  loop ();
  let evs = only_data_retract (List.rev !acc) in
  check (Printf.sprintf "combine: 2 triggers fire on same value (got %d)"
           (List.length evs))
    (List.length evs = 2);

  Printf.printf "\nAll Trigger tests passed.\n"
