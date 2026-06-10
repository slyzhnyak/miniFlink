open Miniflink
(* Тест session-окон: динамические границы по паузам активности + слияние.
   Главное — проверить что событие, перекрывающее разрыв между двумя
   сессиями, сливает их в одну. *)

open Test_support.Domain

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let tel id ts = { device_id = id; speed_kmh = 1.; fuel_pct = 1.;
                  position = { lat = 0.; lon = 0. }; ts; device = None }

(* собрать окна как (key, число событий, last_ts) *)
let windows stream =
  Stream.to_list stream
  |> List.filter_map (function
       | Mf_event.Data ((k, vs), stop) -> Some (k, List.length vs, stop)
       | _ -> None)

(* ── одна сессия: события в пределах gap ───────────────────── *)
let test_single_session () =
  Printf.printf "\n-- events within gap form one session\n";
  (* события на 0,100,200,300; gap=500; потом watermark закрывает *)
  let events = [
    Mf_event.data (tel "A" 0) 0;
    Mf_event.data (tel "A" 100) 100;
    Mf_event.data (tel "A" 200) 200;
    Mf_event.wm 1000;   (* 200 + 500 = 700 <= 1000 → закрыть *)
  ] in
  let out = Stream.of_list events
    |> Pipe.session_window (module Telemetry) ~gap:500 |> windows in
  check "one session" (List.length out = 1);
  check "session has all 3 events" (List.exists (fun (_, n, _) -> n = 3) out)

(* ── две сессии: разрыв больше gap ─────────────────────────── *)
let test_two_sessions () =
  Printf.printf "\n-- gap larger than threshold splits into two sessions\n";
  (* события 0,100 ... затем 2000,2100; gap=500 → разрыв 100→2000 = 1900 > 500 *)
  let events = [
    Mf_event.data (tel "A" 0) 0;
    Mf_event.data (tel "A" 100) 100;
    Mf_event.data (tel "A" 2000) 2000;
    Mf_event.data (tel "A" 2100) 2100;
    Mf_event.wm 5000;
  ] in
  let out = Stream.of_list events
    |> Pipe.session_window (module Telemetry) ~gap:500 |> windows in
  check "two separate sessions" (List.length out = 2);
  check "each has 2 events" (List.for_all (fun (_, n, _) -> n = 2) out)

(* ── СЛИЯНИЕ: событие, сшивающее две сессии ────────────────── *)
let test_merge () =
  Printf.printf "\n-- a bridging event MERGES two sessions into one\n";
  (* Создаём две сессии: [0,100] и [800,900], gap=300.
     Разрыв 100→800 = 700 > 300 → пока две сессии.
     Затем приходит событие на 450: оно в gap от обеих
     (450-100=350 >300? нет... подберём). gap=400:
     событие 450: 450-100=350 <=400 (примыкает к первой),
     800-450=350 <=400 (примыкает ко второй) → слияние! *)
  let events = [
    Mf_event.data (tel "A" 0) 0;
    Mf_event.data (tel "A" 100) 100;
    Mf_event.data (tel "A" 800) 800;
    Mf_event.data (tel "A" 900) 900;
    (* мост: 450 в пределах gap=400 от обеих сессий *)
    Mf_event.data (tel "A" 450) 450;
    Mf_event.wm 5000;
  ] in
  let out = Stream.of_list events
    |> Pipe.session_window (module Telemetry) ~gap:400 |> windows in
  check "merged into ONE session" (List.length out = 1);
  check "merged session has all 5 events"
    (List.exists (fun (_, n, _) -> n = 5) out)

(* ── per-key независимость ─────────────────────────────────── *)
let test_per_key () =
  Printf.printf "\n-- sessions are per-key\n";
  let events = [
    Mf_event.data (tel "A" 0) 0;
    Mf_event.data (tel "B" 50) 50;
    Mf_event.data (tel "A" 100) 100;
    Mf_event.wm 2000;
  ] in
  let out = Stream.of_list events
    |> Pipe.session_window (module Telemetry) ~gap:500 |> windows in
  check "two sessions (one per key)" (List.length out = 2);
  check "A session has 2 events" (List.exists (fun (k, n, _) -> k = "A" && n = 2) out);
  check "B session has 1 event" (List.exists (fun (k, n, _) -> k = "B" && n = 1) out)

(* ── конец потока закрывает открытые сессии ────────────────── *)
let test_end_of_stream () =
  Printf.printf "\n-- end of stream closes open sessions (no watermark needed)\n";
  let events = [
    Mf_event.data (tel "A" 0) 0;
    Mf_event.data (tel "A" 100) 100;
    (* нет watermark — сессия закроется на конце потока *)
  ] in
  let out = Stream.of_list events
    |> Pipe.session_window (module Telemetry) ~gap:500 |> windows in
  check "session closed at end of stream" (List.length out = 1);
  check "has both events" (List.exists (fun (_, n, _) -> n = 2) out)

(* ── валидация ─────────────────────────────────────────────── *)
let test_validation () =
  Printf.printf "\n-- gap must be positive\n";
  let raises f = try ignore (f ()); false with Invalid_argument _ -> true | _ -> false in
  check "gap=0 → Invalid_argument"
    (raises (fun () -> Stream.of_list [] |> Pipe.session_window (module Telemetry) ~gap:0))

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Session windows (dynamic boundaries + merge)\n";
  Printf.printf "==========================================\n";
  test_single_session ();
  test_two_sessions ();
  test_merge ();
  test_per_key ();
  test_end_of_stream ();
  test_validation ();
  Printf.printf "\nAll session window tests passed.\n"
