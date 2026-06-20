open Miniflink
(* Тест бага #2: window обрабатывает late data через retractions *)
open Test_support.Domain
open Time

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let tlm id t s = { device_id=id; speed_kmh=s; fuel_pct=80.;
  position={lat=55.;lon=37.}; ts=t; device=None }

(* ── on-time data: окно закрывается, один Data, без Retract ─── *)
let test_ontime () =
  Printf.printf "\n-- On-time data: single Data, no Retract\n";
  let out =
    Stream.of_list [
      Mf_event.data (tlm "A" 5000 60.) 5000;
      Mf_event.data (tlm "A" 10000 70.) 10000;
      Mf_event.wm 35000;          (* закрывает окно [0,30000) *)
    ]
    |> Pipe.window (module Telemetry) ~latency:0 ~allowed_lateness:(seconds 20)
         (Pipe.tumbling (seconds 30))
    |> Stream.to_list
  in
  let datas = List.filter Mf_event.is_data out in
  let retracts = List.filter (function Mf_event.Retract _ -> true | _ -> false) out in
  check "1 Data emitted" (List.length datas = 1);
  check "0 Retract emitted" (List.length retracts = 0)

(* ── late data within allowed_lateness: Retract + new Data ─── *)
let test_late_retract () =
  Printf.printf "\n-- Late data: Retract old + Data new\n";
  let out =
    Stream.of_list [
      Mf_event.data (tlm "A" 5000 60.) 5000;
      Mf_event.data (tlm "A" 10000 70.) 10000;
      Mf_event.wm 35000;          (* закрывает [0,30000), Fired с 2 событиями *)
      Mf_event.data (tlm "A" 8000 90.) 8000;  (* late! попадает в [0,30000) *)
      Mf_event.wm 40000;          (* ещё в пределах allowed_lateness=20s *)
    ]
    |> Pipe.window (module Telemetry) ~latency:0 ~allowed_lateness:(seconds 20)
         (Pipe.tumbling (seconds 30))
    |> Stream.to_list
  in
  let datas = List.filter Mf_event.is_data out in
  let retracts = List.filter (function Mf_event.Retract _ -> true | _ -> false) out in
  let updates = List.filter (function Mf_event.Update _ -> true | _ -> false) out in
  (* Phase 3: late correction теперь эмитит ОДИН Update event вместо
     пары Retract+Data. Ожидаем: Data(2 события) → Update((2,3)) *)
  check "1 Data emitted (initial)" (List.length datas = 1);
  check "no separate Retract" (List.length retracts = 0);
  check "1 atomic Update emitted (initial → corrected)"
    (List.length updates = 1);
  (* Первый Data — 2 события; Update содержит old=2 события, new=3 *)
  let data_counts = List.filter_map (function
    | Mf_event.Data ((_, vs), _) -> Some (List.length vs) | _ -> None) out in
  check "first window had 2 events" (List.nth data_counts 0 = 2);
  let update_counts = List.filter_map (function
    | Mf_event.Update { old = (_, ovs); new_value = (_, nvs); _ } ->
      Some (List.length ovs, List.length nvs)
    | _ -> None) out in
  check "Update: old=2, new=3"
    (List.nth update_counts 0 = (2, 3))

(* ── very late data beyond allowed_lateness: dropped ───────── *)
let test_too_late () =
  Printf.printf "\n-- Too-late data (beyond allowed_lateness): dropped\n";
  let out =
    Stream.of_list [
      Mf_event.data (tlm "A" 5000 60.) 5000;
      Mf_event.wm 35000;           (* закрывает [0,30000) *)
      Mf_event.wm 60000;           (* > stop+allowed_lateness(20s)=50000 → окно удалено *)
      Mf_event.data (tlm "A" 8000 90.) 8000;  (* очень поздно, окно уже удалено *)
      Mf_event.wm 65000;
    ]
    |> Pipe.window (module Telemetry) ~latency:0 ~allowed_lateness:(seconds 20)
         (Pipe.tumbling (seconds 30))
    |> Stream.to_list
  in
  let retracts = List.filter (function Mf_event.Retract _ -> true | _ -> false) out in
  (* Окно уже удалено — late data создаст НОВОЕ окно (Open), не retract *)
  check "no retract for too-late data" (List.length retracts = 0)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Bug #2: window retraction for late data\n";
  Printf.printf "==========================================\n";
  test_ontime ();
  test_late_retract ();
  test_too_late ();
  Printf.printf "\nAll retraction tests passed.\n"
