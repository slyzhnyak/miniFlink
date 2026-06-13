(** Library-level test for [Ex07_location_lib].

    Главная польза от выделения пайплайнов в библиотеку — возможность
    использовать их вне executable: в бенчмарках, в regression-тестах,
    в потенциальном production-сервисе. Этот тест доказывает что library
    действительно reusable: импортирует, прогоняет, проверяет инварианты. *)

open Miniflink
open Ex07_location_lib

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Ex07 library reusability\n";
  Printf.printf "==========================================\n";

  (* Доступ к доменным типам *)
  let p : Domain.packet = {
    lamp = "TestMiner"; readings = [("B1", -50.)];
    voltage = 4.0; moving = true; sos = false; ts = 0;
  } in
  check "Domain.packet constructible" (p.lamp = "TestMiner");

  (* base_packets — низкоуровневый слой без аномалий канала.
     Не предназначен для бенчмарков! (бенчмарк должен включать late+dups
     чтобы отражать прод-нагрузку). Здесь используется только для
     проверки, что низкоуровневый API экспортируется. *)
  let base_pkts = Mock_source.base_packets () in
  check "base_packets produces packets" (List.length base_pkts > 100);
  check "base_packets has 6 miners" (
    let miners = List.sort_uniq compare
      (List.map (fun (p : Domain.packet) -> p.lamp) base_pkts) in
    List.length miners = 6);

  (* Источник Default — реалистичный, для regression *)
  let module Src = Mock_source.Default in
  let stats = Src.stats () in
  check "Jittered source has stats" (List.length stats >= 2);

  (* Пайплайн локации — стрим, можно прогонять отдельно *)
  let locations =
    Src.read () |> Pipelines.median_rssi |> Stream.to_list in
  let location_data = List.filter_map (function
    | Mf_event.Data (loc, _) -> Some loc | _ -> None) locations in
  check "median_rssi pipeline produces locations" (List.length location_data > 0);
  let retracts = List.filter (function
    | Mf_event.Retract _ -> true | _ -> false) locations in
  check "late packets cause retractions" (List.length retracts > 0);

  (* Пайплайн алертов *)
  let alerts =
    Src.read () |> Pipelines.connectivity_alerts |> Pipe.collect in
  check "connectivity_alerts produces alerts" (List.length alerts > 0);
  check "alerts include Sos" (
    List.exists (function Domain.Sos _ -> true | _ -> false) alerts);
  check "alerts include Low_voltage" (
    List.exists (function Domain.Low_voltage _ -> true | _ -> false) alerts);

  (* FSM-функции — чистые, проверяем переходы *)
  let seen = Pipelines.Last_seen.make () in
  let pkt_with_motion = { p with moving = true; ts = 100 } in
  let _ = Pipelines.Last_seen.record seen ~p:pkt_with_motion in
  check "Last_seen.record updates moving" (seen.moving = Some 100);
  check "Fsm.contact_state pure & callable"
    (Pipelines.Fsm.contact_state ~now:100 seen = Pipelines.Fsm.Contact_ok);

  Printf.printf "\nLibrary reusable from external test ✓\n"
