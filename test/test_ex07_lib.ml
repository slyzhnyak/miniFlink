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
  let updates = List.filter (function
    | Mf_event.Update { old; new_value; _ } -> old <> new_value | _ -> false) locations in
  (* В этом источнике есть опоздавший пакет M2 с необычно сильным B5,
     который переоткрывает уже закрытое окно и меняет top-2 маяков →
     реальная атомарная коррекция позиции (Update). Пример был бы
     нереалистичным без них: late-data correction — ключевая
     возможность библиотеки.

     Два инварианта:
     1. Коррекции реально происходят (есть Update'ы).
     2. Каждый Update несёт настоящее изменение (old <> new) —
        noop-коррекции подавляются window_agg. *)
  check "late packets produce real corrections (Update events exist)"
    (List.length updates > 0);
  check "no noop corrections (every Update changes the result)"
    (List.for_all (function
       | Mf_event.Update { old; new_value; _ } -> old <> new_value
       | _ -> true) locations);
  ignore retracts;

  (* Пайплайн алертов *)
  let alerts =
    Src.read () |> Pipelines.connectivity_alerts |> Pipe.collect in
  check "connectivity_alerts produces alerts" (List.length alerts > 0);
  check "alerts include Sos" (
    List.exists (function Domain.Sos _ -> true | _ -> false) alerts);
  check "alerts include Low_voltage" (
    List.exists (function Domain.Low_voltage _ -> true | _ -> false) alerts);
  (* connectivity_alerts переписан на декларативную композицию
     (on_silence + suppress_while + Trigger). Проверяем, что absence-
     детекторы дают свои алерты — раньше эти ветки не тестировались. *)
  check "alerts include an absence alert (No_packets/No_readings/No_motion)" (
    List.exists (function
      | Domain.No_packets _ | Domain.No_readings _ | Domain.No_motion _ -> true
      | _ -> false) alerts);

  (* Газовый пайплайн (переписан на co_process3) — проверяем, что
     retract-семантика работает end-to-end: есть алерты И есть
     Retract/Update (алерты отзываются/обновляются). *)
  let gas_events =
    let gas = Src.read_gas () in
    let rssi = Src.read () in
    let locs = Src.read () |> Pipelines.median_rssi in
    Pipelines.gas_alerts ~rssi ~locations:locs ~gas ()
    |> Stream.to_list in
  let gas_data = List.filter (function
    | Mf_event.Data (Domain.Gas_alert _, _) -> true | _ -> false) gas_events in
  let gas_retracts = List.filter (function
    | Mf_event.Retract _ -> true | _ -> false) gas_events in
  let gas_updates = List.filter (function
    | Mf_event.Update _ -> true | _ -> false) gas_events in
  check "gas_alerts produces Gas_alert" (List.length gas_data > 0);
  check "gas_alerts uses retract-семантику (Retract или Update есть)"
    (List.length gas_retracts > 0 || List.length gas_updates > 0);

  (* G-6 reactive enrich (закрыт co_process3 + emit_event): газовый
     алерт, выпущенный БЕЗ позиции, переэмитится с позицией, когда она
     приходит позже. Контролируемый сценарий: CH4 6000ppm (Warning) без
     позиции, затем location с координатами на тот же lamp. *)
  let ch4_pkt : Domain.gas_packet = {
    g_lamp = "M1"; g_ts = 1000;
    g_co2 = None; g_co = None; g_h2 = None; g_ch4 = Some 6000. } in
  let loc : Domain.location = {
    loc_lamp = "M1"; loc_wend = 2000;
    loc_top2 = []; loc_position = Some (10., 20., -30) } in
  let re_emit =
    let gas = Stream.of_list [ Mf_event.data ch4_pkt 1000; Mf_event.wm 3000 ] in
    let rssi = Stream.of_list [ Mf_event.wm 3000 ] in
    let locs = Stream.of_list [ Mf_event.data loc 2000; Mf_event.wm 3000 ] in
    Pipelines.gas_alerts ~rssi ~locations:locs ~gas ()
    |> Stream.to_list in
  (* первый алерт без позиции *)
  let had_alert_without_pos = List.exists (function
    | Mf_event.Data (Domain.Gas_alert { ga_position = None; _ }, _) -> true
    | _ -> false) re_emit in
  (* переэмиссия с позицией (Update, где new несёт Some position) *)
  let re_emitted_with_pos = List.exists (function
    | Mf_event.Update { new_value =
        Domain.Gas_alert { ga_position = Some _; _ }; _ } -> true
    | _ -> false) re_emit in
  check "G-6: газовый алерт сперва без позиции" had_alert_without_pos;
  check "G-6: обновление позиции переэмитило алерт с координатами"
    re_emitted_with_pos;

  Printf.printf "\nLibrary reusable from external test ✓\n"
