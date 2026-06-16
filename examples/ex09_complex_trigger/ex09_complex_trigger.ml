(** ex09 — Multi-condition trigger через композицию items.

    Демонстрирует как сложное Zabbix-style выражение типа
    `(CO > 50) AND (voltage < 3.5) AND (avg_rssi 5m < -75) for 1m`
    строится у нас через композицию stream-операторов.

    Контраст с ex08: там простые threshold-триггеры на одиночных
    items. Здесь — multi-stream объединение + window aggregation +
    custom predicate. *)

open Miniflink

let print_alert (a : Ex09_complex_trigger_lib.Domain.alert) =
  let lamp = Ex09_complex_trigger_lib.Domain.alert_lamp a in
  let ts = Ex09_complex_trigger_lib.Domain.alert_ts a in
  match a with
  | Ex09_complex_trigger_lib.Domain.Evacuation_critical { co; voltage; rssi; _ } ->
    Printf.printf
      "  🆘 %s [%d мс]: CRITICAL evacuation — CO=%.0f ppm, V=%.2f В, RSSI=%.1f dBm\n"
      lamp ts co voltage rssi
  | Ex09_complex_trigger_lib.Domain.Evacuation_cleared _ ->
    Printf.printf "  ✓ %s [%d мс]: evacuation cleared\n" lamp ts

let print_event (ev : Ex09_complex_trigger_lib.Domain.alert Mf_event.t) =
  match ev with
  | Mf_event.Data (a, _) -> print_alert a
  | Mf_event.Retract (a, _) ->
    Printf.printf "  ↺ retract: %s\n" (Ex09_complex_trigger_lib.Domain.alert_lamp a)
  | Mf_event.Watermark _ -> ()

let () =
  Printf.printf "=== ex09: multi-condition evacuation trigger ===\n";
  Printf.printf "Условие: CO>%.0fppm И V<%.1fВ И avg_rssi_5m<%.0fdBm, дольше 1мин\n\n"
    Ex09_complex_trigger_lib.Triggers.co_problem
    Ex09_complex_trigger_lib.Triggers.v_problem
    Ex09_complex_trigger_lib.Triggers.rssi_problem;

  Printf.printf "Источник: 6 шахтёров из ex07 (M1-M6) + 2 шахтёра из ex09\n";
  Printf.printf "  M_critical (ex09): voltage 3.8→3.0В, RSSI~-80dBm (далеко), CO 30→100ppm в t=240с\n";
  Printf.printf "  M_safe     (ex09): voltage 3.9В, RSSI -45dBm (рядом), CO 10ppm (норма)\n";
  Printf.printf "  M1-M6      (ex07): voltage 3.2-4.0В, RSSI -45..-65 (норма), без CO-аномалий\n\n";

  let module Ex07_src = Ex07_location_lib.Mock_source.Default in
  let module Demo_src = Ex09_complex_trigger_lib.Demo_source in

  (* Объединяем потоки из двух источников. ex07 даёт 6 шахтёров с
     реалистичным сценарием, ex09 — 2 шахтёра с заданным сценарием для
     срабатывания evacuation-trigger. После union — единый поток,
     trigger сработает только на M_critical (только у него все три
     условия одновременно). *)
  let merged_packets =
    Mf_event.union
      (Ex07_src.read () |> Pipe.event_time ~lateness:(Time.seconds 1))
      (Demo_src.read () |> Pipe.event_time ~lateness:(Time.seconds 1)) in
  let merged_gas =
    Mf_event.union
      (Ex07_src.read_gas () |> Pipe.event_time ~lateness:(Time.seconds 1))
      (Demo_src.read_gas () |> Pipe.event_time ~lateness:(Time.seconds 1)) in

  (* Stream.split — правильный способ разделить pull-stream между
     несколькими потребителями. Заменяет хак с двумя Src.read() из
     первой версии ex09. *)
  let packets_for_voltage, packets_for_rssi =
    Stream.tee merged_packets in

  let module I = Ex09_complex_trigger_lib.Items in
  let voltage = I.voltage_item packets_for_voltage in
  let co      = I.co_item merged_gas in
  let rssi    = I.avg_rssi_item packets_for_rssi in
  let combined = I.combined_item ~voltage ~co ~rssi in

  let alerts =
    combined
    |> Trigger.of_stream Ex09_complex_trigger_lib.Triggers.evacuation in

  let total = ref 0 in
  let retracts = ref 0 in
  alerts |> Pipe.iter_events (fun ev ->
    (match ev with
     | Mf_event.Data _ -> incr total
     | Mf_event.Retract _ -> incr retracts
     | Mf_event.Watermark _ -> ());
    print_event ev);

  Printf.printf "\nИтого: %d алертов (%d retracts)\n" !total !retracts;
  if !total = 0 then
    Printf.printf "Триггер не сработал — проверьте параметры сценария\n"
  else
    Printf.printf
      "Триггер сработал на M_critical когда все три условия дозрели одновременно.\n"
