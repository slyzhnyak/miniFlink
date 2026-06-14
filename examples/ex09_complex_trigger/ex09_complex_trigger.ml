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

  let module Src = Ex07_location_lib.Mock_source.Default in
  let packets = Src.read () |> Pipe.event_time ~lateness:(Time.seconds 1) in
  let gas = Src.read_gas () |> Pipe.event_time ~lateness:(Time.seconds 1) in

  let module I = Ex09_complex_trigger_lib.Items in
  let voltage = I.voltage_item packets in
  let co      = I.co_item gas in
  let rssi    = I.avg_rssi_item packets in
  let combined = I.combined_item ~voltage ~co ~rssi in

  let alerts =
    combined
    |> Trigger.of_stream Ex09_complex_trigger_lib.Triggers.evacuation in

  let total = ref 0 in
  let retracts = ref 0 in
  let rec loop () =
    match alerts () with
    | None -> ()
    | Some ev ->
      (match ev with
       | Mf_event.Data _ -> incr total
       | Mf_event.Retract _ -> incr retracts
       | Mf_event.Watermark _ -> ());
      print_event ev;
      loop ()
  in
  loop ();

  Printf.printf "\nИтого: %d алертов (%d retracts)\n" !total !retracts;
  if !total = 0 then
    Printf.printf "Default-сценарий не содержит шахтёра с критической комбинацией.\n\
                   Это и есть смысл сложного триггера — он срабатывает редко,\n\
                   только при настоящей угрозе.\n"
