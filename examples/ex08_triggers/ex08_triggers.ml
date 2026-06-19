(** ex08 — демонстрация триггерной системы как ортогонального
    дополнения к ex07.

    Структура файла-топологии прозрачно отражает то, как триггеры
    собираются в реальном сервисе:

    1. Берём источник данных (тот же {!Ex07_location_lib.Mock_source.Default}
       что и в ex07 — Triggers работают с тем же источником без
       специальной интеграции).
    2. Извлекаем items простыми преобразованиями: voltage, gas_co,
       silence_age (всё в {!Items}).
    3. Применяем триггеры из {!Triggers} к соответствующим items
       через [Trigger.of_stream] или [Trigger.combine].
    4. Сливаем выходные потоки алертов в один через [Mf_event.union]
       и публикуем в sink.

    Добавить новый триггер — одна декларация в {!Triggers}, ничего
    больше нигде не править. *)

open Miniflink

let section title = Printf.printf "\n%s\n" title

(* Печать одного алерта. *)
let print_alert (a : Ex08_triggers_lib.Domain.alert) =
  let lamp = Ex08_triggers_lib.Domain.alert_lamp a in
  let ts = Ex08_triggers_lib.Domain.alert_ts a in
  match a with
  | Ex08_triggers_lib.Domain.Low_voltage { voltage; _ } ->
    Printf.printf "  ⚠ %s [%d мс]: Low voltage %.2f В\n" lamp ts voltage
  | Ex08_triggers_lib.Domain.Voltage_ok _ ->
    Printf.printf "  ✓ %s [%d мс]: Voltage recovered\n" lamp ts
  | Ex08_triggers_lib.Domain.Voltage_critical { voltage; _ } ->
    Printf.printf "  🆘 %s [%d мс]: CRITICAL voltage %.2f В\n" lamp ts voltage
  | Ex08_triggers_lib.Domain.Voltage_recovered _ ->
    Printf.printf "  ✓ %s [%d мс]: Critical voltage recovered\n" lamp ts
  | Ex08_triggers_lib.Domain.No_packets { silence_ms; _ } ->
    Printf.printf "  ⚠ %s [%d мс]: No packets for %d мс\n"
      lamp ts silence_ms
  | Ex08_triggers_lib.Domain.Packets_resumed _ ->
    Printf.printf "  ✓ %s [%d мс]: Packets resumed\n" lamp ts
  | Ex08_triggers_lib.Domain.Gas_co_warning { ppm; _ } ->
    Printf.printf "  ⚠ %s [%d мс]: CO %.0f ppm\n" lamp ts ppm
  | Ex08_triggers_lib.Domain.Gas_co_safe _ ->
    Printf.printf "  ✓ %s [%d мс]: CO returned to safe\n" lamp ts

let print_event (ev : Ex08_triggers_lib.Domain.alert Mf_event.t) =
  match ev with
  | Mf_event.Data (a, _) -> print_alert a
  | Mf_event.Retract (a, _) ->
    Printf.printf "  ↺ retract: %s\n" (Ex08_triggers_lib.Domain.alert_lamp a)
  | Mf_event.Update { old; new_value; _ } ->
    Printf.printf "  ↻ update: %s → %s\n"
      (Ex08_triggers_lib.Domain.alert_lamp old)
      (Ex08_triggers_lib.Domain.alert_lamp new_value)
  | Mf_event.Watermark _ -> ()

let () =
  Printf.printf "=== ex08: триггерная система ===\n";

  let module Src = Ex07_location_lib.Mock_source.Default in
  (* Mock_source.read даёт чистый Data-поток без watermark'ов.
     Триггеры с debounce срабатывают по watermark'у, поэтому добавляем
     их через event_time. lateness=1с подходит для синтетического
     источника. *)
  let packets = Src.read () |> Pipe.event_time ~lateness:(Time.seconds 1) in
  let gas = Src.read_gas () |> Pipe.event_time ~lateness:(Time.seconds 1) in

  (* ── Voltage-триггеры: demo версии с уменьшенным debounce ─────── *)
  (* В Default-сценарии симуляция короткая (~6 мин), M1 voltage
     падает с 4.0 до 3.2 В. С реальным debounce 2 мин Trigger едва
     успевает сработать. Для demo используем 30-секундный debounce. *)
  let low_voltage_demo =
    Trigger.create
      ~name:"low_voltage_demo"
      ~condition:(Trigger.less_than_with_hysteresis
                    ~problem:3.5 ~recovery:3.7)
      ~problem_for:(Time.seconds 30)
      ~severity:Trigger.Warning
      ~produce_alert:(fun ~key ~value ~ts ->
        Ex08_triggers_lib.Domain.Low_voltage {
          lamp = key; voltage = value; ts })
      ~produce_recovery:(fun ~key ~ts ->
        Ex08_triggers_lib.Domain.Voltage_ok { lamp = key; ts })
      () in
  let voltage_critical_demo =
    Trigger.create
      ~name:"voltage_critical_demo"
      ~condition:(Trigger.less_than 3.3)
      ~problem_for:(Time.seconds 20)
      ~severity:Trigger.Disaster
      ~produce_alert:(fun ~key ~value ~ts ->
        Ex08_triggers_lib.Domain.Voltage_critical {
          lamp = key; voltage = value; ts })
      ~produce_recovery:(fun ~key ~ts ->
        Ex08_triggers_lib.Domain.Voltage_recovered { lamp = key; ts })
      () in
  let voltage_alerts =
    packets
    |> Ex08_triggers_lib.Items.voltage_item
    |> Trigger.combine [low_voltage_demo; voltage_critical_demo] in

  (* ── No-packets через silence_age ─────────────────────────────── *)
  (* Используем меньший tick (5 сек) и пониженный threshold (10 сек)
     чтобы триггер успевал сработать в нашем коротком Default-сценарии
     (~6 минут event-time). В реальной шахте triggers.no_packets имеет
     порог 2 мин и tick 30 сек. *)
  let no_packets_demo =
    Trigger.create
      ~name:"no_packets_demo"
      ~condition:(Trigger.greater_than (float_of_int (Time.seconds 10)))
      ~severity:Trigger.Warning
      ~produce_alert:(fun ~key ~value ~ts ->
        Ex08_triggers_lib.Domain.No_packets {
          lamp = key; silence_ms = int_of_float value; ts })
      ~produce_recovery:(fun ~key ~ts ->
        Ex08_triggers_lib.Domain.Packets_resumed { lamp = key; ts })
      () in
  let no_packets_alerts =
    packets
    |> Ex08_triggers_lib.Items.no_packets_item ~tick:(Time.seconds 5)
    |> Trigger.of_stream no_packets_demo in

  (* ── Газовые триггеры ───────────────────────────────────────── *)
  let gas_alerts =
    gas
    |> Ex08_triggers_lib.Items.gas_co_item
    |> Trigger.of_stream Ex08_triggers_lib.Triggers.gas_co_warning in

  (* ── Объединение в один поток ───────────────────────────────── *)
  let all_alerts =
    Mf_event.union voltage_alerts
      (Mf_event.union no_packets_alerts gas_alerts) in

  section "Алерты (через триггерную систему):";
  let total = ref 0 in
  let retracts = ref 0 in
  all_alerts |> Pipe.iter_events (fun ev ->
    (match ev with
     | Mf_event.Data _ -> incr total
     | Mf_event.Retract _ -> incr retracts
     | Mf_event.Update _ -> incr total
     | Mf_event.Watermark _ -> ());
    print_event ev);
  Printf.printf "\nИтого: %d алертов (%d retracts)\n" !total !retracts
