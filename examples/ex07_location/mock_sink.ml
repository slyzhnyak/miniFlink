(** Мок-sink для алертов и локаций.

    Симметрично {!Mock_source}: в проде на это место станет
    [Kafka_sink.publish] или web-push, на нашем стенде — печать.

    Топология примера ([ex07_location.ml]) ничего не знает о том, что
    sink — это [stdout]; всё, что она видит — функции [publish_alert],
    [publish_location]. *)

open Miniflink
open Domain

(** {2 Алерты — каждый алерт публикуется отдельно} *)

(** Отрендерить один алерт (одна строка на stdout). *)
let publish_alert (a : alert) : unit =
  match a with
  | No_packets (lamp, Some t) ->
    Printf.printf "  ⚠ %s: НЕТ ПАКЕТОВ >%d мин (последний t=%dс)\n"
      lamp (no_packets_threshold / 60000) (t / 1000)
  | No_packets (lamp, None) ->
    Printf.printf "  ⚠ %s: пакетов не было ни разу\n" lamp
  | No_readings (lamp, Some t) ->
    Printf.printf "  ⚠ %s: пакеты идут, но не слышит маяки >%d мин (последние показания t=%dс)\n"
      lamp (no_readings_threshold / 60000) (t / 1000)
  | No_readings (lamp, None) ->
    Printf.printf "  ⚠ %s: не слышал маяки ни разу\n" lamp
  | No_motion (lamp, Some t) ->
    Printf.printf "  ⚠ %s: НЕ ДВИЖЕТСЯ >%d мин (последнее движение t=%dс)\n"
      lamp (no_motion_threshold / 60000) (t / 1000)
  | No_motion (lamp, None) ->
    Printf.printf "  ⚠ %s: не двигался ни разу\n" lamp
  | Low_voltage (lamp, v) ->
    Printf.printf "  ⚠ %s: НИЗКОЕ НАПРЯЖЕНИЕ %.2fВ (порог %.2fВ, подтверждено %d мин)\n"
      lamp v low_voltage_threshold (voltage_debounce / 60000)
  | Voltage_ok lamp ->
    Printf.printf "  ✓ %s: напряжение восстановилось\n" lamp
  | Sos (lamp, t) ->
    Printf.printf "  🆘 %s: НАЖАТА КНОПКА SOS (t=%dс)\n" lamp (t / 1000)

(** Опубликовать все алерты из потока (для финального батч-отчёта). *)
let publish_alerts (alerts : alert Mf_event.t Stream.t) : unit =
  let collected = Pipe.collect alerts in
  if collected = [] then Printf.printf "  все фонари в норме\n"
  else List.iter publish_alert collected

(** {2 Локации — компактный вывод последних 3 окон каждого шахтёра} *)

(** Отрендерить одну локацию в одну строку.

    [~beacon_coords] — функция поиска координат маяка. По умолчанию
    {!Domain.beacon_coords} (справочник из 5 биконов для базового
    сценария). Большая шахта (1024 биконов) подаёт свою функцию. *)
let publish_location ?(beacon_coords = beacon_coords) (loc : location) : unit =
  Printf.printf "  окно→%3dс: " (loc.loc_wend / 1000);
  match loc.loc_top2 with
  | [] -> Printf.printf "—\n"
  | xs ->
    List.iteri (fun i (beacon, med) ->
      match beacon_coords beacon with
      | Some (x, y, h) ->
        Printf.printf "%s%s %.1f dBm (x=%.0f y=%.0f h=%dм)"
          (if i > 0 then "; " else "") beacon med x y h
      | None ->
        Printf.printf "%s%s %.1f dBm"
          (if i > 0 then "; " else "") beacon med) xs;
    if List.length xs < 2 then
      Printf.printf "  ⚠ слышен 1 маяк — для трилатерации мало";
    print_newline ()

(** Сгруппировать локации по шахтёру, для каждого показать последние 3
    окна (если есть). Применяет [Pipe.materialize] чтобы свернуть
    ретракты (опоздавший пакет → перевыпуск окна) в одно финальное
    значение на пару (шахтёр, конец окна).

    Семантически это «снимок состояния пульта в момент завершения
    смены». В проде Kafka_sink публикует {e каждое} обновление окна
    (включая ретракты), а консьюмер слева их применяет.

    [~miners] — список шахтёров, для которых печатать вывод. По
    умолчанию M1..M6 (для базового сценария ex07). Большая шахта
    подаёт свой список (но обычно бенчмарк всё равно использует
    {!publish_alerts_summary}, а не покадровый вывод). *)
let publish_locations
    ?(beacon_coords = beacon_coords)
    ?(miners = ["M1"; "M2"; "M3"; "M4"; "M5"; "M6"])
    (locations : location Mf_event.t Stream.t) : unit =
  let materialized =
    locations
    |> Pipe.materialize ~by:(fun loc _wend -> (loc.loc_lamp, loc.loc_wend)) in
  let by_lamp = Hashtbl.create 8 in
  List.iter (fun ((lamp, wend), loc) ->
    let prev = try Hashtbl.find by_lamp lamp with Not_found -> [] in
    Hashtbl.replace by_lamp lamp ((wend, loc.loc_top2) :: prev)
  ) materialized;
  miners |> List.iter (fun lamp ->
    match Hashtbl.find_opt by_lamp lamp with
    | None | Some [] ->
      Printf.printf "%s: нет окон с показаниями\n\n" lamp
    | Some wins ->
      let last3 = List.sort (fun (a, _) (b, _) -> compare b a) wins
                  |> List.filteri (fun i _ -> i < 3)
                  |> List.rev in
      Printf.printf "%s:\n" lamp;
      List.iter (fun (wend, top2) ->
        publish_location ~beacon_coords
          { loc_lamp = lamp; loc_wend = wend; loc_top2 = top2;
            loc_position = Pipelines.interpolate_position
                             ~find_beacon:beacon_coords top2 }
      ) last3;
      print_newline ())

(** Количество [Retract] в потоке — на сколько окон опоздавшие пакеты
    вызвали пересчёт. Используется для прозрачности отчёта. *)
let count_retracts (events : 'a Mf_event.t list) : int =
  List.length (List.filter
    (function Mf_event.Retract _ -> true | _ -> false) events)

(** {1 Газовые алерты} *)

(** Отрендерить один газовый алерт в строку. Включает позицию если она
    известна — это и есть смысл всей retract-семантики: диспетчер видит
    обновление позиции автоматически. *)
let publish_gas_alert (ev : gas_alert Mf_event.t) : unit =
  match ev with
  | Mf_event.Data (Gas_alert a, _) ->
    let pos_str = match a.ga_position with
      | None -> "позиция неизвестна"
      | Some (x, y, h) ->
        Printf.sprintf "x=%.0f y=%.0f h=%dм" x y h in
    let icon = match a.ga_level with
      | Warning -> "⚠"
      | Critical -> "🆘" in
    Printf.printf "  %s %s: %s %s = %.0f ppm (%s)\n"
      icon a.ga_lamp (Domain.gas_level_name a.ga_level)
      (Domain.gas_name a.ga_gas) a.ga_ppm pos_str
  | Mf_event.Data (Gas_resolved r, _) ->
    Printf.printf "  ✓ %s: %s вернулся в норму\n"
      r.gr_lamp (Domain.gas_name r.gr_gas)
  | Mf_event.Retract _ ->
    (* Retract'ы свернуты в материализованной финальной картине ниже. *)
    ()
  | Mf_event.Watermark _ -> ()

(** Сгруппировать алерты по lamp+gas, применяя retract'ы. Печатаем
    итоговую картину — то, что диспетчер видит в моменте. *)
let publish_gas_alerts (events : gas_alert Mf_event.t Stream.t) : unit =
  let materialized =
    events
    |> Pipe.materialize ~by:(fun a _t ->
         match a with
         | Gas_alert ga -> `Alert (ga.ga_lamp, ga.ga_gas)
         | Gas_resolved gr -> `Resolved (gr.gr_lamp, gr.gr_gas)) in
  let active = Hashtbl.create 8 in
  List.iter (fun (key, alert) ->
    match key, alert with
    | `Alert (lamp, gas), Gas_alert _ ->
      Hashtbl.replace active (lamp, gas) alert
    | `Resolved (lamp, gas), Gas_resolved _ ->
      Hashtbl.remove active (lamp, gas)
    | _ -> ()
  ) materialized;
  if Hashtbl.length active = 0 then
    Printf.printf "  газовых алертов нет\n"
  else
    Hashtbl.iter (fun _ alert ->
      publish_gas_alert (Mf_event.data alert 0)
    ) active
