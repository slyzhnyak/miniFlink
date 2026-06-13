(** Items для ex08 — функции преобразования исходных потоков в
    item-потоки (key, value), пригодные для скармливания Trigger.of_stream.

    Каждый item — отдельный stream обновлений. Этот файл показывает,
    что построение items — это {b простые композиции} стандартных
    операторов miniFlink:
    - простые (voltage, co_ppm) — через Pipe.map
    - timing-based (no_packets) — через Item.silence_age

    Здесь {b нет} никаких state-машин и custom операторов.
    Архитектурно это контраст с FSM-подходом из ex07. *)

open Miniflink
open Ex07_location_lib   (* переиспользуем Domain.packet/gas_packet *)

(** Voltage батареи. Один пакет — один update. *)
let voltage_item (source : Domain.packet Mf_event.t Stream.t)
  : (string * float) Mf_event.t Stream.t =
  source |> Pipe.map (fun (p : Domain.packet) -> (p.lamp, p.voltage))

(** «Время с последнего пакета» в миллисекундах. tick определяет
    разрешение реакции на тишину; 30 с — компромисс между точностью и
    нагрузкой эмиссии.

    Возвращает age как float, чтобы напрямую идти в условие
    [Trigger.greater_than threshold]. *)
let no_packets_item ?(tick = Time.seconds 30)
    (source : Domain.packet Mf_event.t Stream.t)
  : (string * float) Mf_event.t Stream.t =
  source
  |> Item.silence_age ~by:(fun (p : Domain.packet) -> p.lamp) ~tick
  |> Pipe.map (fun (lamp, age) -> (lamp, float_of_int age))

(** CO ppm из газового потока. Газовые сенсоры могут отсутствовать
    (None), такие пакеты фильтруем — Trigger ожидает реальное значение. *)
let gas_co_item (source : Domain.gas_packet Mf_event.t Stream.t)
  : (string * float) Mf_event.t Stream.t =
  source
  |> Pipe.flat_map (fun (g : Domain.gas_packet) ->
       match g.g_co with
       | Some ppm -> [(g.g_lamp, ppm)]
       | None -> [])
