(** {1 Пайплайны примера 8 — чистые Stream-графы (без I/O)}

    Только stream-преобразования, никакого источника/sink. Это позволяет:
    - тестировать логику на [Stream.of_list] без Kafka (см.
      [test_ex08_graphs.ml]);
    - переиспользовать в примере с реальным Kafka
      ([ex08_multi_pipeline.ml]);
    - держать выразительную декларативную форму отдельно от транзакционной
      обвязки.

    Три пайплайна minePASS: низкое напряжение, потеря связи,
    позиционирование по RSSI. *)

open Miniflink
open Time

(** Входное сообщение телеметрии от лампы шахтёра. *)
type reading = {
  lamp    : string;
  beacon  : string;
  rssi    : float;
  voltage : float;
  ts_ms   : int;
}

(** Ключевание по лампе — один раз в типе. *)
module ByLamp = Keyed.Make (struct
  type t = reading
  let key r = r.lamp
end)

(** Выходной алерт. *)
type alert =
  | Low_voltage of string * float
  | No_contact  of string

let alert_to_string = function
  | Low_voltage (lamp, v) -> Printf.sprintf "LOW_VOLTAGE %s %.2f" lamp v
  | No_contact lamp       -> Printf.sprintf "NO_CONTACT %s" lamp

(** Позиция шахтёра. *)
type position = { p_lamp : string; p_est : float }

let position_to_string p =
  Printf.sprintf "POS %s %.1f" p.p_lamp p.p_est

(* ────────────────────────────────────────────────────────────────── *)
(** {2 Пайплайн 1: детектор низкого напряжения}

    Три комбинатора: фильтр просадок → дедуп с cooldown (один алерт на
    лампу раз в 60с, а не поток) → преобразование в алерт. Ни таймеров,
    ни ручного состояния. *)
let voltage_alerts
    : reading Mf_event.t Stream.t -> alert Mf_event.t Stream.t =
  fun input ->
  input
  |> Pipe.filter (fun r -> r.voltage < 3.2)
  |> Pipe.dedup (module ByLamp)
       ~rule:(fun r -> r.lamp)
       ~cooldown:(seconds 60)
  |> Pipe.map (fun r -> Low_voltage (r.lamp, r.voltage))

(* ────────────────────────────────────────────────────────────────── *)
(** {2 Пайплайн 2: детектор потери связи}

    Чистое {!Pipe.on_silence}: [No_contact], если от лампы не было данных
    дольше 30с. Детекция {e отсутствия} событий по event-time,
    персистентное per-key состояние из коробки. *)
let loss_alerts
    : reading Mf_event.t Stream.t -> alert Mf_event.t Stream.t =
  fun input ->
  input
  |> Pipe.on_silence (module ByLamp)
       ~within:(seconds 30)
       ~has:(fun _ -> true)
       ~on_silent:(fun lamp ~last:_ ~ts:_ -> No_contact lamp)

(* ────────────────────────────────────────────────────────────────── *)
(** {2 Пайплайн 3: позиционирование по RSSI}

    Медиана RSSI в скользящем окне 10с сглаживает шум сигнала перед
    оценкой позиции — {!Pipe.window_agg} с инкрементальной агрегацией. *)
let positioning
    : reading Mf_event.t Stream.t -> position Mf_event.t Stream.t =
  fun input ->
  input
  |> Pipe.window_agg (module ByLamp)
       (Pipe.sliding (seconds 10) (seconds 2))
       (Agg.median (fun r -> r.rssi))
  |> Pipe.flat_map (fun (lamp, med_opt) ->
       match med_opt with
       | None -> []
       | Some med_rssi ->
         [ { p_lamp = lamp; p_est = 10. ** ((-. med_rssi -. 40.) /. 20.) } ])
