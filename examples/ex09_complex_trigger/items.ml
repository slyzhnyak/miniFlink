(** Items для ex09.

    Три исходных item-потока (voltage, co_ppm, avg_rssi) объединяются
    в один [(string * combined) Mf_event.t Stream.t] через union +
    process_keyed.

    Эта структура — каноническая для сложных триггеров над несколькими
    items. *)

open Miniflink

(** Простой voltage item. *)
let voltage_item
    (packets : Ex07_location_lib.Domain.packet Mf_event.t Stream.t)
  : (string * float) Mf_event.t Stream.t =
  packets |> Pipe.map (fun (p : Ex07_location_lib.Domain.packet) -> (p.lamp, p.voltage))

(** CO ppm из газового потока. None-сенсоры фильтруем. *)
let co_item
    (gas : Ex07_location_lib.Domain.gas_packet Mf_event.t Stream.t)
  : (string * float) Mf_event.t Stream.t =
  gas
  |> Pipe.flat_map (fun (g : Ex07_location_lib.Domain.gas_packet) ->
       match g.g_co with
       | Some ppm -> [(g.g_lamp, ppm)]
       | None -> [])

(** Скользящее среднее RSSI за 5 минут.

    Шаги:
    1. Каждый пакет даёт несколько RSSI до разных маяков. Берём
       {b максимум} — это "сила связи шахтёра" (сильнейший маяк).
    2. window_agg_keyed по lamp с sliding 5min / 30s,
       агрегат - среднее.
    3. Распаковываем результат в (lamp, avg). *)
let avg_rssi_item
    (packets : Ex07_location_lib.Domain.packet Mf_event.t Stream.t)
  : (string * float) Mf_event.t Stream.t =
  packets
  |> Pipe.flat_map (fun (p : Ex07_location_lib.Domain.packet) ->
       (* Максимум RSSI среди всех маяков пакета. Если readings
          пустой — пропускаем (для отсутствия пакетов другой
          триггер). *)
       match p.readings with
       | [] -> []
       | rs ->
         let max_rssi = List.fold_left
             (fun acc (_, r) -> if r > acc then r else acc)
             neg_infinity rs in
         [(p.lamp, max_rssi)])
  |> Pipe.event_time ~lateness:(Time.seconds 1)
  |> Pipe.window_agg_keyed
       ~by:fst
       (Pipe.sliding (Time.minutes 5) (Time.seconds 30))
       Agg.(mean (fun (_lamp, r) -> r))
  |> Pipe.flat_map (fun (lamp, avg_opt) ->
       match avg_opt with
       | Some avg -> [(lamp, avg)]
       | None -> [])

(** Per-station Keyed.S для join'а — все три stream'а имеют тип
    [(string * float)] и ключуются по station name. *)
module By_lamp : Keyed.S with type t = (string * float) = struct
  type t = string * float
  let key (l, _) = l
end

(** Главный derived item: объединяет три потока в combined-record.

    Семантика: на любое обновление одного из items выдаём
    свежий combined со всеми тремя значениями (или соответствующими
    флагами has_co/has_voltage/has_rssi). Триггер дальше решает
    считать ли triplet валидным.

    Реализация через {!Pipe.keyed_join}: оператор сам ведёт per-key
    snapshot последних значений, эмитя на каждое обновление в любом
    канале. Порядок входов: voltage, co, rssi — это позиции в
    output [option list].

    Тонкость с watermarks: keyed_join использует {!Mf_event.union}
    которая берёт минимум входных watermark'ов. Если какой-то поток
    сильно отстаёт, downstream блокируется. В демо это норма; в
    проде понадобится idle-watermark для замолчавших источников. *)
let combined_item
    ~(voltage : (string * float) Mf_event.t Stream.t)
    ~(co      : (string * float) Mf_event.t Stream.t)
    ~(rssi    : (string * float) Mf_event.t Stream.t)
  : (string * Domain.combined) Mf_event.t Stream.t =
  Pipe.keyed_join (module By_lamp) [voltage; co; rssi]
  |> Pipe.map (fun (lamp, opts) ->
    let v_opt, c_opt, r_opt = match opts with
      | [a; b; c] -> a, b, c
      | _ -> assert false  (* keyed_join сохраняет порядок и длину *)
    in
    let voltage_val   = match v_opt with Some (_, v) -> v | None -> 4.0 in
    let co_ppm_val    = match c_opt with Some (_, v) -> v | None -> 0.0 in
    let avg_rssi_val  = match r_opt with Some (_, v) -> v | None -> 0.0 in
    let combined : Domain.combined = {
      co_ppm      = co_ppm_val;
      voltage     = voltage_val;
      avg_rssi    = avg_rssi_val;
      has_co      = c_opt <> None;
      has_voltage = v_opt <> None;
      has_rssi    = r_opt <> None;
    } in
    (lamp, combined))

