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

(** Tagged union — какой из трёх items обновился. *)
type update =
  | U_voltage of string * float
  | U_co      of string * float
  | U_rssi    of string * float

(** Извлечение ключа для process_keyed. *)
module By_update : Keyed.S with type t = update = struct
  type t = update
  let key = function
    | U_voltage (l, _) | U_co (l, _) | U_rssi (l, _) -> l
end

(** Per-key state — последние видимые значения каждой компоненты. *)
type combined_state = {
  mutable co_ppm      : float;
  mutable voltage     : float;
  mutable avg_rssi    : float;
  mutable has_co      : bool;
  mutable has_voltage : bool;
  mutable has_rssi    : bool;
}

let make_state () = {
  co_ppm = 0.; voltage = 4.0; avg_rssi = 0.;
  has_co = false; has_voltage = false; has_rssi = false;
}

(** Главный derived item: объединяет три потока в combined-record.

    Семантика: на любое обновление одного из items выдаём
    свежий combined со всеми тремя значениями (или соответствующими
    флагами has_co/has_voltage/has_rssi). Триггер дальше решает
    считать ли triplet валидным.

    Тонкость с watermarks: Mf_event.union берёт минимум входных
    watermark'ов. Если какой-то поток сильно отстаёт, downstream
    блокируется. В демо это норма; в проде понадобится idle-watermark
    для замолчавших источников. *)
let combined_item
    ~(voltage : (string * float) Mf_event.t Stream.t)
    ~(co      : (string * float) Mf_event.t Stream.t)
    ~(rssi    : (string * float) Mf_event.t Stream.t)
  : (string * Domain.combined) Mf_event.t Stream.t =
  let s_v = voltage |> Pipe.map (fun (l, v) -> U_voltage (l, v)) in
  let s_c = co      |> Pipe.map (fun (l, v) -> U_co      (l, v)) in
  let s_r = rssi    |> Pipe.map (fun (l, v) -> U_rssi    (l, v)) in
  let unioned = Mf_event.union s_v (Mf_event.union s_c s_r) in
  unioned
  |> Pipe.process_keyed (module By_update)
       ~init:make_state
       ~on_event:(fun ctx key st update ->
         (match update with
          | U_voltage (_, v) -> st.voltage  <- v; st.has_voltage <- true
          | U_co      (_, v) -> st.co_ppm   <- v; st.has_co      <- true
          | U_rssi    (_, v) -> st.avg_rssi <- v; st.has_rssi    <- true);
         let snapshot : Domain.combined = {
           co_ppm      = st.co_ppm;
           voltage     = st.voltage;
           avg_rssi    = st.avg_rssi;
           has_co      = st.has_co;
           has_voltage = st.has_voltage;
           has_rssi    = st.has_rssi;
         } in
         ctx.emit (key, snapshot))
       ~on_timer:(fun _ _ _ _ _ -> ())  (* без таймеров *)
