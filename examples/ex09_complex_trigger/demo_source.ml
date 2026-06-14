(** Demo source для ex09.

    Собственный mock-источник с двумя шахтёрами:
    - {b M_critical}: попадает в evacuation-сценарий. Voltage линейно
      падает с 3.8 до 3.0 за 8 минут. CO растёт от 30 до 100 ppm.
      RSSI слабый (~-80 dBm) — шахтёр в дальнем углу выработки.
    - {b M_safe}: контрольный, всё в норме. Близко к биконам (RSSI ~-45),
      voltage норм (3.9), CO низкое (10 ppm).

    Симуляция 8 минут event-time, шаг пакета 15 секунд (как в реальной
    шахте). Газовый пакет каждые 10 секунд.

    Не трогает Mock_source в ex07 — это полностью изолированный
    источник для ex09. *)

open Miniflink

(* Точные пороги триггера, чтобы было видно когда и где срабатывает *)

let sim_minutes      = 8
let step_seconds     = 15
let gas_period       = 10

let total_ms         = sim_minutes * 60 * 1000
let rssi_steps       = total_ms / (step_seconds * 1000) + 1
let gas_steps        = total_ms / (gas_period   * 1000) + 1

(* ─── Шахтёр M_critical: все три условия в опасной зоне ─── *)

(** Voltage линейно падает 3.8 -> 3.0 за всю симуляцию.
    Пересекает порог 3.5 примерно в середине симуляции (t≈4 мин). *)
let m_crit_voltage (i : int) : float =
  let frac = float_of_int i /. float_of_int (rssi_steps - 1) in
  3.8 -. 0.8 *. frac

(** Слабый RSSI до двух дальних биконов. *)
let m_crit_readings _i =
  [ ("B_far1", -80.0); ("B_far2", -82.0) ]

(** CO начинается с 30 ppm, на t=240с (4 мин) скачок до 100 ppm,
    держится до конца симуляции. *)
let m_crit_co_at_t (t_ms : Time.t) : float =
  if t_ms < 240_000 then 30.0  (* безопасно *)
  else 100.0                    (* в зоне срабатывания *)

(* ─── Шахтёр M_safe: всё в норме ─── *)

let m_safe_voltage _i = 3.9
let m_safe_readings _i = [ ("B_near", -45.0) ]
let m_safe_co_at_t _t = 10.0

(* ─── Построение списков пакетов ─── *)

let rssi_packet (lamp : string) (i : int) : Ex07_location_lib.Domain.packet =
  let ts = i * step_seconds * 1000 in
  match lamp with
  | "M_critical" ->
    { lamp; ts;
      readings = m_crit_readings i;
      voltage  = m_crit_voltage i;
      moving   = true;
      sos      = false; }
  | "M_safe" ->
    { lamp; ts;
      readings = m_safe_readings i;
      voltage  = m_safe_voltage i;
      moving   = true;
      sos      = false; }
  | _ -> failwith "unknown lamp"

let gas_packet (lamp : string) (i : int) : Ex07_location_lib.Domain.gas_packet =
  let ts = i * gas_period * 1000 in
  let co_value = match lamp with
    | "M_critical" -> m_crit_co_at_t ts
    | "M_safe"     -> m_safe_co_at_t ts
    | _ -> failwith "unknown lamp"
  in
  { g_lamp = lamp; g_ts = ts;
    g_co  = Some co_value;
    g_co2 = None; g_h2 = None; g_ch4 = None; }

let all_rssi_packets () : Ex07_location_lib.Domain.packet list =
  let make_for lamp =
    List.init rssi_steps (fun i -> rssi_packet lamp i)
  in
  List.concat [make_for "M_critical"; make_for "M_safe"]

let all_gas_packets () : Ex07_location_lib.Domain.gas_packet list =
  let make_for lamp =
    List.init gas_steps (fun i -> gas_packet lamp i)
  in
  List.concat [make_for "M_critical"; make_for "M_safe"]

(* ─── Source-функции (как Mock_source.S, но проще) ─── *)

let read () : Ex07_location_lib.Domain.packet Mf_event.t Stream.t =
  let pkts = all_rssi_packets () in
  (* Сортируем по ts чтобы watermark монотонен. *)
  let sorted = List.sort
    (fun (a : Ex07_location_lib.Domain.packet) b -> compare a.ts b.ts) pkts in
  Mf_event.of_list ~ts:(fun (p : Ex07_location_lib.Domain.packet) -> p.ts) sorted

let read_gas () : Ex07_location_lib.Domain.gas_packet Mf_event.t Stream.t =
  let pkts = all_gas_packets () in
  let sorted = List.sort
    (fun (a : Ex07_location_lib.Domain.gas_packet) b -> compare a.g_ts b.g_ts) pkts in
  Mf_event.of_list ~ts:(fun (g : Ex07_location_lib.Domain.gas_packet) -> g.g_ts) sorted
