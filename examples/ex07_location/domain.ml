(** Модели данных, справочники и пороги — общая основа примера.

    Этот модуль не зависит ни от чего кроме [Miniflink] и [Time]; на нём
    стоят источник, sink и пайплайны. Менять сценарий (другая шахта,
    другие пороги) — здесь и только здесь. *)

open Miniflink

(** Пакет, который шлёт фонарь шахтёра (~ каждые 15 секунд).

    {b Контракт пакета:} ID фонаря, до пяти самых слышимых маяков с их
    RSSI (маяки слышны только на своём горизонте), напряжение батареи,
    признак движения (true если фонарь двигался с прошлого пакета),
    признак нажатой кнопки SOS, event-time. *)
type packet = {
  lamp     : string;                  (** ID фонаря *)
  readings : (string * float) list;   (** до 5: (ID маяка, RSSI dBm) *)
  voltage  : float;                   (** напряжение батареи, вольты *)
  moving   : bool;                    (** двигался ли с прошлого пакета *)
  sos      : bool;                    (** нажата ли кнопка SOS *)
  ts       : Time.t;                  (** event-time *)
}

(** Внутренний тип одного RSSI-показания после разворота пакета. *)
type reading = {
  r_lamp   : string;
  r_beacon : string;
  r_rssi   : float;
  r_ts     : Time.t;
}

(** Алерты, эмитируемые пайплайном контроля состояния шахтёра.

    Шесть конструкторов — четыре независимые подсистемы плюс явный
    «напряжение восстановилось» (для прозрачности диспетчерскому пульту,
    чтобы было видно: тревога снята). *)
type alert =
  | No_packets   of string * Time.t option  (** пакетов нет >2 мин *)
  | No_readings  of string * Time.t option  (** пакеты идут, маяков нет >5 мин *)
  | No_motion    of string * Time.t option  (** не двигался дольше порога *)
  | Low_voltage  of string * float          (** напряжение подтверждённо ниже порога *)
  | Voltage_ok   of string                  (** напряжение вернулось в норму *)
  | Sos          of string * Time.t         (** нажата кнопка SOS *)

(** Результат локации в одном окне: шахтёр, конец окна, top-2 маяков с
    их median RSSI, и расчётная позиция шахтёра.

    [loc_position] — линейная интерполяция между двумя сильнейшими
    маяками (см. {!Pipelines.interpolate_position}). Шахтный штрек —
    линия с биконами вдоль неё, поэтому интерполяция между двумя
    соседними маяками — корректный метод для этой геометрии (не
    упрощение). [None] если в окне меньше 2 маяков. *)
type location = {
  loc_lamp     : string;
  loc_wend     : Time.t;
  loc_top2     : (string * float) list;          (** до 2: (ID маяка, median RSSI) *)
  loc_position : (float * float * int) option;   (** (x, y, depth_m) *)
}

(** Справочник координат маяков связи: ID → (x, y, горизонт). *)
let beacons : (string, float * float * int) Hashtbl.t =
  [ "B1", (120.0,  40.0, -160);
    "B2", (180.0,  40.0, -160);
    "B3", (240.0,  95.0, -240);
    "B4", (300.0,  95.0, -240);
    "B5", (360.0, 150.0, -240);
  ] |> List.to_seq |> Hashtbl.of_seq

(** Координаты маяка из справочника или [None]. *)
let beacon_coords (b : string) : (float * float * int) option =
  Hashtbl.find_opt beacons b

(** {2 Пороги диагностики (event-time, миллисекунды)} *)

let no_packets_threshold  : Time.t = Time.minutes 2
(** Heartbeat «нет пакетов» — сбрасывается любым пакетом, в том числе пустым. *)

let no_readings_threshold : Time.t = Time.minutes 5
(** Heartbeat «не слышит маяки» — сбрасывается только пакетами с показаниями. *)

let no_motion_threshold   : Time.t = Time.minutes 2
(** Не двигался дольше — порог демо; в проде ~10 мин. *)

let low_voltage_threshold : float = 3.5
(** Вход в состояние подозрения. Хитеризис: возврат — только при пороге [voltage_ok]. *)

let voltage_ok_threshold  : float = 3.7
(** Возврат в норму. Гарантирует «нет дребезга у границы» и ловит факт зарядки/замены. *)

let voltage_debounce      : Time.t = Time.minutes 2
(** Алерт [Low_voltage] выдаётся только если V<low держится дольше debounce. *)

(** {2 Газы — типы и пороги}

    Шахтёр шлёт пакеты с газовыми сенсорами ОТДЕЛЬНО от RSSI-пакетов
    (раз в 10 секунд, обычно). У разных моделей фонарей разный набор
    сенсоров — поэтому каждое поле опционально. *)

(** Пакет газовых показаний от одного фонаря. *)
type gas_packet = {
  g_lamp : string;
  g_ts   : Time.t;
  g_co2  : float option;   (* CO2, ppm *)
  g_co   : float option;   (* CO, ppm *)
  g_h2   : float option;   (* H2, ppm (1% = 10000 ppm) *)
  g_ch4  : float option;   (* CH4, ppm — самый опасный в угольной шахте *)
}

(** Идентификатор газа в алерте — для индексации state и отображения. *)
type gas = Gas_CO2 | Gas_CO | Gas_H2 | Gas_CH4

let gas_name = function
  | Gas_CO2 -> "CO2"
  | Gas_CO  -> "CO"
  | Gas_H2  -> "H2"
  | Gas_CH4 -> "CH4"

(** Уровень опасности. Промежуточных нет — диспетчер не должен мучиться
    с тонкой градацией. *)
type gas_level = Warning | Critical

let gas_level_name = function
  | Warning  -> "WARNING"
  | Critical -> "CRITICAL"

(** Газовый алерт. Алерты {b sticky}: один алерт (lamp, gas) живой пока
    шахтёр в опасной зоне. При смене уровня (Warning↔Critical),
    существенном изменении ppm (>20%), или обновлении position —
    старый алерт {!Mf_event.retract}'ится, эмитится новый с
    обновлёнными данными. Когда газ возвращается в норму — эмитится
    {!Gas_resolved} и алерт перестаёт быть «живым». *)
type gas_alert =
  | Gas_alert of {
      ga_lamp     : string;
      ga_ts       : Time.t;
      ga_gas      : gas;
      ga_level    : gas_level;
      ga_ppm      : float;
      ga_position : (float * float * int) option;
    }
  | Gas_resolved of {
      gr_lamp : string;
      gr_ts   : Time.t;
      gr_gas  : gas;
    }

(** Пороги газов в ppm. Ориентированы на угольную шахту; в реальной
    инсталляции калибруются под условия и регуляторов. *)

let co2_warning  = 5000.    (* CO2: норма ~400, плохо >5000 *)
let co2_critical = 15000.   (* Эвакуация *)
let co_warning   = 50.      (* CO: рабочая зона <50 *)
let co_critical  = 100.     (* Острая токсичность *)
let h2_warning   = 4000.    (* H2: 0.4% (LEL 4%) *)
let h2_critical  = 10000.   (* 1%, четверть LEL *)
let ch4_warning  = 5000.    (* CH4: 0.5%, метан опасен с 1% *)
let ch4_critical = 10000.   (* 1%, критический порог для угольной шахты *)

(** Решить уровень для одного газа по показанию в ppm.
    [None] если в норме (ниже warning-порога). *)
let gas_level_for (g : gas) (ppm : float) : gas_level option =
  let warn, crit = match g with
    | Gas_CO2 -> co2_warning, co2_critical
    | Gas_CO  -> co_warning, co_critical
    | Gas_H2  -> h2_warning, h2_critical
    | Gas_CH4 -> ch4_warning, ch4_critical
  in
  if ppm >= crit then Some Critical
  else if ppm >= warn then Some Warning
  else None

(** Порог «существенного» изменения ppm для refresh-эмиссии. 20% —
    баланс «диспетчер видит рост опасности, но не флудит на колебаниях
    датчика». *)
let gas_ppm_significant_change = 0.20
