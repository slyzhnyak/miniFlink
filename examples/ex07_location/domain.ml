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
    их median RSSI. Эмитится пайплайном локации. *)
type location = {
  loc_lamp : string;
  loc_wend : Time.t;
  loc_top2 : (string * float) list;   (** до 2 элементов: (ID маяка, median RSSI) *)
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
