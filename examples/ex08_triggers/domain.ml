(** Доменные алерты для ex08.

    Отдельный от ex07 [type alert] — потому что триггерная система
    эмитирует {b пары} Problem+Recovery, и ex07 в большинстве своих
    алертов не имеет recovery-конструкторов (см. раздел 4 статьи об
    append-only vs retract). Чтобы не править ex07, делаем здесь
    собственный набор, заточенный под Trigger.of_stream:
    каждый алерт имеет ``парный'' Recovery-вариант.

    Алерты соответствуют четырём демо-триггерам:
    - low_voltage / voltage_ok
    - voltage_critical / voltage_recovered (более жёсткий порог)
    - no_packets / packets_resumed
    - gas_co_warning / gas_co_safe *)

open Miniflink

type alert =
  (* Голос напряжения батареи. *)
  | Low_voltage      of { lamp: string; voltage: float; ts: Time.t }
  | Voltage_ok       of { lamp: string; ts: Time.t }

  (* Критический уровень (более жёсткий порог 3.0 В). *)
  | Voltage_critical of { lamp: string; voltage: float; ts: Time.t }
  | Voltage_recovered of { lamp: string; ts: Time.t }

  (* Отсутствие пакетов. *)
  | No_packets       of { lamp: string; silence_ms: Time.t; ts: Time.t }
  | Packets_resumed  of { lamp: string; ts: Time.t }

  (* Газ CO. *)
  | Gas_co_warning   of { lamp: string; ppm: float; ts: Time.t }
  | Gas_co_safe      of { lamp: string; ts: Time.t }

let alert_lamp = function
  | Low_voltage      { lamp; _ }
  | Voltage_ok       { lamp; _ }
  | Voltage_critical { lamp; _ }
  | Voltage_recovered { lamp; _ }
  | No_packets       { lamp; _ }
  | Packets_resumed  { lamp; _ }
  | Gas_co_warning   { lamp; _ }
  | Gas_co_safe      { lamp; _ } -> lamp

let alert_ts = function
  | Low_voltage      { ts; _ }
  | Voltage_ok       { ts; _ }
  | Voltage_critical { ts; _ }
  | Voltage_recovered { ts; _ }
  | No_packets       { ts; _ }
  | Packets_resumed  { ts; _ }
  | Gas_co_warning   { ts; _ }
  | Gas_co_safe      { ts; _ } -> ts
