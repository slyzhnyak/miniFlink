(** Доменные алерты ex09 — критическая ситуация эвакуации.

    Один тип алертов, один тип значения (triplet). Минимальный набор
    конструкторов для иллюстрации сложного триггера. *)

open Miniflink

(** Состояние шахтёра по трём измерениям. Используется как тип
    значения, к которому применяется триггер evacuation_critical. *)
type combined = {
  co_ppm        : float;   (** последнее видимое значение CO в ppm *)
  voltage       : float;   (** последнее видимое напряжение батареи *)
  avg_rssi      : float;   (** скользящее среднее RSSI за 5 минут *)
  has_co        : bool;    (** есть ли свежее значение CO *)
  has_voltage   : bool;    (** есть ли свежее значение voltage *)
  has_rssi      : bool;    (** есть ли свежее значение avg_rssi *)
}

(** Алерты ex09. Только две пары: критическая ситуация и её
    разрешение. *)
type alert =
  | Evacuation_critical of {
      lamp     : string;
      co       : float;
      voltage  : float;
      rssi     : float;
      ts       : Time.t;
    }
  | Evacuation_cleared of {
      lamp     : string;
      ts       : Time.t;
    }

let alert_lamp = function
  | Evacuation_critical { lamp; _ } | Evacuation_cleared { lamp; _ } -> lamp

let alert_ts = function
  | Evacuation_critical { ts; _ } | Evacuation_cleared { ts; _ } -> ts
