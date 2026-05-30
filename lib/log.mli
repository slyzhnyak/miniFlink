(** Структурированное логирование.

    Библиотека порождает {e структурированные} лог-события (уровень,
    сообщение, набор полей), а {b куда} их писать — решает приложение
    через {!set_sink}. По умолчанию события форматируются в JSON и
    пишутся в [stderr]. Приложение может перенаправить их в файл, Loki,
    свою систему — установив собственный sink, получающий уже готовую
    структуру {!event}, а не строку.

    Это держит границу библиотека/приложение чистой: библиотека решает
    {e что} произошло (событие с полями), приложение — {e куда} и {e как}
    это пишется. *)

(** Уровень важности события. *)
type level = Debug | Info | Warning | Error

(** Структурированное лог-событие. *)
type event = {
  level   : level;
  message : string;                  (** человекочитаемое сообщение *)
  fields  : (string * string) list;  (** структурированные пары ключ-значение *)
  ts_ms   : int;                     (** wall-clock метка, мс с эпохи *)
}

(** Сериализовать событие в одну строку JSON (поля level, ts, msg + все
    [fields]). Удобно для приложений пишущих в Loki/ELK. *)
val to_json : event -> string

(** Установить обработчик событий. По умолчанию — JSON в stderr.
    Приложение вызывает это чтобы перенаправить логи куда нужно. *)
val set_sink : (event -> unit) -> unit

(** Минимальный уровень: события ниже отбрасываются (по умолчанию [Info]). *)
val set_level : level -> unit

(** Породить событие. Обычно используются обёртки ниже. *)
val emit : level -> ?fields:(string * string) list -> string -> unit

val debug : ?fields:(string * string) list -> string -> unit
val info  : ?fields:(string * string) list -> string -> unit
val warn  : ?fields:(string * string) list -> string -> unit
val error : ?fields:(string * string) list -> string -> unit
