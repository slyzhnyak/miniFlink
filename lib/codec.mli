(** Кодеки (де)сериализации значений в/из [bytes].

    [decode] возвращает [result] — битый ввод даёт [Error], не исключение.
    Смена формата (JSON ↔ Protobuf) — замена кодека одной строкой, сам
    пайплайн не меняется. *)

(** Кодек значений типа ['a]. *)
type 'a t = {
  encode : 'a -> bytes;
  decode : bytes -> ('a, string) result;
  name   : string;            (** имя формата (для логов/метрик) *)
}

(** JSON-кодек поверх Yojson. [encode]/[decode] работают с
    [Yojson.Safe.t]; ошибки парсинга JSON оборачиваются в [Error]. *)
val json :
  encode:('a -> Yojson.Safe.t) ->
  decode:(Yojson.Safe.t -> ('a, string) result) ->
  'a t

(** Protobuf-подобный кодек поверх произвольных [bytes].
    Исключение из [decode] оборачивается в [Error]. *)
val protobuf :
  encode:('a -> bytes) ->
  decode:(bytes -> 'a) ->
  'a t
