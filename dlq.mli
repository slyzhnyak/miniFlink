(* ============================================================
   Dlq.mli — Dead Letter Queue

   Невалидные / необработанные сообщения не роняют pipeline —
   они уходят в DLQ с контекстом ошибки.

   Реализации:
   dlq_noop.ml     — молча выбрасывает
   dlq_log.ml      — пишет в stderr
   dlq_kafka.ml    — публикует в Kafka топик "*.dlq"
   ============================================================ *)

type t

type entry = {
  topic     : string;
  payload   : bytes;
  error     : string;
  ts        : int;        (* Unix timestamp ms *)
  attempt   : int;
}

val create : unit -> t
val send   : t -> entry -> unit
val flush  : t -> unit
val count  : t -> int     (* сколько сообщений отправлено в DLQ *)
