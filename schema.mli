(* ============================================================
   Schema.mli — версионирование и эволюция схем

   Проблема: новое поле в telemetry → старые сообщения
   не декодируются → pipeline падает.

   Решение: версионированный codec с migration функциями.

   Реализации:
   schema_noop.ml    — без версионирования, decode-or-fail
   schema_default.ml — версия в заголовке, цепочка миграций
   ============================================================ *)

type version = int

(** Versioned codec: encode добавляет версию, decode мигрирует *)
type 'a t

val make :
  version  : version ->
  encode   : ('a -> bytes) ->
  decode   : (bytes -> ('a, string) result) ->
  ?migrate : (version -> bytes -> bytes) ->  (* old_ver → migrate bytes *)
  unit -> 'a t

val encode : 'a t -> 'a -> bytes
val decode : 'a t -> bytes -> ('a, string) result

val current_version : 'a t -> version
