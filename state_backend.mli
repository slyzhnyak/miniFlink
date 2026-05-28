(* ============================================================
   State_backend.mli — хранилище стейта операторов

   Ключ: строка (обычно operator_id + ":" + item_key)
   Значение: байты (Marshal или Protobuf)

   Три реализации:
   state_backend_memory.ml  — Hashtbl, теряется при рестарте
   state_backend_rocksdb.ml — персистентный, переживает рестарт
   state_backend_noop.ml    — /dev/null, для тестов без стейта
   ============================================================ *)

type t

val create  : unit -> t
val get     : t -> string -> bytes option
val set     : t -> string -> bytes -> unit
val delete  : t -> string -> unit
val keys    : t -> string list

(** Атомарный снапшот всего стейта для checkpoint *)
val snapshot : t -> bytes

(** Восстановить стейт из снапшота *)
val restore  : t -> bytes -> unit

(** Сколько ключей *)
val size : t -> int
