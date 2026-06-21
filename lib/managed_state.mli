(** Keyed-состояние оператора с прозрачной persistence.

    Оператор использует это ВМЕСТО сырого Hashtbl. Снаружи — обычная
    keyed-map. Persistence решается ambient {!Runtime_context}:
    в Ephemeral это память, в Durable — restore на старте + snapshot
    по {!checkpoint}. Оператор не пишет сериализаторов и не ветвит
    логику: один код в обоих режимах.

    Namespace [~name] задаётся оператором (стабилен при рестарте),
    пользователь его не видит. Ключ в backend — ["{name}:{user_key}"]. *)

type ('k, 'v) t

(** Создать managed-state, прочитав текущий {!Runtime_context}.
    В Durable-режиме восстанавливает записи из backend по префиксу
    [name]. [~key_str]/[~key_unstr] — биекция ключа в строку. *)
val create :
  name:string ->
  key_str:('k -> string) ->
  key_unstr:(string -> 'k) ->
  unit -> ('k, 'v) t

(** Managed-state со строковым ключом (частый случай). *)
val create_string : name:string -> unit -> (string, 'v) t

val get : ('k, 'v) t -> 'k -> 'v option
val set : ('k, 'v) t -> 'k -> 'v -> unit
val remove : ('k, 'v) t -> 'k -> unit
val mem : ('k, 'v) t -> 'k -> bool
val fold : ('k, 'v) t -> ('k -> 'v -> 'a -> 'a) -> 'a -> 'a
val iter : ('k, 'v) t -> ('k -> 'v -> unit) -> unit
val size : ('k, 'v) t -> int

(** Снапшот состояния в backend. Зовётся оператором на
    checkpoint-barrier (watermark). В ephemeral — noop, поэтому
    оператор зовёт безусловно. *)
val checkpoint : ('k, 'v) t -> unit

(** Удалить ключ из памяти И backend (eviction старых окон/ключей). *)
val evict : ('k, 'v) t -> 'k -> unit

(** Durable ли состояние. *)
val is_durable : ('k, 'v) t -> bool
