(** Общий интерфейс key-value backend'а для stateful операторов.

    Изначально появился в [Trigger] для persistence триггеров.
    Вынесен в отдельный модуль чтобы [Item.silence_age],
    [Pipe.process_keyed] и другие stateful операторы могли
    использовать тот же backend (например, RocksDB instance
    разделяется между триггерами и silence-таймерами).

    Это {b first-class value} (record-of-functions), не functor —
    позволяет передавать любую реализацию (memory, RocksDB, mock)
    как параметр функции без необходимости заранее параметризовать
    operator. *)

type t = {
  get    : string -> bytes option;
  set    : string -> bytes -> unit;
  delete : string -> unit;
  keys   : unit -> string list;
}
(** Минимальный key-value интерфейс. Ключи — strings, значения —
    bytes (обычно JSON через Yojson, но это выбор пользователя). *)

val of_memory : (string, bytes) Hashtbl.t -> t
(** Обёртка над in-memory [Hashtbl.t]. Используется в тестах и
    простых сценариях где persistence на диск не нужна (например,
    сохранение state между фазами теста без файловой системы). *)
