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

(** {1 Bundle для persistence-параметров}

    {b Историческая заметка.} Раньше каждый stateful-оператор принимал
    persistence явным параметром [~persistence] — пакетом из backend,
    namespace и пары (де)сериализаторов (тип [persist] ниже). Эта схема
    {e устарела}: persistence стала ортогональной. Операторы
    ([Item.silence_age], [Pipe.process_keyed], [Pipe.window_fold],
    [Trigger.of_stream]) больше НЕ принимают [~persistence] и НЕ требуют
    сериализаторов — их состояние closure-free и снапшотится через
    [Marshal] автоматически, когда пайплайн обёрнут в durable
    [Runtime_context]. См. docs/orthogonal-persistence.md.

    Тип [persist] оставлен для обратной совместимости и как описание
    backend-контракта; новый код его не использует. *)

type 'v persist = {
  backend     : t;
  name        : string;
  serialize   : 'v -> Yojson.Safe.t;
  deserialize : Yojson.Safe.t -> 'v;
}
(** {b Legacy.} Пакет параметров persistence старой (не ортогональной)
    модели. Операторы больше его не принимают; см. историческую заметку
    выше. Сохранён для обратной совместимости.

    - [backend] — KV-хранилище
    - [name] — namespace оператора в backend'е
    - [serialize]/[deserialize] — конвертация значения в JSON и обратно *)
