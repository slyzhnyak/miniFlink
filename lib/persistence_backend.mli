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

    Когда оператор использует persistence, ему нужны четыре связанные
    вещи: backend, namespace в нём, и пара (де)сериализаторов для
    пользовательского типа. Эти параметры всегда идут вместе — если
    есть один, нужны все. Тип [persist] объединяет их в один
    record чтобы передавать одним именованным параметром вместо
    четырёх:

    {[
      (* Раньше: *)
      Item.silence_age
        ~backend
        ~backend_name:"no_packets"
        ~serialize_key:(fun k -> `String k)
        ~deserialize_key:(fun j -> Yojson.Safe.Util.to_string j)
        ~by:... ~tick:... source

      (* Теперь: *)
      let pst = {
        backend; name = "no_packets";
        serialize = (fun k -> `String k);
        deserialize = (fun j -> Yojson.Safe.Util.to_string j);
      } in
      Item.silence_age ~persistence:pst ~by:... ~tick:... source
    ]}

    Тип [persist] параметризован типом сериализуемой сущности:
    - [Item.silence_age] — ключ ({!Item} [.silence_age ~persistence:string persist])
    - [Pipe.process_keyed] — состояние (per-user-defined type)
    - [Pipe.window_fold] — accumulator (per-user-defined type)
    - {!Trigger.of_stream} — отдельная, более сложная схема (см. документацию Trigger)
    *)

type 'v persist = {
  backend     : t;
  name        : string;
  serialize   : 'v -> Yojson.Safe.t;
  deserialize : Yojson.Safe.t -> 'v;
}
(** Пакет параметров persistence для одного оператора.

    - [backend] — KV-хранилище
    - [name] — namespace, идентифицирующий конкретный instance оператора
      в этом backend'е (например, ["no_packets"] vs ["gas_silence"])
    - [serialize]/[deserialize] — конвертация значения в JSON и обратно
    *)
