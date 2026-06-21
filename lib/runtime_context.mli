(** Ambient-политика persistence для пайплайна.

    Пайплайн НЕ упоминает persistence. Оператор объявляет managed-state
    по имени; решение «durable или нет и как сериализовать» принимается
    здесь, на краю деплоя, один раз. Аналог Flink RuntimeContext.

    Установка через {!with_context} (динамический scope); managed-state
    читает текущий контекст при создании. *)

(** Кодек сериализации managed-state. Работает через [Obj.t], потому
    что реестр кодеков гетерогенен по типу значения; managed-state
    приводит типы обратно при чтении (безопасно: имя состояния
    однозначно задаёт тип). *)
type codec = {
  to_bytes : Obj.t -> bytes;
  of_bytes : bytes -> Obj.t;
}

(** Политика выбора кодека.
    - [Marshal_codec] — авто для любого замкнуто-свободного значения,
      пользователь не пишет сериализаторов. Идеален для crash-recovery,
      не устойчив к смене типа между версиями.
    - [Registry lookup] — кодеки по имени состояния (для schema
      evolution); [None] от lookup означает fallback на Marshal. *)
type codec_policy =
  | Marshal_codec
  | Registry of (string -> codec option)

(** Режим runtime. *)
type mode =
  | Ephemeral
  | Durable of { backend : Persistence_backend.t; codecs : codec_policy }

type t = { mode : mode }

(** Контекст по умолчанию — ephemeral (поведение «без persistence»). *)
val default : t

(** Выполнить [f] с заданным контекстом, восстановив предыдущий после
    (в т.ч. при исключении). Вложенность поддерживается. *)
val with_context : t -> (unit -> 'a) -> 'a

(** Текущий ambient-контекст. *)
val get : unit -> t

(** Ephemeral-контекст (состояние только в памяти). *)
val ephemeral : t

(** Durable-контекст: состояние снапшотится в [backend].
    [?codecs] по умолчанию [Marshal_codec]. *)
val durable : ?codecs:codec_policy -> Persistence_backend.t -> t

(** Marshal-кодек (дефолтная сериализация). *)
val marshal_codec : codec

(** Разрешить кодек для состояния [name] по политике. *)
val codec_for : codec_policy -> string -> codec
