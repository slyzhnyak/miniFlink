(** Канал между операторами с backpressure.

    Используется для передачи событий от dispatcher к воркерам в
    параллельном режиме. Bounded-канал даёт backpressure автоматически:
    producer блокируется когда канал полон, consumer — когда пуст.

    Две реализации выбираются по версии OCaml (через dune-правило):
    - {b OCaml 4}: Mutex + Condition (Thread);
    - {b OCaml 5}: Atomic lock-free SPSC ring buffer (Domain).

    Sentinel [None] из {!pop} означает что канал закрыт и пуст.

    {b ВАЖНО — контракт SPSC.} На OCaml 5 bounded-канал — это lock-free
    SPSC (Single Producer, Single Consumer) ring buffer: корректен ТОЛЬКО
    при {e ровно одном} producer (push/try_push) и {e ровно одном}
    consumer (pop/try_pop) одновременно. Это наш случай по топологии
    (dispatcher → worker). Использование как MPMC (несколько producer'ов
    или consumer'ов на одном канале) {b не безопасно} — гонки и порча
    данных. Контракт не кодируется типами (ограничение API) и соблюдается
    топологией; модель памяти и обоснование корректности — в
    [variants/channel_v5.ml]. Для нового кода предпочтителен
    {!make_bounded_spsc} — то же что {!make_bounded}, но имя называет
    контракт на месте вызова. *)

(** Непрозрачный тип канала элементов ['a]. *)
type 'a t

(** Создать неограниченный канал (без backpressure). *)
val make_unbounded : unit -> 'a t

(** Создать ограниченный канал заданной ёмкости (с backpressure).
    {b SPSC} — только один producer и один consumer (см. шапку). *)
val make_bounded : int -> 'a t

(** Псевдоним {!make_bounded} с явным именем: подчёркивает SPSC-контракт
    на месте вызова. Предпочтителен в новом коде. *)
val make_bounded_spsc : int -> 'a t

(** Записать значение. Блокирует если канал bounded и полон. *)
val push : 'a t -> 'a -> unit

(** Как {!push}, но возвращает [false] если канал закрыт (например,
    consumer-воркер упал), вместо блокировки навсегда. Предотвращает
    deadlock при падении воркера. *)
val try_push : 'a t -> 'a -> bool

(** Прочитать значение. Блокирует если канал пуст и не закрыт.
    [None] — канал закрыт и пуст. *)
val pop : 'a t -> 'a option

(** Неблокирующее чтение. [None] — нет данных {e сейчас}. *)
val try_pop : 'a t -> 'a option

(** Закрыть канал. После этого {!pop} вернёт оставшиеся элементы,
    затем [None]. *)
val close : 'a t -> unit

(** Текущее число элементов в канале. *)
val length : 'a t -> int

(** Представить канал как pull-поток {!Stream.t}. *)
val to_stream : 'a t -> 'a Stream.t
