(* ============================================================
   Harness.mli — тестовый фреймворк для pipeline

   Позволяет:
   - подавать события вручную с контролем watermark
   - проверять выходные события без реального брокера
   - тестировать late data и retraction сценарии
   - измерять покрытие правил
   ============================================================ *)

(** Контекст теста *)
type 'a ctx

(** Создать контекст с заданным pipeline *)
val create : ('a Mf_event.t Stream.t -> 'b Mf_event.t Stream.t) -> 'a ctx

(** Подать событие *)
val push_event : 'a ctx -> 'a -> ts:int -> unit

(** Подать watermark *)
val push_wm : 'a ctx -> ts:int -> unit

(** Закрыть входной поток *)
val close : 'a ctx -> unit

(** Собрать все выходные Data события *)
val collect : 'a ctx -> 'b list

(** Assert: ожидаемое число выходных событий *)
val assert_count : 'a ctx -> int -> unit

(** Assert: предикат для каждого выходного события *)
val assert_all : 'a ctx -> ('b -> bool) -> string -> unit

(** Запустить и вернуть результат *)
val run : 'a ctx -> 'b list
