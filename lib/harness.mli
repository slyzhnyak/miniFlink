(* ============================================================
   Harness.mli — тестовый фреймворк для pipeline

   Детерминированное тестирование без реального брокера.
   Полный контроль над событиями и watermark-ами.
   ============================================================ *)

(** Контекст теста: входная очередь + pipeline + выходной буфер *)
type ('a, 'b) t

(** Создать контекст теста *)
val create :
  ('a Mf_event.t Stream.t -> 'b Mf_event.t Stream.t) ->
  ('a, 'b) t

(** Подать одно событие *)
val push  : ('a, 'b) t -> 'a -> ts:int -> unit

(** Подать watermark *)
val push_wm : ('a, 'b) t -> ts:int -> unit

(** Подать список событий *)
val push_all : ('a, 'b) t -> ('a * int) list -> unit

(** Закрыть входной поток и вычитать все выходные события *)
val run : ('a, 'b) t -> 'b list

(** Запустить и вернуть только Data значения *)
val run_data : ('a, 'b) t -> 'b list

(** Запустить и вернуть все события включая Watermark/Retract *)
val run_all : ('a, 'b) t -> 'b Mf_event.t list

(** Assertion helpers — бросают Failure с сообщением *)
val expect_count  : ('a, 'b) t -> int -> unit
val expect        : ('a, 'b) t -> ('b -> bool) -> string -> unit
val expect_none   : ('a, 'b) t -> unit

(** Утилиты для построения тестовых событий *)
val ev  : string -> int -> float -> float -> Domain.telemetry
val evs : (string * int * float * float) list -> Domain.telemetry list
