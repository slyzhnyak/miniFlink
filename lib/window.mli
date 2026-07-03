(** Оконные операторы (внутренний модуль; публичный API — через {!Pipe},
    который их переэкспортирует). Здесь скрыты внутренние структуры
    (карта окон, состояния окон/сессий) — наружу только спецификации
    и операторы. *)

(** Спецификация временного окна. *)
type win_spec =
  | Tumbling of Time.t
  | Sliding  of Time.t * Time.t

val tumbling : Time.t -> win_spec
val sliding  : Time.t -> Time.t -> win_spec

(** Какие окна назначаются событию с временем [ts]. Внутренняя деталь,
    экспонирована для модульного тестирования логики назначения. *)
val assign : win_spec -> Time.t -> (Time.t * Time.t) list

val window :
  (module Keyed.S with type t = 'a) ->
  ?latency:Time.t -> ?allowed_lateness:Time.t ->
  ?on_late:('a -> unit) ->
  win_spec ->
  'a Mf_event.t Stream.t -> (string * 'a list) Mf_event.t Stream.t

val window_fold :
  (module Keyed.S with type t = 'a) ->
  ?latency:Time.t -> ?allowed_lateness:Time.t ->
  ?remove:('acc -> 'a -> 'acc) ->
  ?on_error:(exn -> 'a Mf_event.t -> unit) ->
  win_spec ->
  init:(unit -> 'acc) -> add:('acc -> 'a -> 'acc) ->
  'a Mf_event.t Stream.t -> (string * 'acc) Mf_event.t Stream.t
(** Persistence ОРТОГОНАЛЬНА: [window_fold] не принимает persistence-
    параметра. Состояние окон хранится в {!Managed_state}, чьё
    поведение задаёт ambient {!Runtime_context}:
    - вне [Runtime_context.with_context] (или в [ephemeral]) — состояние
      только в памяти;
    - в [durable]-контексте — состояние снапшотится в backend на каждый
      [Watermark] (естественный checkpoint barrier) и восстанавливается
      на старте.

    Один и тот же вызов [window_fold] работает в обоих режимах без
    изменений. Сериализация — по codec-политике контекста (Marshal по
    умолчанию). Namespace в backend: [["window_fold:{spec}:{key}:{start}:{stop}"]]. *)

(** Спецификация count-окна. *)
type count_spec

val count_tumbling : int -> count_spec
val count_sliding  : int -> int -> count_spec

val count_window :
  (module Keyed.S with type t = 'a) ->
  count_spec ->
  'a Mf_event.t Stream.t -> (string * 'a list) Mf_event.t Stream.t

(** Решение триггера global-окна. *)
type trigger_action = Continue | Fire | FireAndPurge

(** Триггер: по числу накопленных и последнему значению — решение. *)
type 'a trigger = count:int -> last:'a -> trigger_action

val trigger_count : int -> 'a trigger
val trigger_on_value : ('a -> bool) -> 'a trigger

val global_window :
  (module Keyed.S with type t = 'a) ->
  trigger:'a trigger ->
  'a Mf_event.t Stream.t -> (string * 'a list) Mf_event.t Stream.t

val session_window :
  (module Keyed.S with type t = 'a) ->
  gap:Time.t ->
  'a Mf_event.t Stream.t -> (string * 'a list) Mf_event.t Stream.t

(** window с гистограммой latency закрытия окна (для runtime-метрик). *)
val window_instrumented :
  (module Keyed.S with type t = 'a) ->
  ?latency:Time.t ->
  observe_window_ms:(float -> unit) ->
  win_spec ->
  'a Mf_event.t Stream.t -> (string * 'a list) Mf_event.t Stream.t
