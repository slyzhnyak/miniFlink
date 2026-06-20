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
  ?backend:Persistence_backend.t ->
  ?backend_name:string ->
  ?serialize_acc:('acc -> Yojson.Safe.t) ->
  ?deserialize_acc:(Yojson.Safe.t -> 'acc) ->
  ?persistence:'acc Persistence_backend.persist ->
  ?remove:('acc -> 'a -> 'acc) ->
  win_spec ->
  init:(unit -> 'acc) -> add:('acc -> 'a -> 'acc) ->
  'a Mf_event.t Stream.t -> (string * 'acc) Mf_event.t Stream.t
(** {b [?backend]} — если передан, [window_fold] сохраняет per-window
    state (аккумулятор + флаг nonempty + FOpen/FFired) в
    [Persistence_backend] на каждый [Watermark] (т.н. checkpoint
    barrier). На старте восстанавливает все окна из backend.

    Требует [?backend_name] + [?serialize_acc]/[?deserialize_acc] для
    пользовательского типа аккумулятора. Если backend подключён, а
    параметры отсутствуют — [Invalid_argument].

    Backend-ключ:
    [["window_fold:{backend_name}:{user_key}:{start}:{stop}"]].

    Значение (JSON):
    {[
      {
        "state":    "open" | "fired",
        "acc":      <serialized 'acc>,
        "nonempty": bool
      }
    ]}

    Snapshot пишется {b на каждом watermark}, не на каждом event —
    это естественный checkpoint barrier, который согласует все
    окна одного watermark'а consistent. Это отличается от Trigger
    (snapshot на transitions), silence_age (на каждый event +
    tick), process_keyed (на каждый on_event/on_timer). *)

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
