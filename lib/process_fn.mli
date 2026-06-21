(** ProcessFunction с таймерами — keyed-состояние + таймеры (как
    KeyedProcessFunction во Flink). Для логики, которой не хватает окон и
    dedup: «нет событий дольше порога», «дольше смены», CEP-подобные
    переходы. *)

(** Событие телеметрии оператора: подключите [?on_stat] к своим метрикам
    (например счётчики Metrics) — паттерн как [~observe_window_ms] у
    {!Window.window_instrumented}. Соотношение set/fired + Watermark_seen
    сразу показывает «таймеры ставятся, но не срабатывают». *)
type stat =
  [ `Event_timer_set | `Event_timer_fired
  | `Processing_timer_set | `Processing_timer_fired
  | `Watermark_seen ]

(** Тип таймера. *)
type timer_kind =
  | Event_time       (** срабатывает когда watermark >= времени таймера *)
  | Processing_time  (** срабатывает когда wall-clock >= времени таймера *)

(** Контекст обработчика: эмиссия и управление таймерами текущего ключа. *)
type 'out ctx = {
  clear_state             : unit -> unit;
  (** удалить состояние текущего ключа (борьба с ростом state при
      высокой кардинальности ключей; обычно вызывается из on_timer
      по TTL-логике) *)
  emit                    : 'out -> unit;
  set_event_timer         : Time.t -> unit;
  set_event_timer_for     : string -> Time.t -> unit;
  (** поставить event-таймер НА УКАЗАННЫЙ ключ (не текущий) — для
      пере-регистрации таймеров из снапшота состояния после рестарта *)
  set_processing_timer    : Time.t -> unit;
  cancel_event_timer      : Time.t -> unit;
  cancel_event_timers     : unit -> unit;       (** снять все event-таймеры ключа *)
  cancel_processing_timer : Time.t -> unit;
  cancel_processing_timers: unit -> unit;
}

(** [process_keyed (module K) ?now_ms ~init ~on_event ~on_timer] —
    оператор с состоянием на ключ и таймерами.
    - [on_event ctx key state event] — на каждое событие ключа.
    - [on_timer ctx key state time kind] — когда таймер сработал.

    Event-time таймеры срабатывают по watermark, processing-time — по
    wall-clock ([?now_ms] инъектируется для тестов). На конце потока все
    оставшиеся таймеры срабатывают.

    {b Ограничение pull-модели}: таймеры проверяются при прохождении
    событий и watermark через оператор (нет фонового потока).
    Processing-time таймер срабатывает при следующей активности потока
    после истечения. При полностью молчащем потоке используйте
    idle-watermark, чтобы проверки двигались.

    {b Самодиагностика}: если к концу потока остались event-таймеры, а в
    потоке не было {e ни одного} watermark — оператор пишет [Log.warn]
    (почти наверняка забыт [Pipe.event_time]; таймеры молчали бы вечно).

    {b Таймеры НЕ переживают перезапуск процесса.} Они живут в памяти
    оператора; после краха/restart (supervisor, EO-recovery) все таймеры
    пусты — пайплайн поднимется, но «кто молчит» забудется, пока ключ не
    пришлёт новое событие (которого у пропавшего источника не будет!).
    Рабочий паттерн для heartbeat-задач: держите время последнего события
    ключа В СОСТОЯНИИ (оно переживает recovery через checkpoint), и при
    первом же событии/любой активности после старта пере-регистрируйте
    таймер из состояния. Для полного восстановления молчащих ключей —
    прогоните по ключам снапшота состояния (см. test_timers:
    test_reregistration_pattern).

    {b Идентичность таймера = пара (ключ, время).} Семантика как у
    Flink: повторный [set_event_timer t] на тот же ключ и время
    {e идемпотентен} (второй таймер не создаётся), а [cancel_event_timer t]
    отменяет {e всю} запись. Поэтому два логически независимых
    event-таймера на одном ключе, чьи времена могут совпасть, опасны:
    Set сольёт их, cancel снимет оба. Правильные паттерны:
    [a] один таймер + диспетчеризация в [on_timer] по состоянию (см.
        ex07: один таймер на шахтёра, тип алерта определяется по
        last_pkt vs last_rd на момент срабатывания);
    [b] два отдельных оператора [process_keyed] на разделённом через
        {!Pipe.fan_out} потоке — каждый со своей TimerSet, коллизий нет.
    *)
val process_keyed :
  (module Keyed.S with type t = 'a) ->
  ?now_ms:(unit -> int) ->
  ?on_stat:(stat -> unit) ->
  ?name:string ->
  ?on_update:('out ctx -> string -> 'st -> old:'a -> new_value:'a -> unit) ->
  init:(unit -> 'st) ->
  on_event:('out ctx -> string -> 'st -> 'a -> unit) ->
  on_timer:('out ctx -> string -> 'st -> Time.t -> timer_kind -> unit) ->
  'a Mf_event.t Stream.t -> 'out Mf_event.t Stream.t
(** Persistence ОРТОГОНАЛЬНА: задаётся ambient {!Runtime_context}, не
    параметром. В durable-контексте per-key состояние (включая event-
    и processing-таймеры) снапшотится в backend на каждое изменение и
    восстанавливается на старте. [?name] — стабильный namespace
    оператора (по умолчанию ["default"]); нужен если в пайплайне
    несколько process_keyed, чтобы их ключи в backend не коллидировали.

    {b [?on_update]} (Phase 3.3) — опциональный callback для
    атомарной обработки {!Mf_event.Update} событий. Если передан,
    вызывается на каждом Update с обоими значениями [old] и [new_value],
    позволяя выполнить atomic state transition — например, откатить
    эффект старого значения и применить новое.

    Если [?on_update] {b не} передан, Update обрабатывается через
    fallback: вызывается [on_event] с [new_value] (Phase 1
    conservative). Это корректно для FSM-style processors которые
    читают только current value; opt-in [on_update] нужен только
    когда state зависит от {e предыдущего} значения и atomic
    rollback важен.

    {b [?backend]} — если передан, [process_keyed] сохраняет
    per-key state ([st] + event/processing таймеры этого ключа) в
    [Persistence_backend] на каждое изменение (после [on_event] и
    [on_timer]). На старте восстанавливает все state'ы и таймеры
    из backend.

    Требует [?backend_name] (namespace в backend) и
    [?serialize_state]/[?deserialize_state] для пользовательского
    типа [st]. Если backend подключён, а параметры отсутствуют —
    [Invalid_argument].

    {b [?persistence]} — современная альтернатива четырём параметрам
    выше: единый bundle {!Persistence_backend.persist}. Эквивалентно
    по семантике. Нельзя смешивать с [?backend] стилем —
    [Invalid_argument].

    Backend-ключ: ["process_keyed:{backend_name}:{key}"].
    Значение (JSON):
    {[
      {
        "state":     <serialized 'st>,
        "ev_timers": [t1, t2, ...],
        "pt_timers": [t1, t2, ...]
      }
    ]}

    {b Ограничения:} Snapshot пишется {b после каждого} [on_event] и
    [on_timer]. Watermark-based snapshot — TODO.

    Без backend'а — поведение как раньше (state и timers в памяти,
    теряются при завершении процесса). *)

(** {2 Record-based конструктор}

    Альтернатива {!process_keyed} с 10 параметрами. Главная польза —
    {b template pattern:} сохранить базовый spec в переменной и
    override'ить конкретные поля через [with], не дублируя 8 общих
    параметров каждый раз.

    Пример:
    {[
      let base : (Domain.packet, my_state, my_alert) Process_fn.spec = {
        keyed = (module ByLamp);
        init = (fun () -> {...});
        on_event = (fun ctx key st pkt -> ...);
        on_timer = (fun ctx key st t kind -> ...);
        now_ms = None;
        on_stat = None;
        persistence = None;
      }

      let persisted_voltage = Pipe.process_keyed_spec
        { base with persistence = Some voltage_persist }

      let persisted_co = Pipe.process_keyed_spec
        { base with
            persistence = Some co_persist;
            on_event = my_co_handler  (* overrides *) }
    ]} *)

type ('a, 'st, 'out) spec = {
  keyed       : (module Keyed.S with type t = 'a);
  init        : unit -> 'st;
  on_event    : 'out ctx -> string -> 'st -> 'a -> unit;
  on_timer    : 'out ctx -> string -> 'st -> Time.t -> timer_kind -> unit;
  now_ms      : (unit -> int) option;
  on_stat     : (stat -> unit) option;
  name        : string option;
}

val default_spec :
  keyed:(module Keyed.S with type t = 'a) ->
  init:(unit -> 'st) ->
  on_event:('out ctx -> string -> 'st -> 'a -> unit) ->
  on_timer:('out ctx -> string -> 'st -> Time.t -> timer_kind -> unit) ->
  ('a, 'st, 'out) spec
(** Минимальный spec с обязательными полями. Defaults для
    [now_ms], [on_stat], [name] — все [None]. *)

val process_keyed_spec :
  ('a, 'st, 'out) spec ->
  'a Mf_event.t Stream.t -> 'out Mf_event.t Stream.t
(** Эквивалентно {!process_keyed} с соответствующими параметрами. *)
