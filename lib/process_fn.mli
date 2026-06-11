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
    test_reregistration_pattern). *)
val process_keyed :
  (module Keyed.S with type t = 'a) ->
  ?now_ms:(unit -> int) ->
  ?on_stat:(stat -> unit) ->
  init:(unit -> 'st) ->
  on_event:('out ctx -> string -> 'st -> 'a -> unit) ->
  on_timer:('out ctx -> string -> 'st -> Time.t -> timer_kind -> unit) ->
  'a Mf_event.t Stream.t -> 'out Mf_event.t Stream.t
