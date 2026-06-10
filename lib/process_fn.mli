(** ProcessFunction с таймерами — keyed-состояние + таймеры (как
    KeyedProcessFunction во Flink). Для логики, которой не хватает окон и
    dedup: «нет событий дольше порога», «дольше смены», CEP-подобные
    переходы. *)

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
    idle-watermark, чтобы проверки двигались. *)
val process_keyed :
  (module Keyed.S with type t = 'a) ->
  ?now_ms:(unit -> int) ->
  init:(unit -> 'st) ->
  on_event:('out ctx -> string -> 'st -> 'a -> unit) ->
  on_timer:('out ctx -> string -> 'st -> Time.t -> timer_kind -> unit) ->
  'a Mf_event.t Stream.t -> 'out Mf_event.t Stream.t
