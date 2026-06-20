(** Item helpers: вспомогательные операторы для построения item-потоков
    под триггерную систему.

    Триггер ([Trigger.of_stream]) потребляет поток [('key * 'v)
    Mf_event.t Stream.t] — последовательность обновлений значения по
    ключу. Простые items получаются через [Pipe.map] из исходного
    потока (например, [(p.lamp, p.voltage)]). Этот модуль содержит
    {b непростые} items, которые требуют per-key state или
    self-timers.

    Главный (и пока единственный) такой item — {!silence_age},
    отвечающий на вопрос «сколько времени прошло с последнего
    события для ключа». Без него триггеры «нет пакетов 2 минуты»
    невыразимы — на пустом потоке никаких обновлений не приходит, и
    триггеру не на что реагировать. *)

val silence_age :
  ?persistence:'key Persistence_backend.persist ->
  by:('event -> 'key) ->
  tick:Time.t ->
  'event Mf_event.t Stream.t ->
  ('key * Time.t) Mf_event.t Stream.t
(** [silence_age ~by ~tick source] — эмитит поток обновлений
    «(ключ, время с последнего события)» в миллисекундах.

    Семантика:
    - На каждое [Data ev] ключа [k = by ev] эмитим [(k, 0)] —
      silence для этого ключа обнулился.
    - Каждые [tick] миллисекунд после последнего события ключа [k]
      эмитим [(k, age)], где [age = current_watermark - last_seen[k]].
    - Таймеры срабатывают {b по watermark'у} (не wall-clock),
      обеспечивая детерминизм при event-time-обработке.

    Когда подключать к триггеру: для триггеров вида «нет пакетов
    дольше N» используем [Trigger.greater_than (N * 1000)] с [tick]
    например 30 секунд (компромисс между точностью и накладными:
    больше tick → реже эмиссии, но grub'ее реакция).

    [Retract] в upstream игнорируется. [Watermark] прокидываются
    после обработки накопленных таймеров.

    {b [?backend]} — если передан, silence_age сохраняет и
    восстанавливает per-key state ([last_seen_ts] + pending timer)
    через [Persistence_backend]. На старте читает все ключи с
    префиксом ["item:silence_age:{backend_name}:"] и восстанавливает
    state.

    Требует параметры [?backend_name], [?serialize_key],
    [?deserialize_key]. Если backend подключён, а хотя бы один из
    них отсутствует — [Invalid_argument]. [backend_name] нужен для
    namespace'инга когда на одном backend'е живут несколько
    silence_age-instance'ов (например, отдельные для пакетов и
    газовых пакетов).

    {b [?persistence]} — современная альтернатива четырём параметрам
    выше: единый bundle {!Persistence_backend.persist} с теми же
    четырьмя полями ([backend], [name], [serialize], [deserialize]).
    Эквивалентно по семантике, короче по записи. Нельзя смешивать
    с [?backend] стилем — это [Invalid_argument].

    Без backend'а — поведение как раньше (state в памяти, теряется
    при завершении процесса). *)
