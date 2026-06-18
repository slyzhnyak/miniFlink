(** Операторы конвейера.

    Каждый оператор берёт поток событий ({!Mf_event.t} {!Stream.t}) и
    возвращает поток. Операторы с группировкой ([window], [enrich],
    [dedup], count/session/global окна) получают ключ через первый
    аргумент-модуль {!Keyed.S} — в пользовательском коде нет повторов
    извлечения ключа.

    Watermark и Retract проходят через операторы прозрачно (где это
    осмысленно), сохраняя event-time семантику. *)

(** {2 Базовые операторы (по значению)} *)

(** [map f] применяет [f] к значению каждого [Data]/[Retract]; watermark
    без изменений. *)
val map : ('a -> 'b) -> 'a Mf_event.t Stream.t -> 'b Mf_event.t Stream.t

val event_time : lateness:Time.t -> 'a Mf_event.t Stream.t -> 'a Mf_event.t Stream.t
(** Пометить поток для обработки по event-time с допуском [lateness] на
    опоздание. Доменный псевдоним {!Mf_event.with_watermarks}: окна
    закрываются по времени событий, а не по порядку прихода. *)

(** [filter p] оставляет [Data], удовлетворяющие [p]; watermark и retract
    проходят. *)
val filter : ('a -> bool) -> 'a Mf_event.t Stream.t -> 'a Mf_event.t Stream.t

(** [flat_map f] заменяет каждое значение списком [f v], сохраняя
    event-time и вид события. *)
val flat_map : ('a -> 'b list) -> 'a Mf_event.t Stream.t -> 'b Mf_event.t Stream.t

(** {2 Изоляция исключений из пользовательского кода} *)

(** По умолчанию исключение из пользовательской функции в [map]/[filter]/
    [aggregate]/[window_fold]/... роняет весь пайплайн (обычная семантика
    OCaml). В проде, где битые события неизбежны, изолируй их: [safe_*]
    ловят исключение, зовут [~on_error] и {e пропускают} событие — один
    плохой элемент не валит весь поток (аналог поведения decode→DLQ). *)

(** Как {!map}, но исключение из [f] на событии перехватывается:
    вызывается [on_error exn], событие отбрасывается, поток продолжается. *)
val safe_map :
  on_error:(exn -> unit) -> ('a -> 'b) ->
  'a Mf_event.t Stream.t -> 'b Mf_event.t Stream.t

(** Как {!filter}, но исключение из предиката перехватывается:
    [on_error exn], событие отбрасывается. *)
val safe_filter :
  on_error:(exn -> unit) -> ('a -> bool) ->
  'a Mf_event.t Stream.t -> 'a Mf_event.t Stream.t

val safe_flat_map :
  on_error:(exn -> unit) -> ('a -> 'b list) ->
  'a Mf_event.t Stream.t -> 'b Mf_event.t Stream.t
(** Как [flat_map], но исключение из функции (например в бизнес-правиле)
    перехватывается: [on_error exn], событие даёт пустой результат, поток
    не падает. *)

(** {2 Обогащение} *)

(** [enrich (module K) ~from ~merge upstream] для каждого события ищет в
    таблице [from] значение по ключу [K.key] и сливает его в событие
    через [merge] (left join: [None] если ключа нет). *)
val enrich :
  (module Keyed.S with type t = 'a) ->
  from:(string, 'b) Table.t ->
  merge:('a -> 'b option -> 'a) ->
  'a Mf_event.t Stream.t -> 'a Mf_event.t Stream.t

(** [update_table tbl ~key upstream] обновляет изменяемую таблицу [tbl]
    значениями проходящих событий и пропускает их дальше без изменений.
    Паттерн «поток обновляет справочник»: один поток наполняет [tbl]
    через этот оператор, другой обогащается через
    [enrich ~from:(Table.of_hashtbl tbl)]. *)
val update_table :
  ('k, 'a) Hashtbl.t -> key:('a -> 'k) ->
  'a Mf_event.t Stream.t -> 'a Mf_event.t Stream.t

(** {2 Multi-stream join по ключу}

    Объединение нескольких потоков одного типа в один поток
    «snapshot последних значений по ключу для каждого источника».
    Часто встречающийся паттерн: «когда новое значение по любому
    каналу — пересчитай условие на основе всех текущих значений
    этого ключа». *)

val keyed_join :
  (module Keyed.S with type t = 'a) ->
  'a Mf_event.t Stream.t list ->
  (string * 'a option list) Mf_event.t Stream.t
(** [keyed_join (module K) streams] объединяет [streams] (все одного
    типа ['a]) в один поток, где для каждого нового [Data]-события
    эмитится [(key, options)] — где [options] — список длины
    [List.length streams] с {b последними} значениями по каждому
    входному потоку для этого ключа ([None] если этот поток ещё не
    присылал данных для этого ключа).

    Семантика:
    - Watermarks объединяются через {!Mf_event.union} (min входов).
    - [Retract] в входных потоках игнорируется.
    - Каждое [Data] триггерит emit; стрим эмитит даже когда
      [options] содержит [None]'ы — пользователь сам решает использовать
      или нет (например, проверяет что все [Some] перед обработкой).
    - Список [options] сохраняет порядок входных потоков; пользователь
      сам мапит позиции.

    Типичный use case — multi-sensor pipeline:
    {[
      let voltage = packets |> Pipe.map (fun p -> (p.lamp, p.voltage)) in
      let co      = gas_packets |> Pipe.map (fun g -> (g.lamp, g.co_ppm)) in
      let rssi    = packets |> Pipe.map (fun p -> (p.lamp, p.avg_rssi)) in
      let joined  = Pipe.keyed_join (module By_lamp) [voltage; co; rssi] in
      (* joined: (string * (string * float) option list) Mf_event.t Stream.t *)
    ]}

    Замещает повторяющийся boilerplate: tagged-union типы + union
    + process_keyed со всеми case-analysis для трёх каналов
    превращаются в одну строку. *)

val keyed_join_map :
  (module Keyed.S with type t = 'a) ->
  f:(string -> 'a option list -> 'b option) ->
  'a Mf_event.t Stream.t list ->
  'b Mf_event.t Stream.t
(** Как {!keyed_join}, но применяет [f] к каждому snapshot'у и
    эмитит {b только} те результаты для которых [f] вернул [Some].

    Удобно когда downstream нужно либо unwrap'нутые значения когда
    все [Some], либо ничего:
    {[
      let evacuation_alerts =
        Pipe.keyed_join_map (module By_lamp)
          ~f:(fun lamp opts ->
            match opts with
            | [Some (_, v); Some (_, c); Some (_, r)]
              when v < 3.5 && c > 50.0 && r < -75.0 ->
              Some (lamp, v, c, r)
            | _ -> None)
          [voltage; co; rssi]
    ]}

    Эквивалентно {!keyed_join} с последующим [Pipe.filter_map],
    но обходится одним проходом и без промежуточного [option list]
    в downstream. *)

(** {2 Окна по времени} *)

(** Спецификация временного окна. *)
type win_spec

(** Неперекрывающиеся окна размера [size] (> 0).
    @raise Invalid_argument при [size <= 0]. *)
val tumbling : Time.t -> win_spec

(** Перекрывающиеся окна размера [size] с шагом [step]
    (при [step < size] окна перекрываются).
    @raise Invalid_argument при [size <= 0] или [step <= 0]. *)
val sliding : Time.t -> Time.t -> win_spec

(** Какие окна назначаются событию с временем [ts] по спецификации.
    Низкоуровневая деталь (используется внутри [window]); экспонирована
    для модульного тестирования логики назначения окон. *)
val assign : win_spec -> Time.t -> (Time.t * Time.t) list

(** [window (module K) ?latency ?allowed_lateness spec upstream] группирует
    события по ключу [K.key] и временным окнам [spec]. Окно закрывается
    (эмитит [(key, values)]) когда watermark проходит его конец.
    [?allowed_lateness] продлевает приём опоздавших после закрытия. *)
val window :
  (module Keyed.S with type t = 'a) ->
  ?latency:Time.t ->
  ?allowed_lateness:Time.t ->
  ?on_late:('a -> unit) ->
  win_spec ->
  'a Mf_event.t Stream.t -> (string * 'a list) Mf_event.t Stream.t
(** [?on_late] — side output: событие, пришедшее позже
    [allowed_lateness] (его окно уже удалено по watermark), передаётся
    в этот callback вместо тихой потери. По умолчанию игнорируется. *)

val window_keyed :
  by:('a -> string) ->
  ?latency:Time.t ->
  ?allowed_lateness:Time.t ->
  ?on_late:('a -> unit) ->
  win_spec ->
  'a Mf_event.t Stream.t -> (string * 'a list) Mf_event.t Stream.t
(** Как {!window}, но ключ задаётся функцией [~by] прямо в вызове, без
    объявления модуля {!Keyed.S}. Сахар поверх {!window}. *)

(** [aggregate f] сворачивает каждое окно [(key, values)] в результат
    [f key values]. *)
val aggregate :
  (string -> 'a list -> 'b) ->
  (string * 'a list) Mf_event.t Stream.t -> 'b Mf_event.t Stream.t

(** [window_fold (module K) ?latency ?allowed_lateness spec ~init ~add
    upstream] — окно с {e инкрементальной} агрегацией. Сворачивает каждое
    событие в аккумулятор сразу ([add acc v], старт [init ()]), а не
    копит список и агрегирует в конце. Эмитит [(key, acc)] при закрытии.

    В отличие от [window |> aggregate] (O(n) памяти на окно — весь
    список), хранит только аккумулятор (O(1) по числу событий). Подходит
    для инкрементальных агрегатов (сумма, счёт, max, min, среднее как
    пара сумма/счёт) — меньше памяти и GC-давления на больших окнах.
    [~init] — функция, чтобы у каждого окна был свежий аккумулятор. *)
val window_fold :
  (module Keyed.S with type t = 'a) ->
  ?latency:Time.t ->
  ?allowed_lateness:Time.t ->
  ?backend:Persistence_backend.t ->
  ?backend_name:string ->
  ?serialize_acc:('acc -> Yojson.Safe.t) ->
  ?deserialize_acc:(Yojson.Safe.t -> 'acc) ->
  ?persistence:'acc Persistence_backend.persist ->
  win_spec ->
  init:(unit -> 'acc) ->
  add:('acc -> 'a -> 'acc) ->
  'a Mf_event.t Stream.t -> (string * 'acc) Mf_event.t Stream.t

(** [window_agg (module K) ?latency ?allowed_lateness spec agg upstream] —
    оконная агрегация готовым комбинируемым агрегатором {!Agg.t}.
    Инкрементально (поверх {!window_fold}): O(1) памяти на окно. Эмитит
    [(key, результат)] при закрытии. Удобнее ручного [~init ~add], и
    несколько агрегатов считаются за один проход через [Agg.both]. *)
val window_agg :
  (module Keyed.S with type t = 'a) ->
  ?latency:Time.t ->
  ?allowed_lateness:Time.t ->
  win_spec ->
  ('a, 'r) Agg.t ->
  'a Mf_event.t Stream.t -> (string * 'r) Mf_event.t Stream.t

val window_agg_keyed :
  by:('a -> string) ->
  ?latency:Time.t ->
  ?allowed_lateness:Time.t ->
  win_spec ->
  ('a, 'r) Agg.t ->
  'a Mf_event.t Stream.t -> (string * 'r) Mf_event.t Stream.t
(** Как {!window_agg}, но ключ функцией [~by] инлайн. Сахар поверх
    {!window_agg}. *)

(** Спецификация count-окна. *)
type count_spec

(** Окно каждые [n] событий (> 0). *)
val count_tumbling : int -> count_spec

(** Окно из [n] событий с шагом [step] (оба > 0). *)
val count_sliding : int -> int -> count_spec

(** [count_window (module K) spec upstream] группирует по ключу и числу
    событий, без watermarks. Неполные хвосты не эмитятся.
    @raise Invalid_argument при недопустимых параметрах. *)
val count_window :
  (module Keyed.S with type t = 'a) ->
  count_spec ->
  'a Mf_event.t Stream.t -> (string * 'a list) Mf_event.t Stream.t

(** {2 Session-окна (динамические границы со слиянием)} *)

(** [session_window (module K) ~gap upstream] группирует события в сессии —
    периоды активности, разделённые паузами больше [gap]. Сессии {e сливаются}
    когда событие перекрывает разрыв между ними. Закрытие по watermark
    ([last + gap]) или на конце потока.
    @raise Invalid_argument при [gap <= 0]. *)
val session_window :
  (module Keyed.S with type t = 'a) ->
  gap:Time.t ->
  'a Mf_event.t Stream.t -> (string * 'a list) Mf_event.t Stream.t

(** {2 Global-окно с триггерами} *)

(** Решение триггера. *)
type trigger_action =
  | Continue        (** копить дальше *)
  | Fire            (** эмитить, оставив накопленное (накопительно) *)
  | FireAndPurge    (** эмитить и сбросить буфер *)

(** Триггер: по числу накопленных и последнему значению — решение. *)
type 'a trigger = count:int -> last:'a -> trigger_action

(** Триггер «каждые [n] событий» (с purge). *)
val trigger_count : int -> 'a trigger

(** Триггер по предикату значения (ранняя эмиссия). *)
val trigger_on_value : ('a -> bool) -> 'a trigger

(** [global_window (module K) ~trigger upstream] — одно окно на ключ,
    эмиссия по [trigger]. Отделяет группировку от политики «когда фаерить».

    На конце потока непустой буфер эмитится только если в нём есть
    {e не-эмитированные} данные — поэтому [Fire] (накопительный) без
    новых событий после него {b не} даёт дубля. Если после [Fire]
    пришли ещё события, на конце потока выходит обновлённое (большее)
    состояние — это не дубль, а догнавшие данные. *)
val global_window :
  (module Keyed.S with type t = 'a) ->
  trigger:'a trigger ->
  'a Mf_event.t Stream.t -> (string * 'a list) Mf_event.t Stream.t

(** {2 Состояние и дедупликация} *)

(** [stateful ~init ~f upstream] — оператор с состоянием: [f state event]
    возвращает новое состояние и список выходных событий. Watermark
    проходит прозрачно.

    {b Внимание:} состояние {e одно на весь поток}, НЕ per-key (в отличие
    от Flink keyed state). Если нужно состояние по ключу — держи [Hashtbl]
    по [K.key] внутри замыкания [f] и обновляй нужную ячейку:
    {[
      let table = Hashtbl.create 64 in
      Pipe.stateful ~init:() ~f:(fun () ev ->
        match ev with
        | Mf_event.Data (v, t) ->
          let k = K.key v in
          let s = Option.value ~default:init (Hashtbl.find_opt table k) in
          let s', outs = step s v in
          Hashtbl.replace table k s';
          (), List.map (fun o -> Mf_event.data o t) outs
        | _ -> (), [])
    ]} *)
val stateful :
  init:'s ->
  f:('s -> 'a Mf_event.t -> 's * 'b Mf_event.t list) ->
  'a Mf_event.t Stream.t -> 'b Mf_event.t Stream.t

(** [dedup (module K) ~rule ~cooldown upstream] подавляет повторы с
    одинаковым [(K.key, rule)] в пределах [cooldown]. Состояние ограничено:
    при watermark записи старше [wm - cooldown] удаляются. *)
val dedup :
  (module Keyed.S with type t = 'a) ->
  rule:('a -> string) ->
  cooldown:Time.t ->
  'a Mf_event.t Stream.t -> 'a Mf_event.t Stream.t

(** {2 Терминальные} *)

(** [sink f stream] вызывает [f] на каждом [Data]-значении (ради эффекта),
    проходя поток до конца. *)
val sink : ('a -> unit) -> 'a Mf_event.t Stream.t -> unit

(** Собрать [Data]-значения потока в список. *)
val collect : 'a Mf_event.t Stream.t -> 'a list

(** {3 Удобные потребители ({!Mf_event.t})}

    Шорткаты для типичных операций над потоком событий. Все они дренят
    поток до [None]; различаются тем что делать с каждым {!Mf_event.Data}.
    [Retract]/[Watermark] {b игнорируются} (за исключением [iter_events]).

    Замещают повторяющийся boilerplate вида
    {[
      let rec loop () = match stream () with
        | None -> ()
        | Some (Mf_event.Data (v, _)) -> handle v; loop ()
        | Some _ -> loop ()
      in loop ()
    ]}
    на один вызов [Pipe.iter_data handle stream]. *)

val iter_data : ('a -> unit) -> 'a Mf_event.t Stream.t -> unit
(** Алиас {!sink}. Имя [iter_data] симметрично с другими [_data]-helpers
    ниже — выбор имени дело вкуса. *)

val fold_data :
  init:'b -> f:('b -> 'a -> 'b) -> 'a Mf_event.t Stream.t -> 'b
(** Свернуть [Data]-значения потока. Например, подсчёт суммы:
    {[
      Pipe.fold_data ~init:0.0 ~f:(fun acc v -> acc +. v) stream
    ]} *)

val count_data : 'a Mf_event.t Stream.t -> int
(** Сколько [Data]-событий в потоке. *)

val iter_events : ('a Mf_event.t -> unit) -> 'a Mf_event.t Stream.t -> unit
(** Как {!iter_data}, но callback вызывается на {b каждое} событие —
    включая [Retract] и [Watermark]. Используется в тестах и логировании
    когда важна полная картина потока. *)

val inspect :
  ?label:string ->
  ('a Mf_event.t -> unit) ->
  'a Mf_event.t Stream.t -> 'a Mf_event.t Stream.t
(** [inspect ?label f stream] вызывает [f] на каждое событие
    (включая [Watermark] и [Retract]) и {b пропускает} event дальше
    без изменений. В отличие от {!iter_events}, [inspect] —
    {b не-терминальный}: возвращает stream, который можно
    использовать в дальнейшем pipeline.

    Используется для отладки: log в середине pipeline, метрики,
    counter'ы — без необходимости разбирать pipe-цепочку.

    {[
      source
      |> Pipe.event_time ~lateness:1000
      |> Pipe.inspect ~label:"after_event_time" (fun ev ->
           match ev with
           | Mf_event.Data (v, ts) ->
             Printf.eprintf "[after_event_time] ts=%d\n" ts
           | _ -> ())
      |> Pipe.window (module K) (Pipe.tumbling 10_000)
    ]}

    [label] — опциональный человекочитаемый ID для логов; сама
    функция [inspect] его не использует, но callback может его
    видеть через closure. *)

val materialize :
  by:('a -> Time.t -> 'k) -> 'a Mf_event.t Stream.t -> ('k * 'a) list
(** Свернуть поток с retract'ами в финальную таблицу записей. [by v ts]
    задаёт идентичность записи (например [(ключ, конец_окна)]); Data
    кладёт/заменяет, Retract убирает. Возвращает финальные
    [(идентичность, значение)] после применения всех retract.
    Декларативная замена ручного сбора результатов оконного потока. *)

(** {2 ProcessFunction с таймерами} *)

(** Тип таймера (переэкспорт {!Process_fn.timer_kind}). *)
type timer_kind = Process_fn.timer_kind =
  | Event_time
  | Processing_time

(** Контекст обработчика (переэкспорт {!Process_fn.ctx}). *)
type 'out ctx = 'out Process_fn.ctx = {
  clear_state             : unit -> unit;
  (** удалить состояние текущего ключа (TTL-очистка из on_timer) *)
  emit                    : 'out -> unit;
  (** эмиссия несёт время: из on_timer — время срабатывания таймера, из
      on_event — время события; эмиссии выходят до вызвавшего watermark,
      поэтому выход безопасно композируется с окнами *)
  set_event_timer         : Time.t -> unit;
  (** идентичность таймера — пара (ключ, время); повторный set на то
      же время идемпотентен, cancel снимает всю запись. Полная семантика
      и паттерны на случай двух логических таймеров — в
      {!Process_fn.process_keyed} *)
  set_event_timer_for     : string -> Time.t -> unit;
  (** таймер на УКАЗАННЫЙ ключ — для пере-регистрации из снапшота
      состояния после рестарта (таймеры не переживают перезапуск;
      см. {!Process_fn.process_keyed}) *)
  set_processing_timer    : Time.t -> unit;
  cancel_event_timer      : Time.t -> unit;
  cancel_event_timers     : unit -> unit;
  cancel_processing_timer : Time.t -> unit;
  cancel_processing_timers: unit -> unit;
}

(** Оператор с keyed-состоянием и таймерами. См. {!Process_fn.process_keyed}.
    Для «нет событий дольше порога» (event-time) и «дольше смены»
    (processing-time). *)
val process_keyed :
  (module Keyed.S with type t = 'a) ->
  ?now_ms:(unit -> int) ->
  ?on_stat:(Process_fn.stat -> unit) ->
  ?backend:Persistence_backend.t ->
  ?backend_name:string ->
  ?serialize_state:('st -> Yojson.Safe.t) ->
  ?deserialize_state:(Yojson.Safe.t -> 'st) ->
  ?persistence:'st Persistence_backend.persist ->
  init:(unit -> 'st) ->
  on_event:('out ctx -> string -> 'st -> 'a -> unit) ->
  on_timer:('out ctx -> string -> 'st -> Time.t -> timer_kind -> unit) ->
  'a Mf_event.t Stream.t -> 'out Mf_event.t Stream.t
