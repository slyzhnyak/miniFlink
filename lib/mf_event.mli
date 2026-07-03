(** События потока: данные, watermark, retract, update.

    Каждое значение несёт свой event-time. Watermark — граница, после
    которой события раньше неё уже не придут. Retract отменяет ранее
    выданный результат (для поздних данных). Update — атомарная
    коррекция: одно событие со старым и новым значением, обрабатывается
    downstream без промежуточного состояния. *)

(** Событие с полезной нагрузкой ['a]. Конструкторы открыты — операторы
    сопоставляются с ними напрямую.

    {b Update vs Retract+Data:} логически [Update {old; new_value; ts}]
    эквивалентен паре [Retract old; Data new_value], но downstream
    видит Update как {b одно} событие — нет промежуточного состояния
    "после retract, до new data". Это критично для snapshot-based
    операторов (например, {!Pipe.keyed_join}) где промежуточный
    [None] в слоте вызывал бы ложные срабатывания. *)
type 'a t =
  | Data      of 'a * Time.t  (** значение + его event-time *)
  | Watermark of Time.t       (** граница: события раньше уже пришли *)
  | Retract   of 'a * Time.t  (** отмена ранее выданного результата *)
  | Update    of { old : 'a; new_value : 'a; ts : Time.t }
    (** атомарная коррекция: [old] заменяется на [new_value] *)

(** [data v ts] — событие-данные со значением [v] на event-time [ts]. *)
val data : 'a -> Time.t -> 'a t

(** [retract v ts] — отмена ранее выданного [v]. *)
val retract : 'a -> Time.t -> 'a t

(** [update old new_v ts] — атомарная замена [old] на [new_v]. *)
val update : 'a -> 'a -> Time.t -> 'a t

(** [wm ts] — watermark на момент [ts]. *)
val wm : Time.t -> 'a t

(** "Текущее" значение события или [None] для watermark.
    Для [Data]/[Retract] — само значение. Для [Update] — [new_value]
    (старое уже заменено). Если нужно [old] из [Update] — match
    explicitly. *)
val value : 'a t -> 'a option

(** Event-time события (для любого вида). *)
val ts : 'a t -> Time.t

(** Является ли событие [Data]. *)
val is_data : 'a t -> bool

(** Применить функцию к значению, сохранив вид и время; watermark — без
    изменений. Для [Update] применяется к обоим — [old] и [new_value]. *)
val map_value : ('a -> 'b) -> 'a t -> 'b t

(** Как {!map_value}, но функция видит и timestamp события. Вид события
    (Data/Retract/Update/Watermark) сохраняется; watermark — без
    изменений; для [Update] оба значения ([old] и [new_value])
    маппятся с общим временем события. Нужна, когда конструируемое
    значение зависит от времени (например конец окна как поле записи). *)
val map_ts : ('a -> Time.t -> 'b) -> 'a t -> 'b t

(** [with_watermarks_ext ~latency ?interval src] вставляет watermarks
    [max_seen - latency] в поток.
    - [~latency] — допуск на опоздание (насколько событие может прийти
      позже своего времени).
    - [~interval] — минимальный сдвиг watermark перед новой эмиссией
      (по event-time); [0] = на каждый новый максимум, больше = реже.
    На конце потока эмитит финальный watermark, закрывающий все окна. *)
val with_watermarks_ext :
  latency:Time.t -> ?interval:Time.t -> 'a t Stream.t -> 'a t Stream.t

(** [with_watermarks ~latency src] — {!with_watermarks_ext} с
    [~interval:0] (watermark на каждый новый максимум). *)
val with_watermarks : latency:Time.t -> 'a t Stream.t -> 'a t Stream.t

(** [of_list ~ts xs] — поток [Data]-событий из списка значений, event-time
    каждого берётся из [ts v]. Убирает повторяющийся шаблон
    [Stream.of_list (List.map (fun v -> data v (ts v)) xs)]. *)
val of_list : ts:('a -> Time.t) -> 'a list -> 'a t Stream.t

(** Idle watermark: продвигает watermark по {e wall-clock} когда источник
    замолчал, чтобы окна не висели в тишине.

    Источник {e неблокирующий}: [poll : unit -> 'a t option option] —
    [None] (поток окончен), [Some None] (данных пока нет), [Some (Some ev)]
    (событие). При тишине дольше [~idle_ms] эмитит watermark, продвинутый
    на [~idle_advance]. [~now_ms]/[~sleep_ms] вынесены параметрами для
    тестируемости (управляемые часы вместо реальных). Таймер живёт здесь,
    в слое источника — чистое event-time ядро не затрагивается. *)
val with_idle_watermarks :
  latency:Time.t -> idle_ms:int -> idle_advance:Time.t ->
  now_ms:(unit -> int) -> sleep_ms:(int -> unit) ->
  (unit -> 'a t option option) -> 'a t Stream.t

(** [union a b] сливает два потока событий, упорядочивая по event-time.
    Оба входа должны быть упорядочены по времени (как после источника
    с watermarks). Watermark объединённого потока = {e минимум} watermarks
    входов: время T считается пройденным только когда оба входа это
    подтвердили — это сохраняет корректность окон ниже по течению. *)
val union : 'a t Stream.t -> 'a t Stream.t -> 'a t Stream.t
(** Слить два потока в один, чередуя события по мере поступления.

    {b Watermark = минимум по входам.} Пока оба входа живы, union
    эмитит [Watermark (min wm_a wm_b)]: общая граница не может уйти
    вперёд входа, который ещё может прислать более раннее событие. Когда
    один вход исчерпан (вернул [None]), union переходит на watermark
    оставшегося.

    {b ⚠ Молчащий вход (E-2).} Min-семантика означает: если один вход
    НЕ исчерпан, но {e молчит} (не шлёт ни данные, ни watermark —
    например датчик офлайн, но соединение живо), общий watermark
    застревает на его последнем значении. Downstream-окна и таймеры,
    завязанные на event-time, перестают закрываться, хотя второй вход
    активен. Это не баг union, а следствие корректной min-границы: union
    не может знать, что молчащий вход не пришлёт раньнее событие.

    Средства против застревания: (1) idle-watermark у молчащих
    источников (периодический watermark по wall-clock, даже без данных);
    (2) на стороне источника — таймаут, помечающий вход исчерпанным.
    Планируется удобный [union ~idle_timeout] (см. dsl-roadmap P2.5),
    пока — idle-watermark в самом источнике. *)

(** Форматировать время как [«1.234s»] (для отладочного вывода). *)
val pp_ts : Time.t -> string
