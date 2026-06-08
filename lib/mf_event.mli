(** События потока: данные, watermark, retract.

    Каждое значение несёт свой event-time. Watermark — граница, после
    которой события раньше неё уже не придут (позволяет закрывать окна
    детерминированно). Retract отменяет ранее выданный результат
    (для поздних данных). *)

(** Событие с полезной нагрузкой ['a]. Конструкторы открыты — операторы
    сопоставляются с ними напрямую. *)
type 'a t =
  | Data      of 'a * Time.t  (** значение + его event-time *)
  | Watermark of Time.t       (** граница: события раньше уже пришли *)
  | Retract   of 'a * Time.t  (** отмена ранее выданного результата *)

(** [data v ts] — событие-данные со значением [v] на event-time [ts]. *)
val data : 'a -> Time.t -> 'a t

(** [retract v ts] — отмена ранее выданного [v]. *)
val retract : 'a -> Time.t -> 'a t

(** [wm ts] — watermark на момент [ts]. *)
val wm : Time.t -> 'a t

(** Значение события ([Data]/[Retract]) или [None] для watermark. *)
val value : 'a t -> 'a option

(** Event-time события (для любого вида). *)
val ts : 'a t -> Time.t

(** Является ли событие [Data]. *)
val is_data : 'a t -> bool

(** Применить функцию к значению, сохранив вид и время; watermark — без
    изменений. *)
val map_value : ('a -> 'b) -> 'a t -> 'b t

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

(** Форматировать время как [«1.234s»] (для отладочного вывода). *)
val pp_ts : Time.t -> string
