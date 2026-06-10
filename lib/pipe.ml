(** Операторы конвейера: enrich, window, aggregate, dedup, flat_map.

    Ключевая особенность: [window], [enrich], [dedup] получают ключ
    группировки из модуля {!Keyed.S}, переданного первым аргументом —
    в пользовательском коде нет повторяющихся [~key:] параметров.

    [window] использует locally abstract type [(type a)], что
    устраняет рекурсивный тип [α = string × α list] без [Obj.magic]. *)

(* ── Lift: применить функцию к значению события ─────────── *)

let emap  f = Stream.map   (Mf_event.map_value f)
let efilt p = Stream.filter (function
  | Mf_event.Data (v,_)  -> p v
  | _                 -> true)
let eflatmap f = Stream.flat_map (function
  | Mf_event.Watermark _ as w -> [w]
  | Mf_event.Retract (v,t) -> List.map (fun w -> Mf_event.retract w t) (f v)
  | Mf_event.Data    (v,t) -> List.map (fun w -> Mf_event.data    w t) (f v))

(* ── Enrich ───────────────────────────────────────────────── *)

(* enrich ~from:table обогащает каждое событие данными из таблицы.
   Тип KEYED говорит как достать ключ. *)
(** [enrich (module K) ~from ~merge upstream] обогащает каждое событие
    данными из справочной таблицы [from] по ключу [K.key]. Left join:
    если ключа нет в таблице, [merge] получает [None] и событие проходит
    необогащённым — конвейер не падает. *)
let enrich
    (type a)
    (module K : Keyed.S with type t = a)
    ~(from  : (string, 'b) Table.t)
    ~(merge : a -> 'b option -> a)
    (upstream : a Mf_event.t Stream.t) : a Mf_event.t Stream.t =
  emap (fun v -> merge v (from (K.key v))) upstream

(* ── Обновление таблицы из потока ─────────────────────────── *)

(** [update_table tbl ~key upstream] обновляет изменяемую таблицу [tbl]
    значениями проходящих [Data]-событий (ключ через [~key]), пропуская
    события дальше без изменений. Паттерн «поток обновляет справочник»:
    один поток через [update_table] наполняет [tbl], другой через
    [enrich ~from:(Table.of_hashtbl tbl)] из него обогащается.

    Side-effect на проходе: к моменту когда обогащаемый поток ищет ключ,
    в [tbl] уже то что прошло через этот оператор. Watermark/Retract
    проходят прозрачно, таблицу не трогают. *)
let update_table (tbl : ('k, 'a) Hashtbl.t) ~(key : 'a -> 'k)
    (upstream : 'a Mf_event.t Stream.t) : 'a Mf_event.t Stream.t =
  Stream.map (fun ev ->
    (match ev with
     | Mf_event.Data (v, _) -> Hashtbl.replace tbl (key v) v
     | _ -> ());
    ev) upstream

(* ── Окна вынесены в Window; переэкспорт для стабильного Pipe.* API ── *)
type win_spec = Window.win_spec
let tumbling = Window.tumbling
let sliding  = Window.sliding
let assign   = Window.assign
let window   = Window.window
let window_fold = Window.window_fold
type count_spec = Window.count_spec
let count_tumbling = Window.count_tumbling
let count_sliding  = Window.count_sliding
let count_window   = Window.count_window
type trigger_action = Window.trigger_action =
  | Continue | Fire | FireAndPurge
type 'a trigger = 'a Window.trigger
let trigger_count    = Window.trigger_count
let trigger_on_value = Window.trigger_on_value
let global_window    = Window.global_window
let session_window   = Window.session_window

(* ── Aggregate ────────────────────────────────────────────── *)

(* aggregate принимает (key * 'a list) — выход window.
   f : string -> 'a list -> 'b *)
(** [aggregate f] сворачивает события каждого окна [(key, vs)] в один
    результат [f key vs] (например max-скорость, min-топливо). *)
let aggregate f = emap (fun (key, vs) -> f key vs)

(** [window_agg (module K) ?latency ?allowed_lateness spec agg upstream] —
    оконная агрегация готовым комбинируемым агрегатором {!Agg.t}.
    Инкрементально (через [window_fold]): O(1) памяти на окно. Эмитит
    [(key, результат_агрегата)] при закрытии окна.

    {[
      ... |> Pipe.window_agg (module Sensor) (Pipe.tumbling (seconds 10))
                Agg.(both (mean temp) (max_by temp))
    ]} *)
let window_agg
    (type a) (type r)
    (module K : Keyed.S with type t = a)
    ?latency ?allowed_lateness
    (spec : Window.win_spec)
    (agg : (a, r) Agg.t)
    (upstream : a Mf_event.t Stream.t)
    : (string * r) Mf_event.t Stream.t =
  Agg.with_parts agg { Agg.k = fun init add finish ->
    window_fold (module K) ?latency ?allowed_lateness spec ~init ~add upstream
    |> emap (fun (key, acc) -> (key, finish acc)) }

(* ── Stateful ─────────────────────────────────────────────── *)

let stateful ~init ~f upstream =
  let st  = ref init in
  let buf = Queue.create () in
  fun () ->
    let rec pull () =
      if not (Queue.is_empty buf) then Some (Queue.pop buf) else
      match upstream () with
      | None -> None
      | Some (Mf_event.Watermark _ as w) -> Some w
      | Some ev ->
        let (s, outs) = f !st ev in
        st := s; List.iter (fun o -> Queue.push o buf) outs; pull ()
    in pull ()

(* ── Dedup ────────────────────────────────────────────────── *)

(** [dedup (module K) ~rule ~cooldown upstream] подавляет повторные
    события с одинаковым [(K.key, rule)] в пределах окна [cooldown].

    Состояние ограничено: при каждом watermark записи старше
    [wm - cooldown] удаляются — они уже не могут подавить будущие
    события, поэтому удаление безопасно. *)
let dedup
    (type a)
    (module K : Keyed.S with type t = a)
    ~(rule     : a -> string)
    ~(cooldown : Time.t)
    (upstream  : a Mf_event.t Stream.t) : a Mf_event.t Stream.t =
  let seen = Hashtbl.create 64 in
  let evict_before wm =
    (* Собираем устаревшие ключи, затем удаляем (нельзя мутировать во время iter) *)
    let stale = Hashtbl.fold (fun k last acc ->
      if last < wm - cooldown then k :: acc else acc) seen [] in
    List.iter (Hashtbl.remove seen) stale
  in
  Stream.filter (function
    | Mf_event.Watermark wm -> evict_before wm; true
    | Mf_event.Retract _ -> true
    | Mf_event.Data (v, t) ->
      let k = K.key v ^ ":" ^ rule v in
      match Hashtbl.find_opt seen k with
      | Some last when t - last <= cooldown -> false
      | _ -> Hashtbl.replace seen k t; true
  ) upstream

(* ── Map / Filter / FlatMap ───────────────────────────────── *)

let map      = emap

(* event_time: пометить поток для обработки по event-time с допуском
   [lateness] на опоздание (псевдоним Mf_event.with_watermarks с
   доменным именем — читается как «обрабатываем по времени событий»). *)
let event_time ~lateness stream = Mf_event.with_watermarks ~latency:lateness stream
let filter   = efilt
let flat_map = eflatmap

(* ── Изоляция исключений из пользовательского кода ────────── *)

(* По умолчанию исключение из пользовательской функции в map/filter/etc.
   роняет весь пайплайн (как и в любом OCaml-коде). Для прода, где битые
   события неизбежны, safe_* ловят исключение, зовут ~on_error и
   ПРОПУСКАЮТ событие — один плохой элемент не валит весь поток.
   Это аналог safe_decode для произвольных пользовательских функций. *)

(** [safe_map ~on_error f] как [map f], но исключение из [f] на событии
    перехватывается: вызывается [on_error exn] и событие отбрасывается
    (поток продолжается). Watermark/Retract проходят как обычно. *)
let safe_map ~on_error f upstream =
  Stream.flat_map (function
    | Mf_event.Data (v, t) ->
      (try [Mf_event.data (f v) t]
       with e -> on_error e; [])
    | Mf_event.Watermark wm -> [Mf_event.wm wm]
    | Mf_event.Retract (_, _) -> [])   (* retract входного типа не транслируется *)
    upstream

(** [safe_filter ~on_error p] как [filter p], но исключение из [p]
    перехватывается: [on_error exn], событие отбрасывается. *)
let safe_filter ~on_error p upstream =
  Stream.flat_map (function
    | Mf_event.Data (v, _) as ev ->
      (try if p v then [ev] else []
       with e -> on_error e; [])
    | Mf_event.Watermark _ | Mf_event.Retract _ as ev -> [ev])
    upstream

(** [safe_flat_map ~on_error f] как [flat_map f], но исключение из [f]
    (например в бизнес-правиле) перехватывается: [on_error exn], событие
    даёт пустой результат, поток продолжается. *)
let safe_flat_map ~on_error f upstream =
  Stream.flat_map (function
    | Mf_event.Data (v, t) ->
      (try List.map (fun out -> Mf_event.data out t) (f v)
       with e -> on_error e; [])
    | Mf_event.Watermark wm -> [Mf_event.wm wm]
    | Mf_event.Retract (_, _) -> [])
    upstream

(* ── Sink helpers ─────────────────────────────────────────── *)

let sink f stream =
  Stream.iter (function Mf_event.Data (v,_) -> f v | _ -> ()) stream

let collect stream =
  List.rev (Stream.fold (fun acc -> function
    | Mf_event.Data (v,_) -> v :: acc | _ -> acc) [] stream)

(* materialize: свернуть поток с retract'ами в финальную таблицу записей.
   [by v ts] задаёт идентичность записи (например (ключ, конец окна));
   Data кладёт/заменяет запись, Retract убирает. Возвращает финальные
   (идентичность, значение) после применения всех retract. Декларативная
   замена ручного Hashtbl-цикла. *)
let materialize ~(by : 'a -> Time.t -> 'k) (stream : 'a Mf_event.t Stream.t)
    : ('k * 'a) list =
  let tbl : ('k, 'a) Hashtbl.t = Hashtbl.create 64 in
  Stream.fold (fun () -> function
    | Mf_event.Data (v, ts)    -> Hashtbl.replace tbl (by v ts) v
    | Mf_event.Retract (v, ts) -> Hashtbl.remove tbl (by v ts)
    | _ -> ()) () stream;
  Hashtbl.fold (fun k v acc -> (k, v) :: acc) tbl []

(* ── Shorthand: seconds / minutes в операторах ───────────── *)

(* ── Instrumented operators ──────────────────────────────── *)
(* Версии операторов с метриками — используются в runtime *)

(** Обернуть stream: вызывать f() на каждом Data событии *)
let with_counter f upstream =
  fun () ->
    match upstream () with
    | Some (Mf_event.Data _ as ev) -> f (); Some ev
    | other -> other


(** window с histogram для latency закрытия окна (переэкспорт из Window) *)
let window_instrumented = Window.window_instrumented
