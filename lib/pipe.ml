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
let filter   = efilt
let flat_map = eflatmap

(* ── Sink helpers ─────────────────────────────────────────── *)

let sink f stream =
  Stream.iter (function Mf_event.Data (v,_) -> f v | _ -> ()) stream

let collect stream =
  List.rev (Stream.fold (fun acc -> function
    | Mf_event.Data (v,_) -> v :: acc | _ -> acc) [] stream)

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
