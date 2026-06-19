(** Операторы конвейера: enrich, window, aggregate, dedup, flat_map.

    Ключевая особенность: [window], [enrich], [dedup] получают ключ
    группировки из модуля {!Keyed.S}, переданного первым аргументом —
    в пользовательском коде нет повторяющихся [~key:] параметров.

    [window] использует locally abstract type [(type a)], что
    устраняет рекурсивный тип [α = string × α list] без [Obj.magic]. *)

(* ── Lift: применить функцию к значению события ─────────── *)

let emap  f = Stream.map   (Mf_event.map_value f)
let efilt p = Stream.filter (function
  | Mf_event.Data (v, _) -> p v
  | Mf_event.Update { new_value = v; _ } -> p v
  | Mf_event.Watermark _ | Mf_event.Retract _ -> true)
let eflatmap f = Stream.flat_map (function
  | Mf_event.Watermark _ as w -> [w]
  | Mf_event.Retract (v, t) -> List.map (fun w -> Mf_event.retract w t) (f v)
  | Mf_event.Data    (v, t) -> List.map (fun w -> Mf_event.data    w t) (f v)
  | Mf_event.Update { old; new_value; ts } ->
    (* flat_map переключает 1 событие на N. Для Update это значит
       N синхронизированных Update'ов: каждое значение из f(old)
       спарено с соответствующим из f(new_value). Если списки разной
       длины — только общая часть. *)
    let olds = f old in
    let news = f new_value in
    let rec zip xs ys = match xs, ys with
      | x :: xt, y :: yt -> Mf_event.update x y ts :: zip xt yt
      | _ -> []
    in zip olds news)

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
     | Mf_event.Update { new_value = v; _ } ->
       (* Update заменяет old → new в таблице. Если key функция
          даёт разные ключи для old и new — будет inconsistency,
          но редкий случай. *)
       Hashtbl.replace tbl (key v) v
     | Mf_event.Watermark _ | Mf_event.Retract _ -> ());
    ev) upstream

(* ────────────────────────────────────────────────────────────────────
   keyed_join: multi-stream join по ключу с emit'ом snapshot'а
   последних значений каждого источника.

   Реализация: каждый входной stream тегаем индексом, объединяем через
   Mf_event.union, состояние хранит `'a option array`, обновляем по
   индексу при каждом Data, эмитим Array.to_list. process_keyed нам
   не нужен — простой fold через Stream.scan_state нет; делаем
   вручную state-машину на closure'ах. *)
let keyed_join
    (type a)
    (module K : Keyed.S with type t = a)
    (streams : a Mf_event.t Stream.t list)
    : (string * a option list) Mf_event.t Stream.t =
  let n = List.length streams in
  if n = 0 then Stream.empty
  else begin
    (* Тегаем каждый stream его индексом 0..n-1. Index несём через
       map_value: значение становится (index, original_value). *)
    let tagged = List.mapi (fun i s ->
      Stream.map (Mf_event.map_value (fun v -> (i, v))) s
    ) streams in
    (* Объединяем все через попарный Mf_event.union (fold справа). *)
    let unioned = match tagged with
      | [] -> assert false
      | [s] -> s
      | first :: rest ->
        List.fold_left Mf_event.union first rest
    in
    (* Состояние: per-key array длиной n с последними значениями. *)
    let states : (string, a option array) Hashtbl.t = Hashtbl.create 64 in
    let get_or_init k =
      match Hashtbl.find_opt states k with
      | Some arr -> arr
      | None ->
        let arr = Array.make n None in
        Hashtbl.replace states k arr;
        arr
    in
    let out_q : (string * a option list) Mf_event.t Queue.t = Queue.create () in
    let rec next () =
      if not (Queue.is_empty out_q) then Some (Queue.pop out_q)
      else match unioned () with
        | None -> None
        | Some (Mf_event.Data ((idx, v), ts)) ->
          let key = K.key v in
          let arr = get_or_init key in
          arr.(idx) <- Some v;
          Queue.push (Mf_event.data (key, Array.to_list arr) ts) out_q;
          next ()
        | Some (Mf_event.Watermark wm) ->
          Some (Mf_event.wm wm)
        | Some (Mf_event.Retract ((idx, v), ts)) ->
          (* Retract обнуляет slot [idx] для ключа [K.key v] ТОЛЬКО
             если текущее значение в слоте эквивалентно retracted
             (OCaml structural равенство [=]). Это правильное
             поведение для случая когда retract отзывает последний
             Data; если после Data(v=10) пришёл Data(v=20), а потом
             Retract(v=10) — slot уже содержит v=20, retract
             игнорируется как stale (более новое значение
             перезаписало старое до retract'а).

             Эмитим новый snapshot после retract'а — downstream
             узнаёт об изменении. *)
          let key = K.key v in
          (match Hashtbl.find_opt states key with
           | Some arr when arr.(idx) = Some v ->
             arr.(idx) <- None;
             Queue.push
               (Mf_event.data (key, Array.to_list arr) ts) out_q
           | _ ->
             (* slot уже None или содержит другое значение — stale
                retract, игнорируем *)
             ());
          next ()
        | Some (Mf_event.Update { old = (old_idx, old_v);
                                   new_value = (new_idx, new_v); ts }) ->
          (* АТОМАРНАЯ коррекция: ОДИН snapshot эмитится с применённым
             изменением. Это ключевое преимущество Update vs пары
             Retract+Data — нет промежуточного None flicker'а.

             Семантика: slot[old_idx] был Some old_v, теперь
             slot[new_idx] = Some new_v. Если old_idx = new_idx
             (тот же канал обновился) — простая замена. Если разные —
             значение "перешло" между каналами (редкий случай, но
             корректно).

             Stale Update'ы (когда slot[old_idx] не равен Some old_v)
             trate'ятся как простое применение new_value, как если
             бы пришёл Data. *)
          let old_key = K.key old_v in
          let new_key = K.key new_v in
          if old_key = new_key then begin
            let arr = get_or_init new_key in
            (match arr.(old_idx) with
             | Some v when v = old_v ->
               (* old match'нулся; применяем атомарно — один snapshot *)
               if old_idx <> new_idx then arr.(old_idx) <- None;
               arr.(new_idx) <- Some new_v;
               Queue.push (Mf_event.data
                            (new_key, Array.to_list arr) ts) out_q
             | _ ->
               (* stale — old уже не там; применяем только new *)
               arr.(new_idx) <- Some new_v;
               Queue.push (Mf_event.data
                            (new_key, Array.to_list arr) ts) out_q)
          end else begin
            (* Update переносит значение между разными ключами —
               редкий cross-key случай. Декомпозируем на clear
               на старом ключе + set на новом. Два snapshot'а,
               потому что они касаются разных entries в state. *)
            (match Hashtbl.find_opt states old_key with
             | Some old_arr when old_arr.(old_idx) = Some old_v ->
               old_arr.(old_idx) <- None;
               Queue.push (Mf_event.data
                            (old_key, Array.to_list old_arr) ts) out_q
             | _ -> ());
            let new_arr = get_or_init new_key in
            new_arr.(new_idx) <- Some new_v;
            Queue.push (Mf_event.data
                         (new_key, Array.to_list new_arr) ts) out_q
          end;
          next ()
    in next
  end

(* keyed_join_map: helper над keyed_join. Применяет f к каждому
   snapshot'у, эмитит результат только если f вернула Some. *)
let keyed_join_map
    (type a) (type b)
    (module K : Keyed.S with type t = a)
    ~(f : string -> a option list -> b option)
    (streams : a Mf_event.t Stream.t list)
    : b Mf_event.t Stream.t =
  let joined = keyed_join (module K) streams in
  let rec next () = match joined () with
    | None -> None
    | Some (Mf_event.Data ((key, opts), ts)) ->
      (match f key opts with
       | Some v -> Some (Mf_event.data v ts)
       | None -> next ())
    | Some (Mf_event.Watermark wm) -> Some (Mf_event.wm wm)
    | Some (Mf_event.Retract _) | Some (Mf_event.Update _) ->
      (* keyed_join по контракту эмитит только Data snapshot'ы для
         коррекций (см. native Update handling выше). Эти ветки
         defensive — skip. *)
      next ()
  in next

(* ── Окна вынесены в Window; переэкспорт для стабильного Pipe.* API ── *)
type win_spec = Window.win_spec
let tumbling = Window.tumbling
let sliding  = Window.sliding
let assign   = Window.assign
let window   = Window.window

(* window_keyed ~by — как window, но ключ функцией инлайн (без module).
   Сахар поверх window через Keyed.of_fun; ядро не тронуто. *)
let window_keyed ~by ?latency ?allowed_lateness ?on_late spec stream =
  window (Keyed.of_fun by) ?latency ?allowed_lateness ?on_late spec stream
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

(* window_agg_keyed ~by — window_agg с ключом-функцией инлайн. *)
let window_agg_keyed ~by ?latency ?allowed_lateness spec agg stream =
  window_agg (Keyed.of_fun by) ?latency ?allowed_lateness spec agg stream

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
      (match Hashtbl.find_opt seen k with
       | Some last when t - last <= cooldown -> false
       | _ -> Hashtbl.replace seen k t; true)
    | Mf_event.Update { new_value = v; ts = t; _ } ->
      (* Update — это коррекция, обычно она НЕ должна быть отброшена
         dedup'ом, потому что несёт обновление существующего значения.
         Пропускаем, но обновляем last_seen чтобы последующие Data на
         том же ключе видели cooldown относительно Update. *)
      let k = K.key v ^ ":" ^ rule v in
      Hashtbl.replace seen k t;
      true
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
    | Mf_event.Update { old; new_value; ts } ->
      (* Применяем f к обоим; если хоть один взорвётся — drop. *)
      (try [Mf_event.update (f old) (f new_value) ts]
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
    | Mf_event.Update { new_value = v; _ } as ev ->
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
    | Mf_event.Update { old; new_value; ts } ->
      (* Для Update: применяем f к old и new, zip результаты. *)
      (try
        let olds = f old in
        let news = f new_value in
        let rec zip xs ys = match xs, ys with
          | x :: xt, y :: yt -> Mf_event.update x y ts :: zip xt yt
          | _ -> []
        in zip olds news
       with e -> on_error e; [])
    | Mf_event.Watermark wm -> [Mf_event.wm wm]
    | Mf_event.Retract (_, _) -> [])
    upstream

(* ── Sink helpers ─────────────────────────────────────────── *)

let sink f stream =
  Stream.iter (function
    | Mf_event.Data (v, _) -> f v
    | Mf_event.Update { new_value = v; _ } -> f v
    | Mf_event.Watermark _ | Mf_event.Retract _ -> ()) stream

let collect stream =
  List.rev (Stream.fold (fun acc -> function
    | Mf_event.Data (v, _) -> v :: acc
    | Mf_event.Update { new_value = v; _ } -> v :: acc
    | Mf_event.Watermark _ | Mf_event.Retract _ -> acc) [] stream)

(* Удобные потребители для типичных паттернов "пройти поток и сделать X
   с каждым Data". Все основаны на Stream.iter/fold; пишутся ради
   читабельности пользовательского кода и тестов. *)
let iter_data = sink

let fold_data ~init ~f stream =
  Stream.fold (fun acc -> function
    | Mf_event.Data (v, _) -> f acc v
    | Mf_event.Update { new_value = v; _ } -> f acc v
    | Mf_event.Watermark _ | Mf_event.Retract _ -> acc) init stream

let count_data stream =
  fold_data ~init:0 ~f:(fun n _ -> n + 1) stream

let iter_events f stream = Stream.iter f stream

(* Non-terminal observer: применяет side-effect к каждому event и
   пропускает его дальше без изменений. label игнорируется самой
   функцией но может использоваться в closure callback'а. *)
let inspect ?label:_ f stream =
  Stream.map (fun ev -> f ev; ev) stream

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
    | Mf_event.Update { old; new_value; ts } ->
      (* Атомарно: remove old, put new. Если by даёт одинаковые
         ключи — просто replace. *)
      let old_k = by old ts in
      let new_k = by new_value ts in
      if old_k = new_k then
        Hashtbl.replace tbl new_k new_value
      else begin
        Hashtbl.remove tbl old_k;
        Hashtbl.replace tbl new_k new_value
      end
    | Mf_event.Watermark _ -> ()) () stream;
  Hashtbl.fold (fun k v acc -> (k, v) :: acc) tbl []

(* ── Shorthand: seconds / minutes в операторах ───────────── *)

(* ── Instrumented operators ──────────────────────────────── *)
(* Версии операторов с метриками — используются в runtime *)

(** Обернуть stream: вызывать f() на каждом Data или Update событии
    (т.е. на каждом несущем "новое" значение). *)
let with_counter f upstream =
  fun () ->
    match upstream () with
    | Some (Mf_event.Data _ as ev)
    | Some (Mf_event.Update _ as ev) -> f (); Some ev
    | Some (Mf_event.Watermark _ as ev)
    | Some (Mf_event.Retract _ as ev) -> Some ev
    | None -> None


(** window с histogram для latency закрытия окна (переэкспорт из Window) *)
let window_instrumented = Window.window_instrumented

(* ── ProcessFunction с таймерами (переэкспорт из Process_fn) ─── *)
type timer_kind = Process_fn.timer_kind =
  | Event_time
  | Processing_time
type 'out ctx = 'out Process_fn.ctx = {
  clear_state             : unit -> unit;
  emit                    : 'out -> unit;
  set_event_timer         : Time.t -> unit;
  set_event_timer_for     : string -> Time.t -> unit;
  set_processing_timer    : Time.t -> unit;
  cancel_event_timer      : Time.t -> unit;
  cancel_event_timers     : unit -> unit;
  cancel_processing_timer : Time.t -> unit;
  cancel_processing_timers: unit -> unit;
}
let process_keyed = Process_fn.process_keyed
let process_keyed_spec = Process_fn.process_keyed_spec
