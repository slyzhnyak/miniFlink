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

(* ── Window ───────────────────────────────────────────────── *)

module WMap = Map.Make(struct
  type t = string * int * int
  let compare = compare
end)

type win_spec =
  | Tumbling of Time.t
  | Sliding  of Time.t * Time.t   (* size, step *)

let assign spec ts =
  match spec with
  | Tumbling size ->
    let s = (ts / size) * size in [(s, s + size)]
  | Sliding (size, step) ->
    (* Событие ts принадлежит окну [s, s+size) когда s <= ts < s+size.
       Окна начинаются на кратных step. Наибольший подходящий старт —
       наибольшее кратное step, не превышающее ts. Идём вниз пока
       окно ещё накрывает ts (s + size > ts) и s >= 0. *)
    let last = (ts / step) * step in
    let rec go s acc =
      if s < 0 || s + size <= ts then acc
      else go (s - step) ((s, s + size) :: acc)
    in go last []

(* window groups by KEYED.key, fires when watermark closes the window.
   Output: (key * value list) per window.

   Late data handling (retractions):
   - Окно после закрытия по watermark переходит в Fired, но данные
     сохраняются ещё на `allowed_lateness` времени.
   - Late Data попадающее в Fired окно: переоткрывает его, эмитит
     Retract(старый результат) затем Data(новый результат).
   - Окно окончательно удаляется когда wm > stop + latency + allowed_lateness. *)

type 'a win_state =
  | Open  of 'a list
  | Fired of 'a list   (* данные сохранены для late data *)

(** [window (module K) ?latency ?allowed_lateness spec upstream]
    группирует события по ключу [K.key] и временным окнам [spec]
    ({!tumbling} или {!sliding}). Окно закрывается когда watermark
    проходит его правую границу (плюс [latency]), выдавая
    [(ключ, событие list)].

    Поздние данные в пределах [~allowed_lateness] переоткрывают
    закрытое окно: эмитится [Retract] старого результата, затем
    [Data] нового. За пределами [allowed_lateness] окно окончательно
    удаляется (ограничивает рост состояния). *)
let window
    (type a)
    (module K : Keyed.S with type t = a)
    ?(latency = 0)
    ?(allowed_lateness = 0)
    (spec     : win_spec)
    (upstream : a Mf_event.t Stream.t)
    : (string * a list) Mf_event.t Stream.t =
  let tbl : a win_state WMap.t ref = ref WMap.empty in
  let out : (string * a list) Mf_event.t Queue.t = Queue.create () in
  let emit_data k stop vs =
    Queue.push (Mf_event.data (k, List.rev vs) stop) out in
  let emit_retract k stop vs =
    Queue.push (Mf_event.retract (k, List.rev vs) stop) out in
  fun () ->
    let rec pull () =
      if not (Queue.is_empty out) then Some (Queue.pop out) else
      match upstream () with
      | None ->
        (* Закрываем все Open окна; Fired уже эмитили *)
        WMap.iter (fun (k,_,stop) st ->
          match st with Open vs when vs <> [] -> emit_data k stop vs | _ -> ()
        ) !tbl;
        tbl := WMap.empty;
        if Queue.is_empty out then None else Some (Queue.pop out)
      | Some (Mf_event.Watermark wm) ->
        (* Open окна со stop+latency <= wm → закрываем (Fire) *)
        WMap.iter (fun (k,s,stop) st ->
          match st with
          | Open vs when stop + latency <= wm ->
            if vs <> [] then emit_data k stop vs;
            tbl := WMap.add (k,s,stop) (Fired vs) !tbl
          | _ -> ()
        ) !tbl;
        (* Fired окна старше allowed_lateness → удаляем окончательно *)
        tbl := WMap.filter (fun (_,_,stop) st ->
          match st with
          | Fired _ -> stop + latency + allowed_lateness > wm
          | Open _  -> true
        ) !tbl;
        Queue.push (Mf_event.wm wm) out;
        pull ()
      | Some (Mf_event.Retract _) -> pull ()
      | Some (Mf_event.Data (v,t)) ->
        List.iter (fun (s, stop) ->
          let mk = (K.key v, s, stop) in
          match WMap.find_opt mk !tbl with
          | None ->
            tbl := WMap.add mk (Open [v]) !tbl
          | Some (Open vs) ->
            tbl := WMap.add mk (Open (v :: vs)) !tbl
          | Some (Fired vs) ->
            (* Late data: переоткрываем окно.
               Retract старого результата, Data нового. *)
            emit_retract (K.key v) stop vs;
            let vs' = v :: vs in
            emit_data (K.key v) stop vs';
            tbl := WMap.add mk (Fired vs') !tbl
        ) (assign spec t);
        pull ()
    in pull ()

(* ── Aggregate ────────────────────────────────────────────── *)

(* aggregate принимает (key * 'a list) — выход window.
   f : string -> 'a list -> 'b *)
(** [aggregate f] сворачивает события каждого окна [(key, vs)] в один
    результат [f key vs] (например max-скорость, min-топливо). *)
let aggregate f = emap (fun (key, vs) -> f key vs)

(* ── Count windows ────────────────────────────────────────── *)

(** Спецификация count-окна: по числу событий, не по времени. *)
type count_spec =
  | CountTumbling of int          (** каждые N событий → окно *)
  | CountSliding  of int * int    (** окно размера N, шаг step (step<=N) *)

(** [count_window (module K) spec upstream] группирует события по ключу
    [K.key] и {e количеству}, а не времени. Окно эмитится когда у ключа
    накопилось нужное число событий — watermarks не нужны (это и есть
    их преимущество: результат без ожидания по event-time).

    - [count_tumbling n]: каждые [n] событий ключа → одно окно [(key, vs)],
      буфер сбрасывается.
    - [count_sliding n step]: окно из последних [n] событий, новое окно
      каждые [step] событий (при [step < n] окна перекрываются).

    Watermark и Retract проходят прозрачно. На конце потока неполные
    буферы {e не} эмитятся (count-окно по определению требует ровно [n]
    событий; неполный остаток — не окно).
    @raise Invalid_argument при [n <= 0] или [step <= 0]. *)
let count_window
    (type a)
    (module K : Keyed.S with type t = a)
    (spec : count_spec)
    (upstream : a Mf_event.t Stream.t)
    : (string * a list) Mf_event.t Stream.t =
  (match spec with
   | CountTumbling n -> if n <= 0 then invalid_arg "count_tumbling: n должно быть > 0"
   | CountSliding (n, step) ->
     if n <= 0 then invalid_arg "count_sliding: размер должен быть > 0";
     if step <= 0 then invalid_arg "count_sliding: шаг должен быть > 0");
  (* буфер на ключ: список накопленных значений (в обратном порядке) +
     счётчик с последней эмиссии (для sliding) *)
  let buffers : (string, a list * int) Hashtbl.t = Hashtbl.create 16 in
  let out : (string * a list) Mf_event.t Queue.t = Queue.create () in
  let emit k vs = Queue.push (Mf_event.data (k, vs) 0) out in
  let push_value k v =
    let (buf, since) = match Hashtbl.find_opt buffers k with
      | Some x -> x | None -> ([], 0) in
    let buf = v :: buf in
    let since = since + 1 in
    match spec with
    | CountTumbling n ->
      if List.length buf >= n then begin
        emit k (List.rev buf);
        Hashtbl.replace buffers k ([], 0)
      end else
        Hashtbl.replace buffers k (buf, since)
    | CountSliding (n, step) ->
      (* держим максимум n последних; эмитим каждые step событий когда
         накоплено >= n *)
      let buf = if List.length buf > n
        then (match List.rev buf with _ :: rest -> List.rev rest | [] -> [])
        else buf in
      if List.length buf >= n && since >= step then begin
        emit k (List.rev buf);
        Hashtbl.replace buffers k (buf, 0)   (* сдвиг: считаем step заново *)
      end else
        Hashtbl.replace buffers k (buf, since)
  in
  fun () ->
    let rec pull () =
      if not (Queue.is_empty out) then Some (Queue.pop out) else
      match upstream () with
      | None -> None   (* неполные буферы не эмитим *)
      | Some (Mf_event.Watermark wm) ->
        (* watermark несёт только время — пересоздаём в выходном типе *)
        Some (Mf_event.wm wm)
      | Some (Mf_event.Retract _) ->
        (* retract входного типа в count-окне не транслируется
           (значение чужого типа) — пропускаем *)
        pull ()
      | Some (Mf_event.Data (v, _)) ->
        push_value (K.key v) v;
        pull ()
    in pull ()

(** Count-tumbling: окно каждые [n] событий. *)
let count_tumbling n = CountTumbling n

(** Count-sliding: окно из [n] событий с шагом [step]. *)
let count_sliding n step = CountSliding (n, step)

(* ── Global window + custom triggers ──────────────────────── *)

(** Решение триггера при поступлении события. *)
type trigger_action =
  | Continue        (** копить дальше *)
  | Fire            (** эмитить окно, оставить накопленное (накопительно) *)
  | FireAndPurge    (** эмитить окно и сбросить буфер *)

(** Триггер: чистая функция от (число накопленных, последнее значение) к
    решению. Отделяет политику «когда фаерить» от группировки. *)
type 'a trigger = count:int -> last:'a -> trigger_action

(** Триггер «каждые n событий». *)
let trigger_count n : 'a trigger =
  fun ~count ~last:_ -> if count >= n then FireAndPurge else Continue

(** Триггер по предикату: фаерит когда событие удовлетворяет условию
    (например тревожное значение → ранняя эмиссия). *)
let trigger_on_value (pred : 'a -> bool) : 'a trigger =
  fun ~count:_ ~last -> if pred last then Fire else Continue

(** [global_window (module K) ~trigger upstream] держит {e одно} окно на
    ключ (без временных/количественных границ) и фаерит его когда скажет
    [trigger]. Основа, на которой через триггеры строятся другие политики:
    assigner (одно окно) отделён от trigger (когда эмитить).

    [Fire] эмитит накопленное не очищая (накопительный результат);
    [FireAndPurge] эмитит и сбрасывает. На конце потока непустой остаток
    эмитится. *)
let global_window
    (type a)
    (module K : Keyed.S with type t = a)
    ~(trigger : a trigger)
    (upstream : a Mf_event.t Stream.t)
    : (string * a list) Mf_event.t Stream.t =
  let buffers : (string, a list) Hashtbl.t = Hashtbl.create 16 in
  let out : (string * a list) Mf_event.t Queue.t = Queue.create () in
  let get k = match Hashtbl.find_opt buffers k with Some b -> b | None -> [] in
  fun () ->
    let rec pull () =
      if not (Queue.is_empty out) then Some (Queue.pop out) else
      match upstream () with
      | None ->
        Hashtbl.iter (fun k buf ->
          if buf <> [] then Queue.push (Mf_event.data (k, List.rev buf) 0) out
        ) buffers;
        Hashtbl.reset buffers;
        if Queue.is_empty out then None else Some (Queue.pop out)
      | Some (Mf_event.Watermark wm) -> Some (Mf_event.wm wm)
      | Some (Mf_event.Retract _) -> pull ()
      | Some (Mf_event.Data (v, _)) ->
        let k = K.key v in
        let buf = v :: get k in
        (match trigger ~count:(List.length buf) ~last:v with
         | Continue -> Hashtbl.replace buffers k buf
         | Fire ->
           Queue.push (Mf_event.data (k, List.rev buf) 0) out;
           Hashtbl.replace buffers k buf      (* накопительно *)
         | FireAndPurge ->
           Queue.push (Mf_event.data (k, List.rev buf) 0) out;
           Hashtbl.replace buffers k []);     (* сброс *)
        pull ()
    in pull ()

(* ── Session windows (с слиянием) ─────────────────────────── *)

(* Сессия одного ключа: интервал [start, last] + накопленные значения.
   gap определяет когда сессии сливаются и когда закрываются. *)
type 'a session = {
  s_start : int;          (* начало (мин. ts) *)
  s_last  : int;          (* конец (макс. ts) *)
  s_vals  : (int * 'a) list;  (* (ts, value), для слияния по времени *)
}

(** [session_window (module K) ~gap upstream] группирует события по ключу
    [K.key] в {e сессии} — периоды активности, разделённые паузами больше
    [gap]. В отличие от tumbling/sliding границы {e динамические}: сессия
    растёт пока приходят события в пределах [gap], и {b сливается} с
    соседней если новое событие перекрывает разрыв между ними.

    Это ломает допущение «окно = чистая функция от timestamp» (которое
    держит tumbling/sliding): сессия зависит от {e последовательности}
    событий, а не только от их времени. Поэтому это отдельный оператор
    со своим состоянием и логикой слияния.

    Сессия закрывается (эмитится [(key, vs)]) когда watermark проходит
    [last + gap] — позже события уже не могут её продлить. На конце потока
    все открытые сессии закрываются.
    @raise Invalid_argument при [gap <= 0]. *)
let session_window
    (type a)
    (module K : Keyed.S with type t = a)
    ~(gap : int)
    (upstream : a Mf_event.t Stream.t)
    : (string * a list) Mf_event.t Stream.t =
  if gap <= 0 then invalid_arg "session_window: gap должен быть > 0";
  (* активные сессии на ключ (список, обычно 1-2 штуки) *)
  let sessions : (string, a session list) Hashtbl.t = Hashtbl.create 16 in
  let out : (string * a list) Mf_event.t Queue.t = Queue.create () in

  (* добавить событие в сессии ключа, выполнив слияние при необходимости *)
  let add_event k ts v =
    let cur = match Hashtbl.find_opt sessions k with Some s -> s | None -> [] in
    (* новое «точечное» окно события *)
    let ev_session = { s_start = ts; s_last = ts; s_vals = [(ts, v)] } in
    (* сессия пересекается с событием если событие в пределах gap от неё:
       [s_start - gap, s_last + gap] *)
    let overlaps s =
      ts >= s.s_start - gap && ts <= s.s_last + gap in
    let (touched, untouched) = List.partition overlaps cur in
    (* сливаем все затронутые + новое событие в одну сессию *)
    let merged = List.fold_left (fun acc s ->
      { s_start = min acc.s_start s.s_start;
        s_last  = max acc.s_last s.s_last;
        s_vals  = s.s_vals @ acc.s_vals })
      ev_session touched in
    Hashtbl.replace sessions k (merged :: untouched)
  in

  (* закрыть сессии ключа, у которых last + gap <= wm *)
  let close_ready wm =
    Hashtbl.iter (fun k sess ->
      let (ready, still) = List.partition (fun s -> s.s_last + gap <= wm) sess in
      List.iter (fun s ->
        (* значения в порядке времени *)
        let vs = List.sort (fun (a,_) (b,_) -> compare a b) s.s_vals
                 |> List.map snd in
        Queue.push (Mf_event.data (k, vs) s.s_last) out) ready;
      if still = [] then Hashtbl.remove sessions k
      else Hashtbl.replace sessions k still
    ) (Hashtbl.copy sessions)
  in

  fun () ->
    let rec pull () =
      if not (Queue.is_empty out) then Some (Queue.pop out) else
      match upstream () with
      | None ->
        (* конец потока: закрыть все открытые сессии *)
        Hashtbl.iter (fun k sess ->
          List.iter (fun s ->
            let vs = List.sort (fun (a,_) (b,_) -> compare a b) s.s_vals
                     |> List.map snd in
            Queue.push (Mf_event.data (k, vs) s.s_last) out) sess
        ) sessions;
        Hashtbl.reset sessions;
        if Queue.is_empty out then None else Some (Queue.pop out)
      | Some (Mf_event.Watermark wm) ->
        close_ready wm;
        Queue.push (Mf_event.wm wm) out;
        pull ()
      | Some (Mf_event.Retract _) -> pull ()
      | Some (Mf_event.Data (v, t)) ->
        add_event (K.key v) t v;
        pull ()
    in pull ()

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
(** Неперекрывающиеся окна фиксированного размера [size] (> 0).
    @raise Invalid_argument если [size <= 0]. *)
let tumbling size =
  if size <= 0 then
    invalid_arg "Pipe.tumbling: размер окна должен быть > 0";
  Tumbling size

(** Перекрывающиеся окна: [sliding size step] — окна размера [size]
    с шагом [step] (при [step < size] окна перекрываются).
    @raise Invalid_argument если [size <= 0] или [step <= 0].
    Замечание: при [step > size] между окнами образуются «дыры» —
    события в них не попадут ни в одно окно. Это допустимо (downsampling),
    но обычно непреднамеренно; следи за соотношением осознанно. *)
let sliding size step =
  if size <= 0 then
    invalid_arg "Pipe.sliding: размер окна должен быть > 0";
  if step <= 0 then
    invalid_arg "Pipe.sliding: шаг окна должен быть > 0";
  Sliding (size, step)

(* ── Instrumented operators ──────────────────────────────── *)
(* Версии операторов с метриками — используются в runtime *)

(** Обернуть stream: вызывать f() на каждом Data событии *)
let with_counter f upstream =
  fun () ->
    match upstream () with
    | Some (Mf_event.Data _ as ev) -> f (); Some ev
    | other -> other

(** window с histogram для latency закрытия окна *)
let window_instrumented
    (type a)
    (module K : Keyed.S with type t = a)
    ?(latency = 0)
    ~observe_window_ms
    (spec : win_spec)
    (upstream : a Mf_event.t Stream.t)
    : (string * a list) Mf_event.t Stream.t =
  let tbl : a list WMap.t ref = ref WMap.empty in
  let out : (string * a list) Mf_event.t Queue.t = Queue.create () in
  let close k stop vs =
    if vs <> [] then begin
      let t0 = int_of_float (Unix.gettimeofday () *. 1_000_000.) in
      Queue.push (Mf_event.data (k, List.rev vs) stop) out;
      let t1 = int_of_float (Unix.gettimeofday () *. 1_000_000.) in
      observe_window_ms (float_of_int (t1 - t0))
    end
  in
  fun () ->
    let rec pull () =
      if not (Queue.is_empty out) then Some (Queue.pop out) else
      match upstream () with
      | None ->
        WMap.iter (fun (k,_,stop) vs -> close k stop vs) !tbl;
        tbl := WMap.empty;
        if Queue.is_empty out then None else Some (Queue.pop out)
      | Some (Mf_event.Watermark wm) ->
        let closed, open_ =
          WMap.partition (fun (_,_,stop) _ -> stop + latency <= wm) !tbl in
        tbl := open_;
        WMap.iter (fun (k,_,stop) vs -> close k stop vs) closed;
        Queue.push (Mf_event.wm wm) out;
        pull ()
      | Some (Mf_event.Retract _) -> pull ()
      | Some (Mf_event.Data (v,t)) ->
        List.iter (fun (s, stop) ->
          let mk = (K.key v, s, stop) in
          let existing = Option.value ~default:[] (WMap.find_opt mk !tbl) in
          tbl := WMap.add mk (v :: existing) !tbl
        ) (assign spec t);
        pull ()
    in pull ()
