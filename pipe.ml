(* ============================================================
   Pipe.ml — операторы pipeline

   Ключевое отличие от предыдущей версии:
   - window, enrich, dedup получают ключ из модуля KEYED
   - никаких ~key: параметров в пользовательском коде
   - window использует locally abstract type — нет рекурсивных типов
   ============================================================ *)

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
    let last = (ts / step) * step in
    let rec go s acc =
      if s + size <= ts || s > ts then acc
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

(* dedup использует KEYED для ключа.
   rule : 'a -> string  — тип алерта (второй компонент ключа дедупликации) *)
let dedup
    (type a)
    (module K : Keyed.S with type t = a)
    ~(rule     : a -> string)
    ~(cooldown : Time.t)
    (upstream  : a Mf_event.t Stream.t) : a Mf_event.t Stream.t =
  let seen = Hashtbl.create 64 in
  Stream.filter (function
    | Mf_event.Watermark _ | Mf_event.Retract _ -> true
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
let tumbling size = Tumbling size
let sliding  size step = Sliding (size, step)

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
