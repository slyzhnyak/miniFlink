(** Экспериментальная реализация [window] на Hashtbl вместо Map.Make.

    Цель — A/B-проверить гипотезу из bench_ops: «WMap (immutable Map)
    с 12× insert на каждое sliding-событие — главный узкий путь
    `lib/window.ml`».

    Иначе говорим: WMap.add создаёт O(log N) дерева на каждое окно;
    у нас 4096 ключей × 24 окна = ~98K записей, log2 ~ 17 узлов
    аллоцируется при каждом add. Sliding 60/5 — 12 окон на событие;
    значит на каждый event ~204 cons-cell аллокаций ТОЛЬКО за счёт
    immutable Map. Hashtbl.replace должен быть на порядок дешевле
    (O(1) амортизированно, без аллокаций при не-resize).

    Это эксперимент, не финальная реализация. Если ускорение
    значительное, переводим [lib/window.ml] на Hashtbl как отдельный
    коммит с обновлением тестов. *)

open Miniflink

(* Ключ окна *)
type win_key = string * int * int   (* (lamp-id, start, stop) *)

(* Состояние окна: открытое (накапливаем) или закрытое (для retract) *)
type 'a win_state =
  | Open  of 'a list
  | Fired of 'a list

(* sliding spec на size/step — точная копия логики lib/window.ml:
   выравнивание стартов от 0, идём ВНИЗ от наибольшего кратного step,
   пока окно ещё накрывает ts. Это обязательное условие для
   эквивалентности с lib. *)
let floor_div a b = if a >= 0 then a / b else (a - b + 1) / b

let sliding_assign ~size ~step ts =
  let last = (floor_div ts step) * step in
  let min_start = if ts >= 0 then 0 else min_int in
  let rec go s acc =
    if s + size <= ts || s < min_start then acc
    else go (s - step) ((s, s + size) :: acc)
  in go last []

(** Hashtbl-based window. Та же семантика что Pipe.window:
    Open накапливает, Fired retract+re-emit для late, allowed_lateness
    выселение из таблицы. *)
let window_hashtbl
    (type a)
    (module K : Keyed.S with type t = a)
    ~size ~step
    ?(latency = 0)
    ?(allowed_lateness = 0)
    (upstream : a Mf_event.t Stream.t)
    : (string * a list) Mf_event.t Stream.t =
  let tbl : (win_key, a win_state) Hashtbl.t = Hashtbl.create 4096 in
  let cur_wm = ref min_int in
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
        Hashtbl.iter (fun (k,_,stop) st ->
          match st with Open vs when vs <> [] -> emit_data k stop vs | _ -> ()
        ) tbl;
        Hashtbl.clear tbl;
        if Queue.is_empty out then None else Some (Queue.pop out)
      | Some (Mf_event.Watermark wm) ->
        cur_wm := wm;
        (* Open окна со stop+latency <= wm → Fire *)
        (* Hashtbl нельзя мутировать в iter — собираем ключи отдельно *)
        let to_fire = ref [] in
        Hashtbl.iter (fun key st ->
          let (_,_,stop) = key in
          match st with
          | Open vs when stop + latency <= wm -> to_fire := (key, vs) :: !to_fire
          | _ -> ()
        ) tbl;
        List.iter (fun ((k,_,stop) as key, vs) ->
          if vs <> [] then emit_data k stop vs;
          Hashtbl.replace tbl key (Fired vs)
        ) !to_fire;
        (* Удалить старые Fired *)
        let to_remove = ref [] in
        Hashtbl.iter (fun key st ->
          let (_,_,stop) = key in
          match st with
          | Fired _ when stop + latency + allowed_lateness <= wm ->
            to_remove := key :: !to_remove
          | _ -> ()
        ) tbl;
        List.iter (Hashtbl.remove tbl) !to_remove;
        Queue.push (Mf_event.wm wm) out;
        pull ()
      | Some (Mf_event.Retract _) -> pull ()
      | Some (Mf_event.Update _) -> pull ()
      | Some (Mf_event.Data (v,t)) ->
        let windows = sliding_assign ~size ~step t in
        List.iter (fun (s, stop) ->
          let mk = (K.key v, s, stop) in
          match Hashtbl.find_opt tbl mk with
          | None ->
            if stop + latency + allowed_lateness <= !cur_wm then ()
            else Hashtbl.add tbl mk (Open [v])
          | Some (Open vs) ->
            Hashtbl.replace tbl mk (Open (v :: vs))
          | Some (Fired vs) ->
            emit_retract (K.key v) stop vs;
            let vs' = v :: vs in
            emit_data (K.key v) stop vs';
            Hashtbl.replace tbl mk (Fired vs')
        ) windows;
        pull ()
    in pull ()
