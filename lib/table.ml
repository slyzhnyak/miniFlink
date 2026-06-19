(* ============================================================
   Table.ml — таблица как функция ключ → значение option

   Три конструктора:
   of_list     — статический список, загружается один раз
   of_hashtbl  — обновляемая таблица
   of_stream   — живой snapshot из потока (последнее значение по ключу)
   ============================================================ *)

type ('k, 'v) t = 'k -> 'v option

let of_list pairs =
  let h = Hashtbl.create (List.length pairs) in
  List.iter (fun (k,v) -> Hashtbl.replace h k v) pairs;
  Hashtbl.find_opt h

let of_hashtbl h = Hashtbl.find_opt h

(* snapshot table: обновляется из потока, возвращает последнее значение.
   ВАЖНО: размер ограничен числом УНИКАЛЬНЫХ ключей (replace, не add),
   а не числом событий. Утечка возможна только если ключи неограниченно
   растут (напр. уникальный id в каждом событии) — для этого случая
   используйте of_stream_ttl.

   Ограничение pump: вычитывает источник до None. Для живого канала
   None = "пока пусто" (ок). Для конечного Stream.of_list первый lookup
   осушит весь источник — используйте of_list для статики. *)
let of_stream ~key (src : 'a Mf_event.t Stream.t) =
  let h = Hashtbl.create 64 in
  let pump () =
    let rec go () = match src () with
      | Some (Mf_event.Data (v, _)) ->
        Hashtbl.replace h (key v) v; go ()
      | Some (Mf_event.Update { new_value = v; _ }) ->
        (* Update заменяет old → new; таблица хранит current value,
           так что просто пишем new_value. *)
        Hashtbl.replace h (key v) v; go ()
      | Some (Mf_event.Watermark _)
      | Some (Mf_event.Retract _)
      | None -> ()
    in go ()
  in
  fun k -> pump (); Hashtbl.find_opt h k

(* snapshot table с TTL: записи старше ttl относительно последнего
   виденного event-time удаляются. Ограничивает размер при растущем
   пространстве ключей. Хранит (значение, last_ts) на ключ. *)
let of_stream_ttl ~key ~ttl (src : 'a Mf_event.t Stream.t) =
  let h = Hashtbl.create 64 in
  let max_ts = ref min_int in
  let evict () =
    if !max_ts > min_int then begin
      let stale = Hashtbl.fold (fun k (_, t) acc ->
        if t < !max_ts - ttl then k :: acc else acc) h [] in
      List.iter (Hashtbl.remove h) stale
    end
  in
  let pump () =
    let rec go () = match src () with
      | Some (Mf_event.Data (v, t)) ->
        if t > !max_ts then max_ts := t;
        Hashtbl.replace h (key v) (v, t);
        go ()
      | Some (Mf_event.Update { new_value = v; ts = t; _ }) ->
        if t > !max_ts then max_ts := t;
        Hashtbl.replace h (key v) (v, t);
        go ()
      | Some (Mf_event.Watermark wm) ->
        if wm > !max_ts then max_ts := wm;
        go ()
      | Some (Mf_event.Retract _) ->
        (* Retract на enrichment-table семантически неоднозначен —
           что значит «отменить добавление сущности в таблицу»?
           Оставляем текущее поведение: останавливаемся, как раньше. *)
        ()
      | None -> ()
    in go (); evict ()
  in
  fun k ->
    pump ();
    match Hashtbl.find_opt h k with Some (v,_) -> Some v | None -> None

let build ~key (src : 'a Mf_event.t Stream.t) =
  let h = Hashtbl.create 64 in
  let rec go () = match src () with
    | Some (Mf_event.Data (v, _)) ->
      Hashtbl.replace h (key v) v; go ()
    | Some (Mf_event.Update { new_value = v; _ }) ->
      Hashtbl.replace h (key v) v; go ()
    | Some (Mf_event.Watermark _) | Some (Mf_event.Retract _) -> go ()
    | None -> ()
  in
  go ();
  fun k -> Hashtbl.find_opt h k
