(* Общий keyed-state с TTL.

   Обобщение TTL, который раньше был только у Table (of_stream_ttl), на
   произвольное состояние оператора. Запись по ключу живёт [ttl]
   (по event-time) с момента последнего обновления; после этого
   считается истёкшей. [advance] физически вычищает истёкшие записи,
   ограничивая память при растущем пространстве ключей.

   Истечение — по EVENT-TIME (переданному [now]), не по wall-clock: это
   детерминированно и согласуется с watermark-моделью библиотеки.
   Типичная идиома: на каждом событии вызвать [put ~now:ts], на
   watermark — [advance ~now:wm]. *)

type ('k, 'v) t = {
  tbl : ('k, 'v * Time.t) Hashtbl.t;   (* ключ → (значение, last_update_ts) *)
  ttl : Time.t;
}

let create ~ttl () = { tbl = Hashtbl.create 64; ttl }

(* запись жива на момент [now], если её последнее обновление не дальше
   [ttl] в прошлом: last + ttl >= now *)
let alive_at ttl last now = last + ttl >= now

(* put: записать/обновить значение, освежив TTL (last := now) *)
let put t ~now k v = Hashtbl.replace t.tbl k (v, now)

(* get: значение если ещё живо на момент [now], иначе None. Истёкшее не
   «воскрешается» — даже если физически ещё в таблице (до advance),
   читается как None. *)
let get t ~now k =
  match Hashtbl.find_opt t.tbl k with
  | Some (v, last) when alive_at t.ttl last now -> Some v
  | _ -> None

(* advance: физически удалить все записи, истёкшие на момент [now].
   Вызывать на watermark, чтобы ограничить рост памяти. *)
let advance t ~now =
  let stale = Hashtbl.fold (fun k (_, last) acc ->
    if alive_at t.ttl last now then acc else k :: acc) t.tbl [] in
  List.iter (Hashtbl.remove t.tbl) stale

(* размер (число физически хранимых записей, включая ещё не вычищенные
   истёкшие — для контроля памяти вызывай advance перед size) *)
let size t = Hashtbl.length t.tbl

(* удалить ключ явно *)
let remove t k = Hashtbl.remove t.tbl k

(* живые пары на момент [now] (для итерации/снапшота) *)
let to_list t ~now =
  Hashtbl.fold (fun k (v, last) acc ->
    if alive_at t.ttl last now then (k, v) :: acc else acc) t.tbl []
