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

(* snapshot table: обновляется из потока, возвращает последнее значение *)
let of_stream ~key (src : 'a Mf_event.t Stream.t) =
  let h = Hashtbl.create 64 in
  (* pump: вычитать все готовые события *)
  let pump () =
    let rec go () = match src () with
      | Some (Mf_event.Data (v,_)) ->
        Hashtbl.replace h (key v) v; go ()
      | _ -> ()
    in go ()
  in
  fun k -> pump (); Hashtbl.find_opt h k
