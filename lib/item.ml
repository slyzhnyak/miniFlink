(* ====================================================================
   item.ml — вспомогательные операторы для item-потоков.

   silence_age — единственный (пока) сложный item, эмитящий
   "(key, age_ms)" обновления:
   - на каждое реальное событие ключа: (key, 0)
   - каждые tick миллисекунд после последнего события: (key, age)

   Реализация pull-based, как Trigger.of_stream:
   - Hashtbl<key_string, last_seen_ts × key>
   - sorted timer list (фиксированное число активных таймеров =
     числу ключей, для 4096 шахтёров с tick=30s это ~140 эмиссий/с)
   - таймеры срабатывают по watermark
   ==================================================================== *)

type pending_timer = {
  pt_fire_at : Time.t;
  pt_key_str : string;
}

module Timers = struct
  type t = { mutable list : pending_timer list }
  let create () = { list = [] }
  let insert tm pt =
    let rec ins acc = function
      | [] -> List.rev (pt :: acc)
      | h :: tl when h.pt_fire_at > pt.pt_fire_at ->
        List.rev_append acc (pt :: h :: tl)
      | h :: tl -> ins (h :: acc) tl
    in
    tm.list <- ins [] tm.list

  let pop_due tm ~wm =
    let rec loop acc = function
      | [] -> tm.list <- []; List.rev acc
      | h :: tl when h.pt_fire_at <= wm -> loop (h :: acc) tl
      | rest -> tm.list <- rest; List.rev acc
    in
    loop [] tm.list

  let remove_key tm ~key_str =
    tm.list <- List.filter (fun pt -> pt.pt_key_str <> key_str) tm.list
end

(* Сериализация ключа через Hashtbl.hash. См. ту же стратегию в
   trigger.ml — храним вместе с last_seen в таблице сам ключ,
   чтобы вернуть его при срабатывании таймера. *)
let key_to_string : 'k -> string = fun k ->
  string_of_int (Hashtbl.hash k)

let silence_age
    (type ev k)
    ~(by : ev -> k)
    ~(tick : Time.t)
    (source : ev Mf_event.t Stream.t)
  : (k * Time.t) Mf_event.t Stream.t =

  (* per-key state: last_seen_ts + сам ключ для обратной деривации *)
  let states : (string, Time.t * k) Hashtbl.t = Hashtbl.create 64 in
  let timers = Timers.create () in
  let out_buf : (k * Time.t) Mf_event.t Queue.t = Queue.create () in

  (* На каждое Data ключа k в момент ts:
     - обновляем last_seen
     - удаляем старый таймер этого ключа (если был)
     - регистрируем новый на ts + tick
     - эмитим (k, 0) *)
  let on_data (ev : ev) (ts : Time.t) =
    let key = by ev in
    let ks = key_to_string key in
    Hashtbl.replace states ks (ts, key);
    Timers.remove_key timers ~key_str:ks;
    Timers.insert timers
      { pt_fire_at = ts + tick; pt_key_str = ks };
    Queue.push (Mf_event.data (key, 0) ts) out_buf
  in

  (* На срабатывание таймера в момент wm:
     - эмитим (key, wm - last_seen) — текущий silence-age
     - регистрируем следующий тик на wm + tick *)
  let on_timer (pt : pending_timer) ~wm =
    match Hashtbl.find_opt states pt.pt_key_str with
    | None -> ()
    | Some (last_seen, key) ->
      let age = wm - last_seen in
      Queue.push (Mf_event.data (key, age) wm) out_buf;
      Timers.insert timers
        { pt_fire_at = wm + tick; pt_key_str = pt.pt_key_str }
  in

  let on_watermark wm =
    let due = Timers.pop_due timers ~wm in
    List.iter (fun pt -> on_timer pt ~wm) due
  in

  let rec next () =
    if not (Queue.is_empty out_buf) then Some (Queue.pop out_buf)
    else
      match source () with
      | None -> None
      | Some (Mf_event.Watermark wm) ->
        on_watermark wm;
        if Queue.is_empty out_buf then Some (Mf_event.wm wm)
        else begin
          Queue.push (Mf_event.wm wm) out_buf;
          Some (Queue.pop out_buf)
        end
      | Some (Mf_event.Retract _) ->
        (* Retract в upstream — игнорируем; нас интересует
           только когда было ПОСЛЕДНЕЕ событие. *)
        next ()
      | Some (Mf_event.Data (v, ts)) ->
        on_data v ts;
        next ()
  in
  next
