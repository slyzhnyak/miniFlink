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
  (* Hashtbl<key_str, fire_at>: O(1) insert/remove_key.
     pop_due — O(n) скан по всем pending, но он вызывается только
     на Watermark (редко) в отличие от insert/remove_key которые
     срабатывают на каждое Data-событие. Это даёт on average
     O(1) per event vs O(n) у списка. *)
  type t = (string, Time.t) Hashtbl.t
  let create () : t = Hashtbl.create 64

  let insert (tm : t) pt =
    Hashtbl.replace tm pt.pt_key_str pt.pt_fire_at

  let pop_due (tm : t) ~wm =
    (* Собираем все due и удаляем из таблицы. Используем
       Hashtbl.fold чтобы пройти один раз. Сортируем result по
       fire_at чтобы on_timer вызывался в правильном порядке. *)
    let due = Hashtbl.fold (fun ks fire_at acc ->
      if fire_at <= wm then
        { pt_fire_at = fire_at; pt_key_str = ks } :: acc
      else acc) tm []
    in
    List.iter (fun pt -> Hashtbl.remove tm pt.pt_key_str) due;
    List.sort (fun a b -> compare a.pt_fire_at b.pt_fire_at) due

  let remove_key (tm : t) ~key_str =
    Hashtbl.remove tm key_str
end

(* Сериализация ключа через Hashtbl.hash. См. ту же стратегию в
   trigger.ml — храним вместе с last_seen в таблице сам ключ,
   чтобы вернуть его при срабатывании таймера. *)
let default_key_to_string : 'k -> string = fun k ->
  string_of_int (Hashtbl.hash k)

let silence_age
    (type ev k)
    ?(name = "default")
    ~(by : ev -> k)
    ~(tick : Time.t)
    (source : ev Mf_event.t Stream.t)
  : (k * Time.t) Mf_event.t Stream.t =

  (* per-key state: last_seen_ts + сам ключ для обратной деривации *)
  let states : (string, Time.t * k) Hashtbl.t = Hashtbl.create 64 in
  let timers = Timers.create () in
  let out_buf : (k * Time.t) Mf_event.t Queue.t = Queue.create () in

  let key_to_string (key : k) : string = default_key_to_string key in

  (* ════════════════════════════════════════════════════════════════
     PERSISTENCE — ортогональная, через Managed_state.

     Persistence решается ambient Runtime_context. Рабочие структуры
     (states + timers) остаются; managed-state — durable-зеркало,
     значение per-ключ = (last_seen_ts, key, fire_at).
     ════════════════════════════════════════════════════════════════ *)
  let mstate : (string, Time.t * k * Time.t) Managed_state.t =
    Managed_state.create_string ~name:("item:silence_age:" ^ name) () in

  let persist (ks : string) (key : k) (last_seen : Time.t) (fire_at : Time.t) =
    Managed_state.set mstate ks (last_seen, key, fire_at);
    Managed_state.checkpoint_key mstate ks
  in

  (* Восстановление per-key state из managed-state на старте. *)
  let restore_all () =
    Managed_state.iter mstate (fun ks (last_seen, key, fire_at) ->
      Hashtbl.replace states ks (last_seen, key);
      Timers.insert timers { pt_fire_at = fire_at; pt_key_str = ks })
  in
  restore_all ();


  (* На каждое Data ключа k в момент ts:
     - обновляем last_seen
     - удаляем старый таймер этого ключа (если был)
     - регистрируем новый на ts + tick
     - эмитим (k, 0) *)
  let on_data (ev : ev) (ts : Time.t) =
    let key = by ev in
    let ks = key_to_string key in
    let fire_at = ts + tick in
    Hashtbl.replace states ks (ts, key);
    Timers.remove_key timers ~key_str:ks;
    Timers.insert timers
      { pt_fire_at = fire_at; pt_key_str = ks };
    Queue.push (Mf_event.data (key, 0) ts) out_buf;
    persist ks key ts fire_at
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
      let next_fire = wm + tick in
      Timers.insert timers
        { pt_fire_at = next_fire; pt_key_str = pt.pt_key_str };
      (* После таймера last_seen НЕ меняется (нового события не было),
         меняется только fire_at следующего таймера. Это важно
         сохранить чтобы restore нашёл правильную позицию. *)
      persist pt.pt_key_str key last_seen next_fire
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
      | Some (Mf_event.Update { new_value = v; ts; _ }) ->
        (* Update — атомарная коррекция; для silence_age важно
           "когда было ПОСЛЕДНЕЕ событие". Update обновляет это
           timestamp, применяем on_data на new_value. *)
        on_data v ts;
        next ()
      | Some (Mf_event.Data (v, ts)) ->
        on_data v ts;
        next ()
  in
  next
