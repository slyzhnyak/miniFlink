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
    ?(backend : Persistence_backend.t option)
    ?(backend_name : string option)
    ?(serialize_key : (k -> Yojson.Safe.t) option)
    ?(deserialize_key : (Yojson.Safe.t -> k) option)
    ?(persistence : k Persistence_backend.persist option)
    ~(by : ev -> k)
    ~(tick : Time.t)
    (source : ev Mf_event.t Stream.t)
  : (k * Time.t) Mf_event.t Stream.t =

  (* Нормализация двух API'шек persistence в единое представление.
     Конфликт (оба заданы) — Invalid_argument. *)
  let old_style_any =
    backend <> None || backend_name <> None
    || serialize_key <> None || deserialize_key <> None in
  if persistence <> None && old_style_any then
    invalid_arg
      "Item.silence_age: cannot mix ?persistence with \
       individual ?backend/?backend_name/?serialize_key/?deserialize_key — \
       use one style or the other";
  let backend, backend_name, serialize_key, deserialize_key =
    match persistence with
    | Some p ->
      (Some p.Persistence_backend.backend,
       Some p.Persistence_backend.name,
       Some p.Persistence_backend.serialize,
       Some p.Persistence_backend.deserialize)
    | None ->
      (backend, backend_name, serialize_key, deserialize_key)
  in

  (* Если backend подключён — обязательны name + сериализаторы. *)
  (match backend with
   | None -> ()
   | Some _ ->
     let missing =
       (if backend_name    = None then ["backend_name"]    else []) @
       (if serialize_key   = None then ["serialize_key"]   else []) @
       (if deserialize_key = None then ["deserialize_key"] else [])
     in
     if missing <> [] then
       invalid_arg (Printf.sprintf
         "Item.silence_age: backend provided but missing: %s"
         (String.concat ", " missing)));

  (* per-key state: last_seen_ts + сам ключ для обратной деривации *)
  let states : (string, Time.t * k) Hashtbl.t = Hashtbl.create 64 in
  let timers = Timers.create () in
  let out_buf : (k * Time.t) Mf_event.t Queue.t = Queue.create () in

  (* Локальный key_to_string. Без backend'а — Hashtbl.hash;
     с backend'ом — JSON-сериализация для детерминизма между
     запусками процесса. *)
  let key_to_string (key : k) : string =
    match backend, serialize_key with
    | Some _, Some sk -> Yojson.Safe.to_string (sk key)
    | _ -> default_key_to_string key
  in

  (* ════════════════════════════════════════════════════════════════
     PERSISTENCE LAYER

     Формат backend-ключа:
       "item:silence_age:{backend_name}:" ^ Yojson.to_string (ser_k user_key)

     Формат значения (JSON в bytes):
       {
         "key":          <serialized 'k>,
         "last_seen_ts": int,
         "fire_at":      int   // запланированное срабатывание таймера
       }

     Snapshot пишется при: on_data (новое событие — обновляем
     last_seen + fire_at) и on_timer (срабатывание — обновляем
     fire_at). На watermark без таймера — не пишем (изменений нет).
     ════════════════════════════════════════════════════════════════ *)

  let key_prefix =
    match backend_name with
    | Some n -> "item:silence_age:" ^ n ^ ":"
    | None -> ""  (* не используется когда backend = None *)
  in

  let ser_k k =
    match serialize_key with
    | Some f -> f k
    | None -> assert false  (* проверено выше *)
  in
  let deser_k j =
    match deserialize_key with
    | Some f -> f j
    | None -> assert false
  in

  let persist (ks : string) (key : k) (last_seen : Time.t) (fire_at : Time.t) =
    match backend with
    | None -> ()
    | Some be ->
      let json = `Assoc [
        ("key",          ser_k key);
        ("last_seen_ts", `Int last_seen);
        ("fire_at",      `Int fire_at);
      ] in
      let bk = key_prefix ^ Yojson.Safe.to_string (ser_k key) in
      be.set bk (Bytes.of_string (Yojson.Safe.to_string json))
  in

  (* Восстановление per-key state из backend на старте. *)
  let restore_all () =
    match backend with
    | None -> ()
    | Some be ->
      let plen = String.length key_prefix in
      List.iter (fun bk ->
        if String.length bk >= plen
           && String.sub bk 0 plen = key_prefix
        then begin
          match be.get bk with
          | None -> ()
          | Some v_bytes ->
            (try
               let json = Yojson.Safe.from_string (Bytes.to_string v_bytes) in
               match json with
               | `Assoc kv ->
                 let key       = deser_k (List.assoc "key" kv) in
                 let last_seen = Yojson.Safe.Util.to_int (List.assoc "last_seen_ts" kv) in
                 let fire_at   = Yojson.Safe.Util.to_int (List.assoc "fire_at" kv) in
                 let ks        = key_to_string key in
                 Hashtbl.replace states ks (last_seen, key);
                 Timers.insert timers
                   { pt_fire_at = fire_at; pt_key_str = ks }
               | _ -> failwith "Item.silence_age restore: top-level JSON not object"
             with
             | Yojson.Json_error msg ->
               failwith ("Item.silence_age restore: invalid JSON (key=" ^ bk ^ "): " ^ msg)
             | Not_found ->
               failwith ("Item.silence_age restore: missing field (key=" ^ bk ^ ")"))
        end
      ) (be.keys ())
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
      | Some (Mf_event.Data (v, ts)) ->
        on_data v ts;
        next ()
  in
  next
