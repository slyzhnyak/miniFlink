(* ====================================================================
   trigger.ml — реализация триггерной системы.

   Архитектура pull-based: возвращаем Stream.t, который при каждом
   вызове либо отдаёт накопленное из out_buf, либо подтягивает
   событие из upstream и обрабатывает его.

   Состояние per-key — Hashtbl<key, machine>. Таймеры — простой
   priority queue (отсортированный массив, на 4096 шахтёров и 1-2
   таймерах на каждого — терпимо).

   Таймеры срабатывают по watermark'у: на каждом Watermark'е
   проходим pending-таймеры с t <= wm, эмитим что нужно.
   ==================================================================== *)

type severity =
  | Not_classified
  | Info
  | Warning
  | Average
  | High
  | Disaster

let severity_name = function
  | Not_classified -> "NOT_CLASSIFIED"
  | Info     -> "INFO"
  | Warning  -> "WARNING"
  | Average  -> "AVERAGE"
  | High     -> "HIGH"
  | Disaster -> "DISASTER"

(* ────────────────────────────────────────────────────────────────────
   Backend для persistence — алиас на общий Persistence_backend.t
   ──────────────────────────────────────────────────────────────────── *)

type backend = Persistence_backend.t = {
  get    : string -> bytes option;
  set    : string -> bytes -> unit;
  delete : string -> unit;
  keys   : unit -> string list;
}

(* Обёртка над state_backend_memory (Hashtbl) — для тестов и
   простых случаев. Алиас для Persistence_backend.of_memory. *)
let backend_of_memory = Persistence_backend.of_memory

(* ────────────────────────────────────────────────────────────────────
   Условие
   ──────────────────────────────────────────────────────────────────── *)

type 'v condition = {
  check    : 'v -> bool;
  recovery : 'v -> bool;
}

let greater_than t = {
  check    = (fun v -> v > t);
  recovery = (fun v -> v <= t);
}

let less_than t = {
  check    = (fun v -> v < t);
  recovery = (fun v -> v >= t);
}

let greater_than_with_hysteresis ~problem ~recovery = {
  check    = (fun v -> v > problem);
  recovery = (fun v -> v < recovery);
}

let less_than_with_hysteresis ~problem ~recovery = {
  check    = (fun v -> v < problem);
  recovery = (fun v -> v > recovery);
}

let is_some = {
  check    = (function Some _ -> true | None -> false);
  recovery = (function None -> true   | Some _ -> false);
}

let is_none = {
  check    = (function None -> true   | Some _ -> false);
  recovery = (function Some _ -> true | None -> false);
}

let custom ~problem ~recovery = {
  check    = problem;
  recovery;
}

(* ────────────────────────────────────────────────────────────────────
   Спецификация
   ──────────────────────────────────────────────────────────────────── *)

type ('key, 'v, 'alert) spec = {
  s_name             : string;
  s_condition        : 'v condition;
  s_problem_for      : Time.t;
  s_recovery_for     : Time.t;
  s_severity         : severity;
  s_produce_alert    : key:'key -> value:'v -> ts:Time.t -> 'alert;
  s_produce_recovery : key:'key -> ts:Time.t -> 'alert;
  (* Опциональные сериализаторы для persistence. Если хотя бы один
     None, использовать backend в [of_stream] нельзя — получим
     Invalid_argument. *)
  s_serialize_key      : ('key -> Yojson.Safe.t) option;
  s_deserialize_key    : (Yojson.Safe.t -> 'key) option;
  s_serialize_value    : ('v -> Yojson.Safe.t) option;
  s_deserialize_value  : (Yojson.Safe.t -> 'v) option;
  s_serialize_alert    : ('alert -> Yojson.Safe.t) option;
  s_deserialize_alert  : (Yojson.Safe.t -> 'alert) option;
}

let create
    ~name
    ~condition
    ?(problem_for  = 0)
    ?(recovery_for = 0)
    ?(severity     = Not_classified)
    ~produce_alert
    ~produce_recovery
    ?serialize_key
    ?deserialize_key
    ?serialize_value
    ?deserialize_value
    ?serialize_alert
    ?deserialize_alert
    () =
  {
    s_name              = name;
    s_condition         = condition;
    s_problem_for       = problem_for;
    s_recovery_for      = recovery_for;
    s_severity          = severity;
    s_produce_alert     = produce_alert;
    s_produce_recovery  = produce_recovery;
    s_serialize_key     = serialize_key;
    s_deserialize_key   = deserialize_key;
    s_serialize_value   = serialize_value;
    s_deserialize_value = deserialize_value;
    s_serialize_alert   = serialize_alert;
    s_deserialize_alert = deserialize_alert;
  }

let name     s = s.s_name
let severity s = s.s_severity

(* ── Record-based config alternative API ───────────────────────────
   Backwards-compat: оригинальный Trigger.create остаётся; of_config
   просто разворачивает record в вызов create. *)

type ('key, 'v, 'alert) serdes_config = {
  serialize_key     : 'key -> Yojson.Safe.t;
  deserialize_key   : Yojson.Safe.t -> 'key;
  serialize_value   : 'v -> Yojson.Safe.t;
  deserialize_value : Yojson.Safe.t -> 'v;
  serialize_alert   : 'alert -> Yojson.Safe.t;
  deserialize_alert : Yojson.Safe.t -> 'alert;
}

type ('key, 'v, 'alert) config = {
  name             : string;
  condition        : 'v condition;
  produce_alert    : key:'key -> value:'v -> ts:Time.t -> 'alert;
  produce_recovery : key:'key -> ts:Time.t -> 'alert;
  problem_for      : Time.t;
  recovery_for     : Time.t;
  severity         : severity;
  serdes           : ('key, 'v, 'alert) serdes_config option;
}

let default_config ~name ~condition ~produce_alert ~produce_recovery = {
  name; condition; produce_alert; produce_recovery;
  problem_for  = 0;
  recovery_for = 0;
  severity     = Warning;
  serdes       = None;
}

let of_config (cfg : ('key, 'v, 'alert) config) : ('key, 'v, 'alert) spec =
  match cfg.serdes with
  | None ->
    create
      ~name:cfg.name
      ~condition:cfg.condition
      ~problem_for:cfg.problem_for
      ~recovery_for:cfg.recovery_for
      ~severity:cfg.severity
      ~produce_alert:cfg.produce_alert
      ~produce_recovery:cfg.produce_recovery
      ()
  | Some s ->
    create
      ~name:cfg.name
      ~condition:cfg.condition
      ~problem_for:cfg.problem_for
      ~recovery_for:cfg.recovery_for
      ~severity:cfg.severity
      ~produce_alert:cfg.produce_alert
      ~produce_recovery:cfg.produce_recovery
      ~serialize_key:s.serialize_key
      ~deserialize_key:s.deserialize_key
      ~serialize_value:s.serialize_value
      ~deserialize_value:s.deserialize_value
      ~serialize_alert:s.serialize_alert
      ~deserialize_alert:s.deserialize_alert
      ()

(* ────────────────────────────────────────────────────────────────────
   Внутреннее состояние per-key

   Ok                — условие не выполняется
   Pending_problem   — check=true, ждём дозревания problem_for
   Problem           — Problem эмитирован; запоминаем последний
                       эмитированный alert (для retract'а на recovery)
   Pending_ok        — recovery=true, ждём дозревания recovery_for
   ──────────────────────────────────────────────────────────────────── *)

type ('key, 'v, 'alert) state =
  | S_ok
  | S_pending_problem of {
      since      : Time.t;
      last_value : 'v;
      fire_at    : Time.t;   (* since + problem_for *)
    }
  | S_problem of {
      since         : Time.t;
      last_alert    : 'alert;
      last_alert_ts : Time.t;
    }
  | S_pending_ok of {
      problem_since : Time.t;
      last_alert    : 'alert;
      last_alert_ts : Time.t;
      recovery_since : Time.t;
      fire_at        : Time.t;
    }

(* Pending timer: запись «в момент T для ключа K выполнить переход».
   Храним только время + ключ. Содержимое state читаем из таблицы.

   Сортируем по времени; при срабатывании watermark идём по
   списку и забираем все с t <= wm. *)

type pending_timer = {
  t_fire_at : Time.t;
  t_key     : string;   (* key serialized: используем (Hashtbl.hash) → строку  *)
}

(* Для простоты используем массив, который держим отсортированным.
   На наших 4096 шахтёрах с 1-2 таймерами на каждого вставка
   O(n) не критична. *)
module Timers = struct
  type t = {
    mutable list : pending_timer list;  (* отсортирован по t_fire_at *)
  }
  let create () = { list = [] }
  let insert tm pt =
    let rec ins acc = function
      | [] -> List.rev (pt :: acc)
      | h :: tl when h.t_fire_at > pt.t_fire_at -> List.rev_append acc (pt :: h :: tl)
      | h :: tl -> ins (h :: acc) tl
    in
    tm.list <- ins [] tm.list

  let pop_due tm ~wm =
    let rec loop acc = function
      | [] -> tm.list <- []; List.rev acc
      | h :: tl when h.t_fire_at <= wm -> loop (h :: acc) tl
      | rest -> tm.list <- rest; List.rev acc
    in
    loop [] tm.list

  let remove_key tm ~key =
    tm.list <- List.filter (fun pt -> pt.t_key <> key) tm.list
end

(* ────────────────────────────────────────────────────────────────────
   of_stream: реализация
   ──────────────────────────────────────────────────────────────────── *)

(* Сериализация ключа для использования в строковом Hashtbl таймеров.
   Используем Hashtbl.hash; чтобы избежать коллизий в реальном коде
   с произвольным 'key, дополнительно пишем в state-таблицу с
   тем же ключом — это даёт страховку. *)
let key_to_string : 'k -> string = fun k ->
  string_of_int (Hashtbl.hash k)

let of_stream
    (type k v a)
    ?(backend : backend option)
    (spec : (k, v, a) spec)
    (source : (k * v) Mf_event.t Stream.t)
  : a Mf_event.t Stream.t =

  (* Если backend подключён, обязательны сериализаторы для всех трёх
     параметров. Иначе Invalid_argument в момент создания stream'а
     (не на первом event'е). *)
  (match backend with
   | None -> ()
   | Some _ ->
     let missing =
       (if spec.s_serialize_key     = None then ["serialize_key"]     else []) @
       (if spec.s_deserialize_key   = None then ["deserialize_key"]   else []) @
       (if spec.s_serialize_value   = None then ["serialize_value"]   else []) @
       (if spec.s_deserialize_value = None then ["deserialize_value"] else []) @
       (if spec.s_serialize_alert   = None then ["serialize_alert"]   else []) @
       (if spec.s_deserialize_alert = None then ["deserialize_alert"] else [])
     in
     if missing <> [] then
       invalid_arg (Printf.sprintf
         "Trigger.of_stream: backend provided but missing serializers: %s"
         (String.concat ", " missing)));

  let states : (string, (k, v, a) state * k) Hashtbl.t = Hashtbl.create 64 in
  let last_event_ts : (string, Time.t) Hashtbl.t = Hashtbl.create 64 in
  let timers = Timers.create () in
  let out_buf : a Mf_event.t Queue.t = Queue.create () in

  (* Локальный key_to_string. Без backend'а используем Hashtbl.hash
     (быстрее), с backend'ом — JSON-сериализованный ключ (детерминирован,
     корректно работает с restore). *)
  let key_to_string (key : k) : string =
    match backend, spec.s_serialize_key with
    | Some _, Some sk -> Yojson.Safe.to_string (sk key)
    | _ -> string_of_int (Hashtbl.hash key)
  in

  (* ════════════════════════════════════════════════════════════════
     PERSISTENCE LAYER (Step 2)

     Сериализация state-машины в JSON, запись в backend на каждом
     изменении state. Реализована только если [backend] передан;
     иначе persist_state и restore_all — no-op.

     Формат backend-ключа:
       "trigger:{spec.name}:" ^ Yojson.to_string (serialize_key user_key)

     Формат значения (JSON в bytes):
       {
         "key": <serialized 'k>,
         "state": <tagged state>,
         "last_event_ts": int
       }

     Tagged state:
       {"tag":"ok"}
       {"tag":"pending_problem", "since":int, "last_value":<v>, "fire_at":int}
       {"tag":"problem", "since":int, "last_alert":<a>, "last_alert_ts":int}
       {"tag":"pending_ok", "problem_since":int, "last_alert":<a>,
        "last_alert_ts":int, "recovery_since":int, "fire_at":int}
     ════════════════════════════════════════════════════════════════ *)

  let key_prefix = "trigger:" ^ spec.s_name ^ ":" in

  (* Безопасные дёрнут-сериализаторы. Вызываются только когда backend
     подключён, т.е. сериализаторы заведомо Some после валидации. *)
  let ser_v v =
    match spec.s_serialize_value with
    | Some f -> f v
    | None -> invalid_arg "Trigger: (de)serializer missing — backend set but spec lacks it (invariant violated)"
  in
  let ser_a a =
    match spec.s_serialize_alert with
    | Some f -> f a
    | None -> invalid_arg "Trigger: (de)serializer missing — backend set but spec lacks it (invariant violated)"
  in
  let ser_k k =
    match spec.s_serialize_key with
    | Some f -> f k
    | None -> invalid_arg "Trigger: (de)serializer missing — backend set but spec lacks it (invariant violated)"
  in

  let state_to_json (s : (k, v, a) state) : Yojson.Safe.t =
    match s with
    | S_ok ->
      `Assoc [("tag", `String "ok")]
    | S_pending_problem { since; last_value; fire_at } ->
      `Assoc [
        ("tag",        `String "pending_problem");
        ("since",      `Int since);
        ("last_value", ser_v last_value);
        ("fire_at",    `Int fire_at);
      ]
    | S_problem { since; last_alert; last_alert_ts } ->
      `Assoc [
        ("tag",            `String "problem");
        ("since",          `Int since);
        ("last_alert",     ser_a last_alert);
        ("last_alert_ts",  `Int last_alert_ts);
      ]
    | S_pending_ok { problem_since; last_alert; last_alert_ts;
                     recovery_since; fire_at } ->
      `Assoc [
        ("tag",             `String "pending_ok");
        ("problem_since",   `Int problem_since);
        ("last_alert",      ser_a last_alert);
        ("last_alert_ts",   `Int last_alert_ts);
        ("recovery_since",  `Int recovery_since);
        ("fire_at",         `Int fire_at);
      ]
  in

  let backend_key_for_user_key (key : k) : string =
    key_prefix ^ Yojson.Safe.to_string (ser_k key)
  in

  let persist_state (ks : string) (key : k) (st : (k, v, a) state) : unit =
    match backend with
    | None -> ()
    | Some be ->
      let last_ts =
        match Hashtbl.find_opt last_event_ts ks with
        | Some t -> t | None -> 0
      in
      let json =
        `Assoc [
          ("key",            ser_k key);
          ("state",          state_to_json st);
          ("last_event_ts",  `Int last_ts);
        ] in
      let bk = backend_key_for_user_key key in
      be.set bk (Bytes.of_string (Yojson.Safe.to_string json))
  in

  (* Десериализация одного state-значения из JSON. *)
  let deser_v j =
    match spec.s_deserialize_value with
    | Some f -> f j
    | None -> invalid_arg "Trigger: (de)serializer missing — backend set but spec lacks it (invariant violated)"
  in
  let deser_a j =
    match spec.s_deserialize_alert with
    | Some f -> f j
    | None -> invalid_arg "Trigger: (de)serializer missing — backend set but spec lacks it (invariant violated)"
  in
  let deser_k j =
    match spec.s_deserialize_key with
    | Some f -> f j
    | None -> invalid_arg "Trigger: (de)serializer missing — backend set but spec lacks it (invariant violated)"
  in
  let state_of_json (j : Yojson.Safe.t) : (k, v, a) state =
    match j with
    | `Assoc kv ->
      let tag = Yojson.Safe.Util.to_string (List.assoc "tag" kv) in
      (match tag with
       | "ok" -> S_ok
       | "pending_problem" ->
         let since      = Yojson.Safe.Util.to_int   (List.assoc "since" kv) in
         let last_value = deser_v (List.assoc "last_value" kv) in
         let fire_at    = Yojson.Safe.Util.to_int   (List.assoc "fire_at" kv) in
         S_pending_problem { since; last_value; fire_at }
       | "problem" ->
         let since         = Yojson.Safe.Util.to_int (List.assoc "since" kv) in
         let last_alert    = deser_a (List.assoc "last_alert" kv) in
         let last_alert_ts = Yojson.Safe.Util.to_int (List.assoc "last_alert_ts" kv) in
         S_problem { since; last_alert; last_alert_ts }
       | "pending_ok" ->
         let problem_since   = Yojson.Safe.Util.to_int (List.assoc "problem_since" kv) in
         let last_alert      = deser_a (List.assoc "last_alert" kv) in
         let last_alert_ts   = Yojson.Safe.Util.to_int (List.assoc "last_alert_ts" kv) in
         let recovery_since  = Yojson.Safe.Util.to_int (List.assoc "recovery_since" kv) in
         let fire_at         = Yojson.Safe.Util.to_int (List.assoc "fire_at" kv) in
         S_pending_ok { problem_since; last_alert; last_alert_ts;
                        recovery_since; fire_at }
       | other -> failwith ("Trigger.restore: unknown state tag: " ^ other))
    | _ -> failwith "Trigger.restore: state JSON not object"
  in

  (* На старте, если backend подключён, восстанавливаем все per-key
     state из его записей. Заодно re-register'им pending timers
     (они derive'ятся из Pending_* состояний). *)
  let restore_all () =
    match backend with
    | None -> ()
    | Some be ->
      let all_keys = be.keys () in
      List.iter (fun bk ->
        (* Фильтр по нашему префиксу — backend может содержать ключи
           других триггеров на той же backend-таблице. *)
        let plen = String.length key_prefix in
        if String.length bk >= plen
           && String.sub bk 0 plen = key_prefix
        then begin
          match be.get bk with
          | None -> ()  (* race с delete? игнорируем *)
          | Some v_bytes ->
            (try
               let json = Yojson.Safe.from_string (Bytes.to_string v_bytes) in
               match json with
               | `Assoc kv ->
                 let key       = deser_k (List.assoc "key" kv) in
                 let st        = state_of_json (List.assoc "state" kv) in
                 let last_ts   = Yojson.Safe.Util.to_int (List.assoc "last_event_ts" kv) in
                 let ks        = key_to_string key in
                 Hashtbl.replace states ks (st, key);
                 if last_ts > 0 then
                   Hashtbl.replace last_event_ts ks last_ts;
                 (* Pending state'ы → восстанавливаем timer *)
                 (match st with
                  | S_pending_problem { fire_at; _ }
                  | S_pending_ok { fire_at; _ } ->
                    Timers.insert timers
                      { t_fire_at = fire_at; t_key = ks }
                  | _ -> ())
               | _ -> failwith "Trigger.restore: top-level JSON not object"
             with
             | Yojson.Json_error msg ->
               failwith ("Trigger.restore: invalid JSON in backend (key=" ^ bk ^ "): " ^ msg)
             | Not_found ->
               failwith ("Trigger.restore: missing field in backend record (key=" ^ bk ^ ")"))
        end
      ) all_keys
  in
  restore_all ();

  let get_state ks =
    match Hashtbl.find_opt states ks with
    | Some (s, _) -> s
    | None -> S_ok
  in
  let set_state ks key st =
    Hashtbl.replace states ks (st, key);
    (* На каждое изменение state — пишем snapshot в backend (если есть).
       persist_state — no-op без backend'а. *)
    persist_state ks key st
  in
  let get_key ks =
    match Hashtbl.find_opt states ks with
    | Some (_, k) -> Some k
    | None -> None
  in

  (* Обработка Data ((key, value), ts).
     Out-of-order events (ts < last_event_ts[key]) игнорируются —
     иначе опоздавший пакет с устаревшим значением мог бы ложно
     перевести триггер в recovery. Триггер — strictly forward-in-time. *)
  let on_data (key : k) (value : v) (ts : Time.t) =
    let ks = key_to_string key in
    let last = try Hashtbl.find last_event_ts ks with Not_found -> min_int in
    if ts < last then () else begin
    Hashtbl.replace last_event_ts ks ts;
    let st = get_state ks in
    match st with
    | S_ok ->
      if spec.s_condition.check value then begin
        if spec.s_problem_for = 0 then begin
          (* мгновенный переход в Problem *)
          let alert = spec.s_produce_alert ~key ~value ~ts in
          Queue.push (Mf_event.data alert ts) out_buf;
          set_state ks key (S_problem {
            since = ts; last_alert = alert; last_alert_ts = ts;
          })
        end else begin
          let fire_at = ts + spec.s_problem_for in
          set_state ks key (S_pending_problem {
            since = ts; last_value = value; fire_at;
          });
          Timers.insert timers { t_fire_at = fire_at; t_key = ks }
        end
      end
    | S_pending_problem { since; fire_at; _ } ->
      if spec.s_condition.check value then begin
        (* всё ещё в проблемной зоне — обновим last_value, debounce не сбрасываем *)
        set_state ks key (S_pending_problem {
          since; last_value = value; fire_at;
        })
      end else begin
        (* вышли из проблемной зоны до дозревания — откатываемся *)
        Timers.remove_key timers ~key:ks;
        set_state ks key S_ok
      end
    | S_problem { since; last_alert; last_alert_ts } ->
      if spec.s_condition.recovery value then begin
        if spec.s_recovery_for = 0 then begin
          (* мгновенный recovery *)
          Queue.push (Mf_event.retract last_alert last_alert_ts) out_buf;
          let rec_alert = spec.s_produce_recovery ~key ~ts in
          Queue.push (Mf_event.data rec_alert ts) out_buf;
          set_state ks key S_ok
        end else begin
          let fire_at = ts + spec.s_recovery_for in
          set_state ks key (S_pending_ok {
            problem_since = since;
            last_alert; last_alert_ts;
            recovery_since = ts; fire_at;
          });
          Timers.insert timers { t_fire_at = fire_at; t_key = ks }
        end
      end
      (* иначе остаёмся в Problem без эмиссии (sticky); refresh-внутри-Problem
         не делаем по дизайну. Если check всё ещё true — это норма. *)
    | S_pending_ok { problem_since; last_alert; last_alert_ts; fire_at; _ } ->
      if spec.s_condition.check value then begin
        (* откат — снова Problem *)
        Timers.remove_key timers ~key:ks;
        set_state ks key (S_problem {
          since = problem_since; last_alert; last_alert_ts;
        })
      end
      (* иначе остаёмся в Pending_ok, debounce не сбрасываем *)
      else if spec.s_condition.recovery value then
        set_state ks key (S_pending_ok {
          problem_since; last_alert; last_alert_ts;
          recovery_since = ts; fire_at;
        })
    end
  in

  (* Обработка таймера: переход из Pending_* в финальное состояние.
     Вызывается когда watermark достиг t_fire_at. *)
  let on_timer (pt : pending_timer) =
    let ks = pt.t_key in
    let st = get_state ks in
    match st, get_key ks with
    | S_pending_problem { since; last_value; fire_at }, Some key
      when fire_at = pt.t_fire_at ->
      let alert = spec.s_produce_alert ~key ~value:last_value ~ts:fire_at in
      Queue.push (Mf_event.data alert fire_at) out_buf;
      set_state ks key (S_problem {
        since; last_alert = alert; last_alert_ts = fire_at;
      })
    | S_pending_ok { last_alert; last_alert_ts; fire_at; _ }, Some key
      when fire_at = pt.t_fire_at ->
      Queue.push (Mf_event.retract last_alert last_alert_ts) out_buf;
      let rec_alert = spec.s_produce_recovery ~key ~ts:fire_at in
      Queue.push (Mf_event.data rec_alert fire_at) out_buf;
      set_state ks key S_ok
    | _ -> ()
    (* Stale timer (state changed между регистрацией и срабатыванием) —
       просто игнорируем. *)
  in

  let on_watermark wm =
    let due = Timers.pop_due timers ~wm in
    List.iter on_timer due
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
          (* Сначала эмитим накопленные таймером события, потом WM —
             добавляем WM в конец очереди. *)
          Queue.push (Mf_event.wm wm) out_buf;
          Some (Queue.pop out_buf)
        end
      | Some (Mf_event.Retract _) ->
        (* Retract в upstream item-потоке игнорируем — триггер
           реагирует только на свежие Data. *)
        next ()
      | Some (Mf_event.Update { new_value = (key, value); ts; _ }) ->
        (* Update — атомарная коррекция. Триггер тогда reactes на
           new value: если new value пересекает порог — triggers fire
           normally. Это correct behavior для debounce/recovery:
           Update появляется как 'свежее значение' после коррекции. *)
        on_data key value ts;
        next ()
      | Some (Mf_event.Data ((key, value), ts)) ->
        on_data key value ts;
        next ()
  in
  next

(* ────────────────────────────────────────────────────────────────────
   combine: несколько триггеров на одном потоке
   ──────────────────────────────────────────────────────────────────── *)

let combine
    (type k v a)
    ?(backend : backend option)
    (specs : (k, v, a) spec list)
    (source : (k * v) Mf_event.t Stream.t)
  : a Mf_event.t Stream.t =
  (* Multi-trigger pattern: каждый триггер получает свою копию
     source через Stream.split (lazy buffered fan-out), затем
     результаты сливаются через Mf_event.union.

     Stream.split НЕ материализует source — он буферизует только
     те events которые уже прочитаны одной копией но ещё не
     прочитаны другой. Это даёт O(buffer_size) памяти вместо
     O(total_stream_size), и работает на бесконечных источниках.

     Если backend подключён — передаётся каждому триггеру; они
     различаются по [name] и не интерферируют в backend по ключам. *)
  match specs with
  | [] -> Stream.empty
  | [s] -> of_stream ?backend s source
  | _ ->
    let copies = Stream.split (List.length specs) source in
    let trigger_streams = List.map2
      (fun spec copy -> of_stream ?backend spec copy)
      specs copies in
    match trigger_streams with
    | [] -> Stream.empty  (* unreachable: specs non-empty *)
    | first :: rest ->
      List.fold_left Mf_event.union first rest
