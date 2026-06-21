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
   Persistence — ортогональная, через Runtime_context + Managed_state.
   Триггер больше не объявляет свой backend-тип и не носит
   сериализаторы: состояние (closure-free вариант) Marshal'ится
   автоматически.
   ──────────────────────────────────────────────────────────────────── *)

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
}

let create
    ~name
    ~condition
    ?(problem_for  = 0)
    ?(recovery_for = 0)
    ?(severity     = Not_classified)
    ~produce_alert
    ~produce_recovery
    () =
  {
    s_name              = name;
    s_condition         = condition;
    s_problem_for       = problem_for;
    s_recovery_for      = recovery_for;
    s_severity          = severity;
    s_produce_alert     = produce_alert;
    s_produce_recovery  = produce_recovery;
  }

let name     s = s.s_name
let severity s = s.s_severity

(* ── Record-based config alternative API ───────────────────────────
   Backwards-compat: оригинальный Trigger.create остаётся; of_config
   просто разворачивает record в вызов create. *)

type ('key, 'v, 'alert) config = {
  name             : string;
  condition        : 'v condition;
  produce_alert    : key:'key -> value:'v -> ts:Time.t -> 'alert;
  produce_recovery : key:'key -> ts:Time.t -> 'alert;
  problem_for      : Time.t;
  recovery_for     : Time.t;
  severity         : severity;
}

let default_config ~name ~condition ~produce_alert ~produce_recovery = {
  name; condition; produce_alert; produce_recovery;
  problem_for  = 0;
  recovery_for = 0;
  severity     = Warning;
}

let of_config (cfg : ('key, 'v, 'alert) config) : ('key, 'v, 'alert) spec =
  create
    ~name:cfg.name
    ~condition:cfg.condition
    ~problem_for:cfg.problem_for
    ~recovery_for:cfg.recovery_for
    ~severity:cfg.severity
    ~produce_alert:cfg.produce_alert
    ~produce_recovery:cfg.produce_recovery
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
    (spec : (k, v, a) spec)
    (source : (k * v) Mf_event.t Stream.t)
  : a Mf_event.t Stream.t =

  let states : (string, (k, v, a) state * k) Hashtbl.t = Hashtbl.create 64 in
  let last_event_ts : (string, Time.t) Hashtbl.t = Hashtbl.create 64 in
  let timers = Timers.create () in
  let out_buf : a Mf_event.t Queue.t = Queue.create () in

  let key_to_string (key : k) : string = string_of_int (Hashtbl.hash key) in

  (* ════════════════════════════════════════════════════════════════
     PERSISTENCE — ортогональная, через Managed_state.

     Persistence решается ambient Runtime_context, не параметром.
     Рабочие структуры (states + last_event_ts + timers) остаются для
     скорости; managed-state — durable-зеркало. Значение per-ключ
     упаковывает (state, исходный ключ, last_event_ts). state —
     closure-free вариант, Marshal'ится автоматически, поэтому 6
     сериализаторов больше не нужны. Таймеры derive'ятся при restore
     из Pending-состояний.
     ════════════════════════════════════════════════════════════════ *)
  let mstate : (string, (k, v, a) state * k * Time.t) Managed_state.t =
    Managed_state.create_string ~name:("trigger:" ^ spec.s_name) () in

  let persist_state (ks : string) (key : k) (st : (k, v, a) state) : unit =
    let last_ts = match Hashtbl.find_opt last_event_ts ks with
      | Some t -> t | None -> 0 in
    Managed_state.set mstate ks (st, key, last_ts);
    Managed_state.checkpoint_key mstate ks
  in

  (* Восстановление per-key state из managed-state на старте.
     Заодно re-register'им pending timers из Pending-состояний. *)
  let restore_all () =
    Managed_state.iter mstate (fun ks (st, key, last_ts) ->
      Hashtbl.replace states ks (st, key);
      if last_ts > 0 then Hashtbl.replace last_event_ts ks last_ts;
      (match st with
       | S_pending_problem { fire_at; _ }
       | S_pending_ok { fire_at; _ } ->
         Timers.insert timers { t_fire_at = fire_at; t_key = ks }
       | _ -> ()))
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

     Триггеры различаются по [name] и не интерферируют в backend по
     ключам (managed-state namespace = "trigger:{name}"). *)
  match specs with
  | [] -> Stream.empty
  | [s] -> of_stream s source
  | _ ->
    let copies = Stream.split (List.length specs) source in
    let trigger_streams = List.map2
      (fun spec copy -> of_stream spec copy)
      specs copies in
    match trigger_streams with
    | [] -> Stream.empty  (* unreachable: specs non-empty *)
    | first :: rest ->
      List.fold_left Mf_event.union first rest
