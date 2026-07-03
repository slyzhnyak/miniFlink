(* ProcessFunction с таймерами — низкоуровневый оператор с keyed-состоянием
   и таймерами (как ProcessFunction/KeyedProcessFunction во Flink).

   Два типа таймеров (оба per-key):
   - Event_time: срабатывает когда watermark >= времени таймера. Детерминирован,
     движется по данным. Для «нет событий дольше порога» по event-time.
   - Processing_time: срабатывает когда wall-clock >= времени таймера.
     Для «дольше смены» в реальном времени, даже если данные молчат.

   Обработчики:
   - on_event ctx key state event: на каждое событие ключа. Может менять
     состояние, ставить/снимать таймеры, эмитить.
   - on_timer ctx key state time kind: когда таймер сработал.

   ОГРАНИЧЕНИЕ pull-модели: таймеры проверяются при ПРОХОЖДЕНИИ событий и
   watermark-ов через оператор (нет фонового потока). Processing-time
   таймер сработает при следующей активности потока после истечения, не
   мгновенно. Если поток полностью молчит — используйте idle-watermark
   (Mf_event.with_idle_watermarks), чтобы watermark и проверки двигались. *)

type timer_kind = Event_time | Processing_time

type 'out ctx = {
  clear_state             : unit -> unit;   (* удалить состояние текущего ключа *)
  emit                    : 'out -> unit;
  emit_retract            : 'out -> unit;
    (* эмитить Retract значения (отзыв ранее выпущенного вывода) с тем же
       временем, что и emit. Открывает retract-семантику keyed-логике —
       раньше это приходилось делать в обход process_keyed. *)
  emit_update             : old:'out -> 'out -> unit;
    (* эмитить атомарный Update old→new: downstream видит коррекцию за
       один шаг, без промежуточного «вывод исчез» (None-flicker). *)
  emit_event              : 'out Mf_event.t -> unit;
    (* эмитить уже готовое событие (Data/Retract/Update/Watermark) с его
       собственным временем. Нужно, когда доменная функция возвращает
       список Mf_event.t — избавляет от разбора вида для выбора
       emit/emit_retract/emit_update. *)
  set_event_timer         : Time.t -> unit;
  set_event_timer_for     : string -> Time.t -> unit;
  set_processing_timer    : Time.t -> unit;
  cancel_event_timer      : Time.t -> unit;
  cancel_event_timers     : unit -> unit;      (* снять все event-таймеры этого ключа *)
  cancel_processing_timer : Time.t -> unit;
  cancel_processing_timers: unit -> unit;
}

(* ── Single_timer — один логический event-таймер на ключ с переносом
   цели на ближайший дедлайн (закрытие разрыва G-3).

   Частый паттерн keyed-FSM: у ключа один «следующий момент проверки»,
   который по мере прихода событий сдвигается. Наивно ставить новый
   event-таймер на каждое событие → десятки таймеров на ключ, а сработает
   лишний. Single_timer держит ОДНУ цель: reschedule снимает предыдущий
   таймер (если цель поменялась) и ставит новый; идемпотентен по target.
   consumed вызывается в on_timer, когда таймер отработал. Вынесено из
   ручного кода ex07 (Layer 2). *)
module Single_timer = struct
  type t = { mutable target : Time.t option }
  let make () = { target = None }

  (* Поставить event-таймер на [target], сняв предыдущий если он был на
     другое время. Идемпотентно по [target] (повторный вызов на то же
     время — no-op). *)
  let reschedule t (ctx : 'out ctx) ~target =
    match t.target with
    | Some old when old = target -> ()  (* та же цель — no-op (идемпотентно) *)
    | Some old ->
      ctx.cancel_event_timer old;        (* цель сдвинулась — снять старый *)
      t.target <- Some target;
      ctx.set_event_timer target
    | None ->
      t.target <- Some target;
      ctx.set_event_timer target

  (* Отметить, что таймер отработал (вызывать в on_timer). После этого
     reschedule на новую цель поставит свежий таймер. *)
  let consumed t = t.target <- None

  (* Текущая цель, если есть. *)
  let target t = t.target
end

(* таймер: (ключ, время, тип) *)
(* событие телеметрии оператора — для подключения внешних метрик *)
type stat =
  [ `Event_timer_set | `Event_timer_fired
  | `Processing_timer_set | `Processing_timer_fired
  | `Watermark_seen ]

module TimerSet = Set.Make (struct
  type t = string * Time.t
  let compare = compare
end)

let process_keyed
    (type a) (type st) (type out)
    (module K : Keyed.S with type t = a)
    ?(now_ms = fun () -> int_of_float (Unix.gettimeofday () *. 1000.))
    ?(on_stat : stat -> unit = fun _ -> ())
    ?(name = "default")
    ?(on_update : (out ctx -> string -> st -> old:a -> new_value:a -> unit) option)
    ?(on_retract : (out ctx -> string -> st -> a -> unit) option)
    ~(init : unit -> st)
    ~(on_event : out ctx -> string -> st -> a -> unit)
    ~(on_timer : out ctx -> string -> st -> Time.t -> timer_kind -> unit)
    (upstream : a Mf_event.t Stream.t)
    : out Mf_event.t Stream.t =

  let states : (string, st) Hashtbl.t = Hashtbl.create 64 in
  let ev_timers = ref TimerSet.empty in   (* (key, time) event-time *)
  let pt_timers = ref TimerSet.empty in   (* (key, time) processing-time *)
  let out_q : out Mf_event.t Queue.t = Queue.create () in
  let wm_seen = ref false in
  let ev_set = ref 0 and ev_fired = ref 0 in
  let stat e = on_stat e in

  (* ════════════════════════════════════════════════════════════════
     PERSISTENCE — ортогональная, через Managed_state.

     Persistence решается ambient Runtime_context, не параметром.
     Рабочие структуры (states + ev/pt_timers) остаются для быстрого
     доступа; managed-state — durable-зеркало. Значение per-ключ
     упаковывает состояние и таймеры этого ключа.
     ════════════════════════════════════════════════════════════════ *)
  let mstate : (string, st option * int list * int list) Managed_state.t =
    Managed_state.create_string ~name:("process_keyed:" ^ name) () in

  let timers_int_for_key timers key =
    TimerSet.fold (fun (k, t) acc -> if k = key then t :: acc else acc)
      !timers []
  in

  (* persist_key (ks) — собрать состояние+таймеры ключа в managed-state
     и точечно сcheckpoint'ить. В ephemeral checkpoint_key — noop. *)
  let persist_key (ks : string) =
    let st_opt = Hashtbl.find_opt states ks in
    let evs = timers_int_for_key ev_timers ks in
    let pts = timers_int_for_key pt_timers ks in
    (match st_opt with
     | None when evs = [] && pts = [] -> Managed_state.remove mstate ks
     | _ -> Managed_state.set mstate ks (st_opt, evs, pts));
    Managed_state.checkpoint_key mstate ks
  in

  (* Восстановление при старте: managed-state уже загрузил записи из
     backend (если durable), раскладываем их в рабочие структуры. *)
  let restore_all () =
    Managed_state.iter mstate (fun ks (st_opt, evs, pts) ->
      (match st_opt with Some s -> Hashtbl.replace states ks s | None -> ());
      List.iter (fun t -> ev_timers := TimerSet.add (ks, t) !ev_timers) evs;
      List.iter (fun t -> pt_timers := TimerSet.add (ks, t) !pt_timers) pts)
  in
  restore_all ();

  let state_of key =
    match Hashtbl.find_opt states key with
    | Some s -> s
    | None -> let s = init () in Hashtbl.replace states key s; s in

  (* контекст для конкретного ключа *)
  let ctx_for key ~emit_ts = {
    clear_state = (fun () -> Hashtbl.remove states key);
    emit = (fun o -> Queue.push (Mf_event.data o emit_ts) out_q);
    emit_retract = (fun o -> Queue.push (Mf_event.retract o emit_ts) out_q);
    emit_update = (fun ~old o -> Queue.push (Mf_event.update old o emit_ts) out_q);
    emit_event = (fun ev -> Queue.push ev out_q);
    set_event_timer = (fun t ->
      incr ev_set; stat `Event_timer_set;
      ev_timers := TimerSet.add (key, t) !ev_timers);
    set_event_timer_for = (fun k t ->
      incr ev_set; stat `Event_timer_set;
      ev_timers := TimerSet.add (k, t) !ev_timers);
    set_processing_timer = (fun t ->
      stat `Processing_timer_set;
      pt_timers := TimerSet.add (key, t) !pt_timers);
    cancel_event_timer = (fun t -> ev_timers := TimerSet.remove (key, t) !ev_timers);
    cancel_event_timers = (fun () ->
      ev_timers := TimerSet.filter (fun (k,_) -> k <> key) !ev_timers);
    cancel_processing_timer = (fun t -> pt_timers := TimerSet.remove (key, t) !pt_timers);
    cancel_processing_timers = (fun () ->
      pt_timers := TimerSet.filter (fun (k,_) -> k <> key) !pt_timers);
  } in

  (* сработавшие таймеры с time <= threshold, по возрастанию времени *)
  let fire_due timers kind threshold =
    let due, rest = TimerSet.partition (fun (_, t) -> t <= threshold) !timers in
    timers := rest;
    TimerSet.elements due
    |> List.sort (fun (_,t1) (_,t2) -> compare t1 t2)
    |> List.iter (fun (key, t) ->
         (match kind with
          | Event_time -> incr ev_fired; stat `Event_timer_fired
          | Processing_time -> stat `Processing_timer_fired);
         on_timer (ctx_for key ~emit_ts:t) key (state_of key) t kind;
         persist_key key) in

  let upstream_done = ref false in

  fun () ->
    let rec pull () =
      if not (Queue.is_empty out_q) then Some (Queue.pop out_q)
      else if !upstream_done then None
      else match upstream () with
        | None ->
          upstream_done := true;
          if not !wm_seen && not (TimerSet.is_empty !ev_timers) then
            Log.warn ~fields:[
              ("event_timers_set", string_of_int !ev_set);
              ("event_timers_fired", string_of_int !ev_fired);
              ("pending_event_timers", string_of_int (TimerSet.cardinal !ev_timers))]
              "process_keyed: event-таймеры установлены, но в потоке не было ни                одного watermark — они никогда не сработают. Добавьте                Pipe.event_time перед process_keyed (или idle-watermark для                молчаливых источников)";
          None
        | Some (Mf_event.Data (v, ts)) ->
          let key = K.key v in
          on_event (ctx_for key ~emit_ts:ts) key (state_of key) v;
          persist_key key;
          (* при активности проверяем processing-time таймеры по wall-clock *)
          fire_due pt_timers Processing_time (now_ms ());
          pull ()
        | Some (Mf_event.Watermark wm) ->
          wm_seen := true; stat `Watermark_seen;
          fire_due ev_timers Event_time wm;
          fire_due pt_timers Processing_time (now_ms ());
          Queue.push (Mf_event.wm wm) out_q;
          pull ()
        | Some (Mf_event.Retract (v, ts)) ->
          (* По умолчанию Retract отбрасывается (исторически). Если
             передан ?on_retract — пользователь реагирует на отзыв
             значения (например, удаляет его из своего состояния),
             симметрично ?on_update. *)
          (match on_retract with
           | Some cb ->
             let key = K.key v in
             cb (ctx_for key ~emit_ts:ts) key (state_of key) v;
             persist_key key;
             fire_due pt_timers Processing_time (now_ms ())
           | None -> ());
          pull ()
        | Some (Mf_event.Update { old; new_value; ts }) ->
          (* Phase 3.3: если ?on_update передан — atomic native handling.
             Иначе Phase 1 conservative fallback: обработать Update
             как Data на new_value. Оба пути обновляют state через
             колбэк пользователя. *)
          let key = K.key new_value in
          let ctx = ctx_for key ~emit_ts:ts in
          let st = state_of key in
          (match on_update with
           | Some cb -> cb ctx key st ~old ~new_value
           | None -> on_event ctx key st new_value);
          persist_key key;
          fire_due pt_timers Processing_time (now_ms ());
          pull ()
    in pull ()

(* ── Record-based spec API — backwards-compat wrapper ──────────────── *)

type ('a, 'st, 'out) spec = {
  keyed       : (module Keyed.S with type t = 'a);
  init        : unit -> 'st;
  on_event    : 'out ctx -> string -> 'st -> 'a -> unit;
  on_timer    : 'out ctx -> string -> 'st -> Time.t -> timer_kind -> unit;
  now_ms      : (unit -> int) option;
  on_stat     : (stat -> unit) option;
  name        : string option;
}

let default_spec ~keyed ~init ~on_event ~on_timer = {
  keyed; init; on_event; on_timer;
  now_ms = None; on_stat = None; name = None;
}

let process_keyed_spec (spec : ('a, 'st, 'out) spec)
    (upstream : 'a Mf_event.t Stream.t) : 'out Mf_event.t Stream.t =
  process_keyed spec.keyed
    ?now_ms:spec.now_ms
    ?on_stat:spec.on_stat
    ?name:spec.name
    ~init:spec.init
    ~on_event:spec.on_event
    ~on_timer:spec.on_timer
    upstream
