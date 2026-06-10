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
  set_event_timer         : Time.t -> unit;
  set_processing_timer    : Time.t -> unit;
  cancel_event_timer      : Time.t -> unit;
  cancel_event_timers     : unit -> unit;      (* снять все event-таймеры этого ключа *)
  cancel_processing_timer : Time.t -> unit;
  cancel_processing_timers: unit -> unit;
}

(* таймер: (ключ, время, тип) *)
module TimerSet = Set.Make (struct
  type t = string * Time.t
  let compare = compare
end)

let process_keyed
    (type a) (type st) (type out)
    (module K : Keyed.S with type t = a)
    ?(now_ms = fun () -> int_of_float (Unix.gettimeofday () *. 1000.))
    ~(init : unit -> st)
    ~(on_event : out ctx -> string -> st -> a -> unit)
    ~(on_timer : out ctx -> string -> st -> Time.t -> timer_kind -> unit)
    (upstream : a Mf_event.t Stream.t)
    : out Mf_event.t Stream.t =
  let states : (string, st) Hashtbl.t = Hashtbl.create 64 in
  let ev_timers = ref TimerSet.empty in   (* (key, time) event-time *)
  let pt_timers = ref TimerSet.empty in   (* (key, time) processing-time *)
  let out_q : out Mf_event.t Queue.t = Queue.create () in

  let state_of key =
    match Hashtbl.find_opt states key with
    | Some s -> s
    | None -> let s = init () in Hashtbl.replace states key s; s in

  (* контекст для конкретного ключа *)
  let ctx_for key ~emit_ts = {
    clear_state = (fun () -> Hashtbl.remove states key);
    emit = (fun o -> Queue.push (Mf_event.data o emit_ts) out_q);
    set_event_timer = (fun t -> ev_timers := TimerSet.add (key, t) !ev_timers);
    set_processing_timer = (fun t -> pt_timers := TimerSet.add (key, t) !pt_timers);
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
    (* по возрастанию времени (TimerSet уже отсортирован по (key,time);
       пересортируем по времени для корректного порядка срабатывания) *)
    TimerSet.elements due
    |> List.sort (fun (_,t1) (_,t2) -> compare t1 t2)
    |> List.iter (fun (key, t) ->
         on_timer (ctx_for key ~emit_ts:t) key (state_of key) t kind) in

  let upstream_done = ref false in

  fun () ->
    let rec pull () =
      if not (Queue.is_empty out_q) then Some (Queue.pop out_q)
      else if !upstream_done then None
      else match upstream () with
        | None ->
          upstream_done := true;
          (* Конец потока НЕ срабатывает таймеры автоматически: event-time
             таймер ждёт watermark, processing-time — wall-clock. Если
             нужно «дренировать» таймеры на завершении, пошлите финальный
             watermark (max_int) перед концом. *)
          None
        | Some (Mf_event.Data (v, ts)) ->
          let key = K.key v in
          on_event (ctx_for key ~emit_ts:ts) key (state_of key) v;
          (* при активности проверяем processing-time таймеры по wall-clock *)
          fire_due pt_timers Processing_time (now_ms ());
          pull ()
        | Some (Mf_event.Watermark wm) ->
          (* watermark двигает event-time таймеры *)
          fire_due ev_timers Event_time wm;
          (* и заодно проверяем processing-time *)
          fire_due pt_timers Processing_time (now_ms ());
          Queue.push (Mf_event.wm wm) out_q;
          pull ()
        | Some (Mf_event.Retract _) -> pull ()   (* retract во вход не транслируется *)
    in pull ()
