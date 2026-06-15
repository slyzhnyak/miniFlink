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
  set_event_timer_for     : string -> Time.t -> unit;
  set_processing_timer    : Time.t -> unit;
  cancel_event_timer      : Time.t -> unit;
  cancel_event_timers     : unit -> unit;      (* снять все event-таймеры этого ключа *)
  cancel_processing_timer : Time.t -> unit;
  cancel_processing_timers: unit -> unit;
}

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
    ?(backend : Persistence_backend.t option)
    ?(backend_name : string option)
    ?(serialize_state : (st -> Yojson.Safe.t) option)
    ?(deserialize_state : (Yojson.Safe.t -> st) option)
    ~(init : unit -> st)
    ~(on_event : out ctx -> string -> st -> a -> unit)
    ~(on_timer : out ctx -> string -> st -> Time.t -> timer_kind -> unit)
    (upstream : a Mf_event.t Stream.t)
    : out Mf_event.t Stream.t =

  (* Если backend подключён — обязательны name + сериализаторы. *)
  (match backend with
   | None -> ()
   | Some _ ->
     let missing =
       (if backend_name      = None then ["backend_name"]      else []) @
       (if serialize_state   = None then ["serialize_state"]   else []) @
       (if deserialize_state = None then ["deserialize_state"] else [])
     in
     if missing <> [] then
       invalid_arg (Printf.sprintf
         "Process_fn.process_keyed: backend provided but missing: %s"
         (String.concat ", " missing)));

  let states : (string, st) Hashtbl.t = Hashtbl.create 64 in
  let ev_timers = ref TimerSet.empty in   (* (key, time) event-time *)
  let pt_timers = ref TimerSet.empty in   (* (key, time) processing-time *)
  let out_q : out Mf_event.t Queue.t = Queue.create () in
  let wm_seen = ref false in
  let ev_set = ref 0 and ev_fired = ref 0 in
  let stat e = on_stat e in

  (* ════════════════════════════════════════════════════════════════
     PERSISTENCE LAYER

     Backend-ключ:
       "process_keyed:{backend_name}:{key}"

     Значение (JSON):
       {
         "state":     <serialized 'st>,
         "ev_timers": [t1, t2, ...],
         "pt_timers": [t1, t2, ...]
       }

     persist_key (ks) — записать всё, что относится к ключу ks:
     - текущее состояние из states
     - таймеры из ev_timers / pt_timers с этим ключом
     ════════════════════════════════════════════════════════════════ *)
  let key_prefix =
    match backend_name with
    | Some n -> "process_keyed:" ^ n ^ ":"
    | None -> ""
  in

  let ser_st s =
    match serialize_state with
    | Some f -> f s
    | None -> assert false
  in
  let deser_st j =
    match deserialize_state with
    | Some f -> f j
    | None -> assert false
  in

  let timers_for_key timers key =
    TimerSet.fold (fun (k, t) acc ->
      if k = key then `Int t :: acc else acc
    ) !timers []
  in

  let persist_key (ks : string) =
    match backend with
    | None -> ()
    | Some be ->
      (* Если state отсутствует и таймеров нет — удаляем запись. *)
      let st_opt = Hashtbl.find_opt states ks in
      let evs = timers_for_key ev_timers ks in
      let pts = timers_for_key pt_timers ks in
      let bk = key_prefix ^ ks in
      (match st_opt with
       | None when evs = [] && pts = [] ->
         be.delete bk
       | _ ->
         let state_json = match st_opt with
           | Some s -> ser_st s
           | None -> `Null in
         let json = `Assoc [
           ("state",     state_json);
           ("ev_timers", `List evs);
           ("pt_timers", `List pts);
         ] in
         be.set bk (Bytes.of_string (Yojson.Safe.to_string json)))
  in

  (* Восстановление при старте. *)
  let restore_all () =
    match backend with
    | None -> ()
    | Some be ->
      let plen = String.length key_prefix in
      List.iter (fun bk ->
        if String.length bk >= plen
           && String.sub bk 0 plen = key_prefix
        then begin
          let ks = String.sub bk plen (String.length bk - plen) in
          match be.get bk with
          | None -> ()
          | Some v_bytes ->
            (try
              let json = Yojson.Safe.from_string (Bytes.to_string v_bytes) in
              match json with
              | `Assoc kv ->
                (match List.assoc "state" kv with
                 | `Null -> ()
                 | sj -> Hashtbl.replace states ks (deser_st sj));
                let ev_list = match List.assoc "ev_timers" kv with
                  | `List xs -> List.map Yojson.Safe.Util.to_int xs
                  | _ -> [] in
                List.iter (fun t ->
                  ev_timers := TimerSet.add (ks, t) !ev_timers) ev_list;
                let pt_list = match List.assoc "pt_timers" kv with
                  | `List xs -> List.map Yojson.Safe.Util.to_int xs
                  | _ -> [] in
                List.iter (fun t ->
                  pt_timers := TimerSet.add (ks, t) !pt_timers) pt_list
              | _ -> failwith "process_keyed restore: top-level not assoc"
            with
            | Yojson.Json_error msg ->
              failwith ("process_keyed restore: invalid JSON (" ^ bk ^ "): " ^ msg)
            | Not_found ->
              failwith ("process_keyed restore: missing field (" ^ bk ^ ")"))
        end
      ) (be.keys ())
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
        | Some (Mf_event.Retract _) -> pull ()
    in pull ()
