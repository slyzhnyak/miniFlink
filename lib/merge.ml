(* Слияние N watermarked-потоков (партиций) в один.

   Каждая партиция несёт своё event-time. Общий watermark = минимум по
   АКТИВНЫМ партициям (нельзя считать время T прошедшим, пока все живые
   входы это не подтвердят — иначе потеря данных отстающей партиции).

   Молчащая партиция застопорила бы минимум, поэтому idle-стратегия
   (выбирается при создании) решает, учитывать ли её:
   - Never: всегда учитывать (строго, без потерь, но молчащая вешает wm)
   - Wall_clock_timeout ms: молчит ms реальных мс → idle, исключается из
     минимума; вернётся в расчёт когда снова даст событие.

   Data/Retract пробрасываются как есть (по факту прихода). Watermark
   эмитится когда минимум по активным продвинулся. *)

type idle_strategy =
  | Never
  | Wall_clock_timeout of int   (* мс реального времени без событий → idle *)

type 'a part = {
  pull            : 'a Mf_event.t Stream.t;
  mutable wm      : Time.t;     (* последний watermark партиции *)
  mutable done_   : bool;       (* поток исчерпан *)
  mutable last_activity : int;  (* wall-clock мс последней активности *)
}

let merge_partitioned
    ?(now_ms = fun () -> int_of_float (Unix.gettimeofday () *. 1000.))
    ~(idle : idle_strategy)
    (streams : 'a Mf_event.t Stream.t list)
    : 'a Mf_event.t Stream.t =
  let parts = Array.of_list (List.map (fun s ->
    { pull = s; wm = min_int; done_ = false; last_activity = now_ms () }) streams) in
  let emitted_wm = ref min_int in
  let out : 'a Mf_event.t Queue.t = Queue.create () in

  (* активна ли партиция для расчёта минимума *)
  let active (p : 'a part) : bool =
    if p.done_ then false
    else match idle with
      | Never -> true
      | Wall_clock_timeout ms -> now_ms () - p.last_activity <= ms in

  (* минимум watermark по активным; если активных нет — None *)
  let min_active_wm () : Time.t option =
    Array.fold_left (fun acc p ->
      if active p then
        match acc with Some m -> Some (min m p.wm) | None -> Some p.wm
      else acc) None parts in

  (* попробовать продвинуть общий watermark *)
  let maybe_emit_wm () =
    match min_active_wm () with
    | Some m when m > !emitted_wm -> emitted_wm := m; Queue.push (Mf_event.wm m) out
    | _ -> () in

  let all_done () = Array.for_all (fun p -> p.done_) parts in

  fun () ->
    let rec step () =
      if not (Queue.is_empty out) then Some (Queue.pop out)
      else if all_done () then None
      else begin
        (* один проход по партициям: тянем по одному событию с каждой *)
        let progressed = ref false in
        Array.iter (fun p ->
          if not p.done_ then
            match p.pull () with
            | None ->
              p.done_ <- true; progressed := true;
              if p.wm = min_int && idle = Never then
                Log.warn
                  "merge_partitioned: партиция завершилась, не дав ни одного \
                   watermark — при idle:Never она всё это время держала общий \
                   watermark на минимуме (окна ниже не закрывались). Источник \
                   без watermark? Рассмотрите Wall_clock_timeout";
              maybe_emit_wm ()
            | Some (Mf_event.Watermark w) ->
              if w > p.wm then p.wm <- w;
              p.last_activity <- now_ms ();
              progressed := true;
              maybe_emit_wm ()
            | Some (Mf_event.Data _ as ev) ->
              p.last_activity <- now_ms ();
              progressed := true;
              Queue.push ev out
            | Some (Mf_event.Retract _ as ev) ->
              p.last_activity <- now_ms ();
              progressed := true;
              Queue.push ev out
            | Some (Mf_event.Update _ as ev) ->
              (* Update — атомарная коррекция, передаётся прозрачно. *)
              p.last_activity <- now_ms ();
              progressed := true;
              Queue.push ev out
        ) parts;
        if not (Queue.is_empty out) then Some (Queue.pop out)
        else if !progressed then step ()
        else if all_done () then None
        else None  (* нет прогресса (все idle/пусто сейчас) *)
      end
    in step ()
