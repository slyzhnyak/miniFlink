(* ============================================================
   Harness.ml — рабочий тестовый фреймворк

   Архитектура:
   - in_buf: Queue куда мы пишем тестовые события
   - pipeline применяется лениво при вызове run
   - run вычитывает всё из pipeline до None

   Никакого Obj.magic — честная типизация через
   ('a, 'b) t где 'a = тип входа, 'b = тип выхода.
   ============================================================ *)

type ('a, 'b) t = {
  in_buf   : 'a Mf_event.t Queue.t;
  pipeline : 'a Mf_event.t Stream.t -> 'b Mf_event.t Stream.t;
}

let create pipeline = {
  in_buf   = Queue.create ();
  pipeline;
}

let push ctx v ~ts =
  Queue.push (Mf_event.data v ts) ctx.in_buf

let push_wm ctx ~ts =
  Queue.push (Mf_event.wm ts) ctx.in_buf

let push_all ctx pairs =
  List.iter (fun (v, ts) -> push ctx v ~ts) pairs

let run_all ctx =
  let src () =
    if Queue.is_empty ctx.in_buf then None
    else Some (Queue.pop ctx.in_buf)
  in
  Stream.to_list (ctx.pipeline src)

let run_data ctx =
  run_all ctx |> List.filter_map (function
    | Mf_event.Data (v,_) -> Some v
    | _ -> None)

let run ctx = run_data ctx

(* ── Assertions ─────────────────────────────────────────── *)

let expect_count ctx n =
  let got = List.length (run_data ctx) in
  if got <> n then
    failwith (Printf.sprintf "expect_count: expected %d, got %d" n got)

let expect ctx pred msg =
  if not (List.for_all pred (run_data ctx)) then
    failwith (Printf.sprintf "expect failed: %s" msg)

let expect_none ctx =
  let results = run_data ctx in
  if results <> [] then
    failwith (Printf.sprintf "expect_none: got %d events" (List.length results))

(* ── Утилиты ─────────────────────────────────────────────── *)

let ev device_id t_s speed fuel : Domain.telemetry =
  { Domain.device_id; speed_kmh = speed; fuel_pct = fuel;
    position = { Domain.lat = 55.75; lon = 37.61 };
    ts = t_s * 1000; device = None }

let evs lst = List.map (fun (id, t, s, f) -> ev id t s f) lst
