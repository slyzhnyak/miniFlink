(** Тест: Pipe.process_keyed_spec — record-based alternative API. *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

type ev = { ev_key : string; ev_value : int }
type st = { mutable count : int }
type out = Out of string * int

module ByKey : Keyed.S with type t = ev = struct
  type t = ev
  let key v = v.ev_key
end

let () =
  Printf.printf "Test: Pipe.process_keyed_spec\n%!";

  (* ── 1. process_keyed_spec equivalent to process_keyed ── *)
  Printf.printf "\n-- 1. process_keyed_spec equivalent to process_keyed\n";

  let events = [
    Mf_event.data { ev_key = "A"; ev_value = 10 } 0;
    Mf_event.data { ev_key = "A"; ev_value = 20 } 1000;
    Mf_event.data { ev_key = "B"; ev_value = 5 } 1500;
    Mf_event.wm 2000;
  ] in

  let make_handler () =
    let on_event (ctx : out Process_fn.ctx) k st (e : ev) =
      st.count <- st.count + e.ev_value;
      ctx.emit (Out (k, st.count))
    in
    let on_timer _ _ _ _ _ = () in
    (on_event, on_timer)
  in

  let on_event, on_timer = make_handler () in
  let direct_alerts = events |> Stream.of_list
    |> Pipe.process_keyed (module ByKey)
         ~init:(fun () -> { count = 0 })
         ~on_event ~on_timer
    |> Pipe.collect in

  let on_event2, on_timer2 = make_handler () in
  let via_spec_alerts = events |> Stream.of_list
    |> Pipe.process_keyed_spec
         (Process_fn.default_spec
            ~keyed:(module ByKey)
            ~init:(fun () -> { count = 0 })
            ~on_event:on_event2
            ~on_timer:on_timer2)
    |> Pipe.collect in

  check "direct = via_spec"
    (direct_alerts = via_spec_alerts);
  check (Printf.sprintf "3 emissions (got %d)" (List.length direct_alerts))
    (List.length direct_alerts = 3);

  (* ── 2. Template pattern с override ─────────────────── *)
  Printf.printf "\n-- 2. Template pattern\n";

  let on_event_v1 (ctx : out Process_fn.ctx) k st (_e : ev) =
    st.count <- st.count + 1;
    ctx.emit (Out ("v1-" ^ k, st.count))
  in
  let on_event_v2 (ctx : out Process_fn.ctx) k st (_e : ev) =
    st.count <- st.count + 10;
    ctx.emit (Out ("v2-" ^ k, st.count))
  in
  let on_timer _ _ _ _ _ = () in

  let base = Process_fn.default_spec
    ~keyed:(module ByKey)
    ~init:(fun () -> { count = 0 })
    ~on_event:on_event_v1  (* placeholder *)
    ~on_timer in

  let result_v1 = events |> Stream.of_list
    |> Pipe.process_keyed_spec { base with on_event = on_event_v1 }
    |> Pipe.collect in
  let result_v2 = events |> Stream.of_list
    |> Pipe.process_keyed_spec { base with on_event = on_event_v2 }
    |> Pipe.collect in

  check "v1 prefix" (List.for_all (function
    | Out (s, _) -> String.length s >= 2 && String.sub s 0 2 = "v1") result_v1);
  check "v2 prefix" (List.for_all (function
    | Out (s, _) -> String.length s >= 2 && String.sub s 0 2 = "v2") result_v2);
  check "v1 counts increment by 1"
    (List.map (function Out (_, n) -> n) result_v1 = [1; 2; 1]);
  check "v2 counts increment by 10"
    (List.map (function Out (_, n) -> n) result_v2 = [10; 20; 10]);

  Printf.printf "\nTest passed.\n"
