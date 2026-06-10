open Miniflink
(* ProcessFunction с таймерами — для minePASS:
   1. heartbeat: нет событий от шахтёра дольше порога -> алерт (event-time)
   2. смена: шахтёр под землёй дольше смены -> алерт (processing-time)
   Таймеры per-key, set/cancel в обработчиках; срабатывание:
   event-time по watermark, processing-time по wall-clock. *)

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

type ping = { miner : string; ts : int }
module ByMiner = Keyed.Make (struct type t = ping let key p = p.miner end)

(* 1. Heartbeat (event-time): таймер на ts+threshold, сбрасывается каждым
   событием; watermark прошёл порог без сброса -> пропал *)
let test_heartbeat () =
  Printf.printf "\n-- heartbeat: silent miner past threshold -> alert (event-time)\n";
  let threshold = 30 in
  let fired = ref [] in
  let src = Stream.of_list [
    Mf_event.data { miner="M1"; ts=0 } 0;
    Mf_event.data { miner="M1"; ts=10 } 10;
    Mf_event.data { miner="M2"; ts=12 } 12;
    Mf_event.wm 100;
  ] in
  let _ =
    src
    |> Pipe.process_keyed (module ByMiner)
         ~init:(fun () -> ())
         ~on_event:(fun ctx _key _st p ->
           ctx.Pipe.cancel_event_timers ();
           ctx.Pipe.set_event_timer (p.ts + threshold);
           ())
         ~on_timer:(fun ctx key _st _t kind ->
           (match kind with Pipe.Event_time -> fired := key :: !fired | _ -> ());
           ignore ctx; ())
    |> Stream.to_list in
  check "M1 flagged missing (last ts=10 +30 <= wm)" (List.mem "M1" !fired);
  check "M2 flagged missing" (List.mem "M2" !fired);
  check "each miner fires once" (List.length !fired = 2)

(* 2. Heartbeat НЕ срабатывает если шахтёр продолжает слать *)
let test_heartbeat_alive () =
  Printf.printf "\n-- active miner is NOT flagged\n";
  let threshold = 30 in
  let fired = ref [] in
  let src = Stream.of_list [
    Mf_event.data { miner="M1"; ts=0 } 0;
    Mf_event.data { miner="M1"; ts=20 } 20;
    Mf_event.data { miner="M1"; ts=40 } 40;
    Mf_event.wm 50;
  ] in
  let _ =
    src |> Pipe.process_keyed (module ByMiner)
      ~init:(fun () -> ())
      ~on_event:(fun ctx _k _st p ->
        ctx.Pipe.cancel_event_timers (); ctx.Pipe.set_event_timer (p.ts + threshold); ())
      ~on_timer:(fun _ctx key _st _t _kind -> fired := key :: !fired; ())
    |> Stream.to_list in
  check "active miner M1 not flagged" (not (List.mem "M1" !fired))

(* 3. Смена (processing-time): под землёй дольше смены по wall-clock *)
let test_shift_processing_time () =
  Printf.printf "\n-- shift overrun: underground past shift -> alert (processing-time)\n";
  let clock = ref 1000 in
  let now_ms () = !clock in
  let shift_ms = 500 in
  let overrun = ref [] in
  let src = Stream.of_list [
    Mf_event.data { miner="M1"; ts=0 } 0;
    Mf_event.data { miner="M2"; ts=1 } 1;
    Mf_event.wm 10;
  ] in
  let _ =
    src |> Pipe.process_keyed (module ByMiner) ~now_ms
      ~init:(fun () -> ())
      ~on_event:(fun ctx _k _st _p ->
        ctx.Pipe.set_processing_timer (now_ms () + shift_ms);
        clock := !clock + 600;
        ())
      ~on_timer:(fun _ctx key _st _t kind ->
        (match kind with Pipe.Processing_time -> overrun := key :: !overrun | _ -> ()); ())
    |> Stream.to_list in
  check "M1 shift overrun fired (wall-clock past shift)" (List.mem "M1" !overrun);
  check "M2 shift overrun fired" (List.mem "M2" !overrun)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  ProcessFunction with timers\n";
  Printf.printf "==========================================\n";
  test_heartbeat ();
  test_heartbeat_alive ();
  test_shift_processing_time ();
  Printf.printf "\nTimer tests passed.\n"
