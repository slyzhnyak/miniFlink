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

(* Эмиссии должны нести осмысленное время и НЕ нарушать монотонность:
   композиция process_keyed -> окно должна работать. До фикса emit клал
   ts=0 и эмиссии выходили ПОСЛЕ вызвавшего их watermark — нижестоящее
   окно молча теряло их как опоздавшие. *)
let test_emit_time_composes_with_window () =
  Printf.printf "\n-- timer emissions carry fire-time and precede the watermark\n";
  let threshold = 30 in
  let src = Stream.of_list [
    Mf_event.data { miner="M1"; ts=0 } 0;
    Mf_event.wm 100;
  ] in
  let raw =
    src |> Pipe.process_keyed (module ByMiner)
      ~init:(fun () -> ())
      ~on_event:(fun ctx _k _st p ->
        ctx.Pipe.set_event_timer (p.ts + threshold); ())
      ~on_timer:(fun ctx key _st _t _kind -> ctx.Pipe.emit key; ())
    |> Stream.to_list in
  (* (1) эмиссия имеет время срабатывания таймера (30), не 0 *)
  let emit_ts = List.filter_map (function
    | Mf_event.Data (_, t) -> Some t | _ -> None) raw in
  check "emission timestamped with fire time (30)" (emit_ts = [30]);
  (* (2) эмиссия идёт ДО watermark 100 в выходном порядке *)
  let order = List.map (function
    | Mf_event.Data _ -> `D | Mf_event.Watermark _ -> `W | _ -> `R) raw in
  check "emission precedes the triggering watermark"
    (order = [`D; `W] || (List.mem `D order &&
       (let di = ref (-1) and wi = ref (-1) in
        List.iteri (fun i x -> if x = `D && !di < 0 then di := i;
                               if x = `W && !wi < 0 then wi := i) order;
        !di < !wi)))

let test_emit_into_window () =
  Printf.printf "\n-- composition: process_keyed -> window sees the emission\n";
  let threshold = 30 in
  let module KS = Keyed.Make (struct type t = string let key s = s end) in
  let src = Stream.of_list [
    Mf_event.data { miner="M1"; ts=0 } 0;
    Mf_event.wm 100;
  ] in
  let windows =
    src |> Pipe.process_keyed (module ByMiner)
      ~init:(fun () -> ())
      ~on_event:(fun ctx _k _st p ->
        ctx.Pipe.set_event_timer (p.ts + threshold); ())
      ~on_timer:(fun ctx key _st _t _kind -> ctx.Pipe.emit key; ())
    |> Pipe.window (module KS) (Pipe.tumbling 50)
    |> Stream.to_list
    |> List.filter_map (function
       | Mf_event.Data ((k, items), _) -> Some (k, List.length items)
       | _ -> None) in
  check "downstream window captured the timer emission"
    (windows = [("M1", 1)])

let () =
  test_emit_time_composes_with_window ();
  test_emit_into_window ()

(* clear_state: состояние ключа удаляется (борьба с ростом state).
   После clear ключ начинает с init заново. *)
let test_clear_state () =
  Printf.printf "\n-- clear_state removes per-key state (next event re-inits)\n";
  let inits = ref 0 in
  let src = Stream.of_list [
    Mf_event.data { miner="M1"; ts=0 } 0;
    Mf_event.data { miner="M1"; ts=10 } 10;  (* то же состояние *)
    Mf_event.wm 50;                           (* таймер на 30 -> clear_state *)
    Mf_event.data { miner="M1"; ts=60 } 60;  (* после clear -> re-init *)
    Mf_event.wm 200;
  ] in
  let _ =
    src |> Pipe.process_keyed (module ByMiner)
      ~init:(fun () -> incr inits; ref 0)
      ~on_event:(fun ctx _k st p ->
        incr st;
        ctx.Pipe.cancel_event_timers ();
        ctx.Pipe.set_event_timer (p.ts + 30); ())
      ~on_timer:(fun ctx _k _st _t _kind ->
        ctx.Pipe.clear_state (); ())
    |> Stream.to_list in
  check "state re-initialized after clear (2 inits, not 1)" (!inits = 2)

let () = test_clear_state ()

(* Паттерн пере-регистрации (документирован в process_fn.mli): таймеры
   НЕ переживают перезапуск, но СОСТОЯНИЕ переживает (checkpoint). Держим
   last_seen в состоянии; после «рестарта» при любой активности потока
   пере-регистрируем heartbeat-таймеры из снапшота состояния — пропавший
   до рестарта шахтёр всё равно обнаруживается. *)
let test_reregistration_pattern () =
  Printf.printf "\n-- re-registration: missing miner detected even across restart\n";
  let threshold = 30 in
  let fired = ref [] in
  (* «снапшот состояния», переживший рестарт: last_seen по ключам.
     M1 видели на ts=10 и он ПРОПАЛ до рестарта. *)
  let snapshot = [("M1", 10)] in
  (* процесс ПОСЛЕ рестарта: таймеров нет, но при старте пайплайн
     пере-регистрирует их из снапшота — здесь через первое же событие
     служебного ключа/любую активность. Для простоты прогоняем
     пере-регистрацию на первом on_event любого ключа. *)
  let reregistered = ref false in
  let src = Stream.of_list [
    Mf_event.data { miner="M2"; ts=50 } 50;   (* активность другого ключа *)
    Mf_event.wm 100;                           (* 10+30=40 <= 100 -> M1 fired *)
  ] in
  let _ =
    src |> Pipe.process_keyed (module ByMiner)
      ~init:(fun () -> ref None)
      ~on_event:(fun ctx _k st p ->
        if not !reregistered then begin
          (* пере-регистрация из снапшота: таймеры пропавших ключей *)
          List.iter (fun (k, last) ->
            ctx.Pipe.set_event_timer_for k (last + threshold)) snapshot;
          reregistered := true
        end;
        st := Some p.ts;
        ())
      ~on_timer:(fun _ctx key _st t _kind ->
        fired := (key, t) :: !fired; ())
    |> Stream.to_list in
  check "M1 (silent since BEFORE restart) detected via snapshot timer"
    (List.mem ("M1", 40) !fired)

let () = test_reregistration_pattern ()
