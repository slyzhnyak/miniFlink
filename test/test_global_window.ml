(* Тест global-окна + триггеров: одно окно на ключ, политика «когда
   фаерить» отделена в trigger. Проверяем count-триггер, value-триггер
   (ранняя эмиссия), Fire vs FireAndPurge, конец потока. *)

open Domain

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let tel ?(speed=1.) id ts = { device_id = id; speed_kmh = speed; fuel_pct = 1.;
                              position = { lat = 0.; lon = 0. }; ts; device = None }

let windows stream =
  Stream.to_list stream
  |> List.filter_map (function
       | Mf_event.Data ((k, vs), _) -> Some (k, List.length vs)
       | _ -> None)

(* ── count-триггер через global (FireAndPurge каждые n) ────── *)
let test_trigger_count () =
  Printf.printf "\n-- trigger_count: fires every n, purges\n";
  let events = List.init 7 (fun i -> Mf_event.data (tel "A" i) i) in
  let out = Stream.of_list events
    |> Pipe.global_window (module Telemetry) ~trigger:(Pipe.trigger_count 3)
    |> windows in
  (* 7 событий, fire каждые 3 с purge: окна по 3,3, остаток 1 на конце *)
  check "two full windows of 3" (List.length (List.filter (fun (_,n) -> n=3) out) = 2);
  check "trailing remainder of 1 at end" (List.exists (fun (_,n) -> n=1) out)

(* ── value-триггер: ранняя эмиссия по условию ──────────────── *)
let test_trigger_on_value () =
  Printf.printf "\n-- trigger_on_value: early fire on alerting value\n";
  (* фаерим когда speed > 100 (тревога). Накопительно (Fire, не purge). *)
  let events = [
    Mf_event.data (tel ~speed:50. "A" 0) 0;
    Mf_event.data (tel ~speed:60. "A" 1) 1;
    Mf_event.data (tel ~speed:150. "A" 2) 2;   (* триггерит здесь *)
  ] in
  let out = Stream.of_list events
    |> Pipe.global_window (module Telemetry)
         ~trigger:(Pipe.trigger_on_value (fun t -> t.speed_kmh > 100.))
    |> windows in
  (* Fire накопительный (не purge): эмитит при тревоге все 3 события,
     буфер остаётся → на конце потока тот же остаток выходит снова.
     Значит 2 окна, оба по 3 события. Это корректное следствие
     накопительной семантики Fire. *)
  check "fired on alert + remainder at end (2 windows)" (List.length out = 2);
  check "alert fire captured all 3 events"
    (List.exists (fun (_, n) -> n = 3) out)

(* ── FireAndPurge сбрасывает, Fire накапливает ─────────────── *)
let test_fire_vs_purge () =
  Printf.printf "\n-- FireAndPurge resets buffer (count windows don't overlap)\n";
  (* trigger_count purge: после фаера буфер пуст, следующее окно с нуля *)
  let events = List.init 6 (fun i -> Mf_event.data (tel "A" i) i) in
  let out = Stream.of_list events
    |> Pipe.global_window (module Telemetry) ~trigger:(Pipe.trigger_count 3)
    |> windows in
  check "two non-overlapping windows of 3 (6 events)"
    (out = [("A", 3); ("A", 3)])

(* ── per-key ───────────────────────────────────────────────── *)
let test_per_key () =
  Printf.printf "\n-- global window is per-key\n";
  let events = [
    Mf_event.data (tel "A" 0) 0; Mf_event.data (tel "B" 1) 1;
    Mf_event.data (tel "A" 2) 2; Mf_event.data (tel "B" 3) 3;
  ] in
  let out = Stream.of_list events
    |> Pipe.global_window (module Telemetry) ~trigger:(Pipe.trigger_count 2)
    |> windows in
  check "one window per key" (out = [("A", 2); ("B", 2)] || out = [("B", 2); ("A", 2)])

(* ── конец потока эмитит остаток ───────────────────────────── *)
let test_end_remainder () =
  Printf.printf "\n-- end of stream emits non-empty remainder\n";
  (* триггер который никогда не фаерит → всё выйдет на конце потока *)
  let never : telemetry Pipe.trigger = fun ~count:_ ~last:_ -> Pipe.Continue in
  let events = List.init 4 (fun i -> Mf_event.data (tel "A" i) i) in
  let out = Stream.of_list events
    |> Pipe.global_window (module Telemetry) ~trigger:never |> windows in
  check "single window with all 4 at end" (out = [("A", 4)])

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Global window + custom triggers\n";
  Printf.printf "==========================================\n";
  test_trigger_count ();
  test_trigger_on_value ();
  test_fire_vs_purge ();
  test_per_key ();
  test_end_remainder ();
  Printf.printf "\nAll global window tests passed.\n"
