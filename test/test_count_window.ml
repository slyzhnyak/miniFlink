open Miniflink
(* Тест count-окон: фаерят по числу событий, не по времени.
   Watermarks не нужны. Проверяем tumbling (каждые N), sliding (N с шагом),
   группировку по ключу, прозрачность watermark, валидацию. *)

open Test_support.Domain

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let tel id ts = { device_id = id; speed_kmh = 1.; fuel_pct = 1.;
                  position = { lat = 0.; lon = 0. }; ts; device = None }

let windows_out stream =
  Stream.to_list stream
  |> List.filter_map (function
       | Mf_event.Data ((k, vs), _) -> Some (k, List.length vs)
       | _ -> None)

(* ── count_tumbling: окно каждые N событий ─────────────────── *)
let test_count_tumbling () =
  Printf.printf "\n-- count_tumbling: window every N events per key\n";
  (* 7 событий ключа A, N=3 → два полных окна (3+3), остаток 1 не эмитится *)
  let events = List.init 7 (fun i -> Mf_event.data (tel "A" i) i) in
  let out = Stream.of_list events
    |> Pipe.count_window (module Telemetry) (Pipe.count_tumbling 3)
    |> windows_out in
  check "two windows fired (7 events / 3)" (List.length out = 2);
  check "each window has 3 events" (List.for_all (fun (_, n) -> n = 3) out);
  check "all from key A" (List.for_all (fun (k, _) -> k = "A") out)

(* ── группировка по ключу независима ───────────────────────── *)
let test_count_per_key () =
  Printf.printf "\n-- count windows are per-key independent\n";
  (* A: 3 события, B: 3 события, вперемешку; N=3 → по одному окну на ключ *)
  let events = [
    Mf_event.data (tel "A" 0) 0; Mf_event.data (tel "B" 1) 1;
    Mf_event.data (tel "A" 2) 2; Mf_event.data (tel "B" 3) 3;
    Mf_event.data (tel "A" 4) 4; Mf_event.data (tel "B" 5) 5;
  ] in
  let out = Stream.of_list events
    |> Pipe.count_window (module Telemetry) (Pipe.count_tumbling 3)
    |> windows_out in
  check "two windows (one per key)" (List.length out = 2);
  check "A window fired" (List.mem ("A", 3) out);
  check "B window fired" (List.mem ("B", 3) out)

(* ── count_sliding: окно N с шагом step ────────────────────── *)
let test_count_sliding () =
  Printf.printf "\n-- count_sliding: window of N, new one every step\n";
  (* 6 событий, окно 3, шаг 1 → окна при достижении 3, потом каждое +1:
     после события 3 (окно [1,2,3]), 4 ([2,3,4]), 5 ([3,4,5]), 6 ([4,5,6]) *)
  let events = List.init 6 (fun i -> Mf_event.data (tel "A" i) i) in
  let out = Stream.of_list events
    |> Pipe.count_window (module Telemetry) (Pipe.count_sliding 3 1)
    |> windows_out in
  check "4 sliding windows (6 events, size 3, step 1)" (List.length out = 4);
  check "each sliding window has 3 events" (List.for_all (fun (_, n) -> n = 3) out)

(* ── watermark проходит прозрачно ──────────────────────────── *)
let test_watermark_transparent () =
  Printf.printf "\n-- watermarks pass through count window\n";
  let events = [
    Mf_event.data (tel "A" 0) 0;
    Mf_event.wm 100;
    Mf_event.data (tel "A" 1) 1;
  ] in
  let all = Stream.of_list events
    |> Pipe.count_window (module Telemetry) (Pipe.count_tumbling 5)
    |> Stream.to_list in
  let wms = List.filter (function Mf_event.Watermark _ -> true | _ -> false) all in
  check "watermark preserved" (List.length wms = 1)

(* ── валидация параметров ──────────────────────────────────── *)
let test_validation () =
  Printf.printf "\n-- count window rejects non-positive params\n";
  let raises f = try ignore (f ()); false with Invalid_argument _ -> true | _ -> false in
  check "count_tumbling 0 → Invalid_argument"
    (raises (fun () -> Stream.of_list [] |> Pipe.count_window (module Telemetry) (Pipe.count_tumbling 0)));
  check "count_sliding 3 0 → Invalid_argument"
    (raises (fun () -> Stream.of_list [] |> Pipe.count_window (module Telemetry) (Pipe.count_sliding 3 0)))

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Count windows (fire by event count)\n";
  Printf.printf "==========================================\n";
  test_count_tumbling ();
  test_count_per_key ();
  test_count_sliding ();
  test_watermark_transparent ();
  test_validation ();
  Printf.printf "\nAll count window tests passed.\n"
