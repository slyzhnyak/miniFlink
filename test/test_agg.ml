open Miniflink
(* Тесты комбинируемых агрегаторов: каждый готовый агрегат, комбинирование
   (both / let+ / and+), и интеграция с Pipe.window_agg. *)

open Domain
open Time

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

(* ── готовые агрегаторы через run (на списках) ─────────────── *)
let test_basic () =
  Printf.printf "\n-- basic aggregators\n";
  let xs = [1.; 2.; 3.; 4.] in
  check "count = 4" (Agg.run Agg.count xs = 4);
  check "sum = 10" (Agg.run (Agg.sum (fun x -> x)) xs = 10.);
  check "mean = 2.5" (Agg.run (Agg.mean (fun x -> x)) xs = Some 2.5);
  check "min = 1" (Agg.run (Agg.min_by (fun x -> x)) xs = Some 1.);
  check "max = 4" (Agg.run (Agg.max_by (fun x -> x)) xs = Some 4.);
  check "count_if >2 = 2" (Agg.run (Agg.count_if (fun x -> x > 2.)) xs = 2);
  check "first = 1" (Agg.run Agg.first xs = Some 1.);
  check "last = 4" (Agg.run Agg.last xs = Some 4.);
  check "to_list preserves order" (Agg.run Agg.to_list xs = xs)

(* ── пустой вход ───────────────────────────────────────────── *)
let test_empty () =
  Printf.printf "\n-- aggregators on empty input\n";
  check "count [] = 0" (Agg.run Agg.count [] = 0);
  check "mean [] = None" (Agg.run (Agg.mean (fun x -> x)) [] = None);
  check "max [] = None" (Agg.run (Agg.max_by (fun x -> x)) [] = None);
  check "first [] = None" (Agg.run Agg.first ([] : float list) = None)

(* ── arg_min / arg_max возвращают сам элемент ──────────────── *)
let test_arg () =
  Printf.printf "\n-- arg_min / arg_max return the element\n";
  let people = [("alice", 30.); ("bob", 25.); ("carol", 40.)] in
  check "arg_max by age = carol"
    (Agg.run (Agg.arg_max snd) people = Some ("carol", 40.));
  check "arg_min by age = bob"
    (Agg.run (Agg.arg_min snd) people = Some ("bob", 25.))

(* ── комбинирование: both и let+/and+ ──────────────────────── *)
let test_combine () =
  Printf.printf "\n-- combining aggregators (one pass)\n";
  let xs = [10.; 20.; 30.] in
  let mean_max = Agg.both (Agg.mean (fun x -> x)) (Agg.max_by (fun x -> x)) in
  check "both: (mean, max) = (20, 30)" (Agg.run mean_max xs = (Some 20., Some 30.));
  (* let+/and+ синтаксис *)
  let stats = Agg.(
    let+ n   = count
    and+ avg = mean (fun x -> x)
    and+ hi  = max_by (fun x -> x) in
    (n, avg, hi)) in
  check "let+/and+: (3, Some 20, Some 30)" (Agg.run stats xs = (3, Some 20., Some 30.))

(* ── map / contramap ──────────────────────────────────────── *)
let test_map_contramap () =
  Printf.printf "\n-- map / contramap\n";
  (* map результата *)
  let doubled = Agg.map (fun n -> n * 2) Agg.count in
  check "map: count*2" (Agg.run doubled [(); (); ()] = 6);
  (* contramap входа: проекция перед агрегатом *)
  let people = [("a", 1.); ("b", 2.); ("c", 3.)] in
  let sum_ages = Agg.contramap snd (Agg.sum (fun x -> x)) in
  check "contramap: sum of projected = 6" (Agg.run sum_ages people = 6.)

(* ── интеграция с window_agg ───────────────────────────────── *)
let tel ?(speed=0.) id ts = { device_id = id; speed_kmh = speed; fuel_pct = 0.;
                              position = { lat = 0.; lon = 0. }; ts; device = None }

let test_window_agg () =
  Printf.printf "\n-- Pipe.window_agg integration\n";
  let events = [
    tel ~speed:10. "A" (seconds 1); tel ~speed:30. "A" (seconds 2);
    tel ~speed:20. "A" (seconds 3);
  ] in
  (* среднее и максимум скорости за окно, одним проходом *)
  let agg = Agg.both (Agg.mean (fun t -> t.speed_kmh))
                     (Agg.max_by (fun t -> t.speed_kmh)) in
  let out = Stream.of_list (List.map (fun (t:telemetry) -> Mf_event.data t t.ts) events)
    |> Mf_event.with_watermarks ~latency:0
    |> Pipe.window_agg (module Telemetry) (Pipe.tumbling (seconds 10)) agg
    |> Stream.to_list
    |> List.filter_map (function Mf_event.Data (v,_) -> Some v | _ -> None) in
  check "window_agg: (mean, max) = (20, 30)"
    (out = [("A", (Some 20., Some 30.))])

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Composable aggregators (Agg)\n";
  Printf.printf "==========================================\n";
  test_basic ();
  test_empty ();
  test_arg ();
  test_combine ();
  test_map_contramap ();
  test_window_agg ();
  Printf.printf "\nAll aggregator tests passed.\n"
