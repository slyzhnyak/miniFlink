open Miniflink
(* Тесты комбинируемых агрегаторов: каждый готовый агрегат, комбинирование
   (both / let+ / and+), и интеграция с Pipe.window_agg. *)

open Test_support.Domain
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

let test_median () =
  Printf.printf "\n-- median\n";
  let run vals = Agg.run (Agg.median (fun x -> x)) vals in
  check "odd count: middle" (run [3.; 1.; 2.] = Some 2.);
  check "even count: mean of middles" (run [4.; 1.; 3.; 2.] = Some 2.5);
  check "single" (run [7.] = Some 7.);
  check "empty -> None" (run [] = None);
  check "unsorted robust" (run [10.; -5.; 0.] = Some 0.)

let () = test_median ()

(* ── group_by + top_k_by / bottom_k_by ──────────────────────── *)

let test_group_by_basic () =
  Printf.printf "\n-- group_by: per-subkey inner aggregate\n";
  let xs = [("a", 1.); ("b", 5.); ("a", 3.); ("b", 10.); ("a", 2.)] in
  let agg = Agg.group_by ~key:fst ~inner:(Agg.mean snd) in
  let result = Agg.run agg xs in
  let sorted = List.sort compare result in
  check "groups: a-mean=2, b-mean=7.5"
    (sorted = [ ("a", Some 2.0); ("b", Some 7.5) ])

let test_top_k_by () =
  Printf.printf "\n-- top_k_by: K elements by numeric projection, descending\n";
  let xs = [("a", 3.); ("b", 7.); ("c", 1.); ("d", 9.); ("e", 5.)] in
  let agg = Agg.top_k_by 2 ~by:snd in
  check "top-2 by value: d=9, b=7" (Agg.run agg xs = [("d", 9.); ("b", 7.)]);
  let agg5 = Agg.top_k_by 5 ~by:snd in
  check "top-5 of 5: all five descending"
    (List.map fst (Agg.run agg5 xs) = ["d"; "b"; "e"; "a"; "c"]);
  let agg_big = Agg.top_k_by 10 ~by:snd in
  check "k > n: returns all n" (List.length (Agg.run agg_big xs) = 5);
  check "empty: returns []" (Agg.run agg5 [] = [])

let test_bottom_k_by () =
  Printf.printf "\n-- bottom_k_by: K elements by numeric projection, ascending\n";
  let xs = [("a", 3.); ("b", 7.); ("c", 1.); ("d", 9.); ("e", 5.)] in
  check "bottom-2 by value: c=1, a=3"
    (Agg.run (Agg.bottom_k_by 2 ~by:snd) xs = [("c", 1.); ("a", 3.)]);
  check "bottom of empty: []"
    (Agg.run (Agg.bottom_k_by 3 ~by:snd) [] = [])

let test_group_by_compose_top () =
  Printf.printf "\n-- composition: window_agg ... group_by then post-process top_k_by\n";
  (* реальный сценарий ex07: median per (lamp,beacon), затем top-2 *)
  let readings = [
    ("M1", "B1", -45.); ("M1", "B2", -52.); ("M1", "B1", -47.);
    ("M2", "B3", -48.); ("M2", "B4", -50.); ("M2", "B3", -46.);
  ] in
  (* всё внутри одного агрегата: group_by по биконам, потом top_k_by *)
  let per_miner = Agg.group_by
    ~key:(fun (_, b, _) -> b)
    ~inner:(Agg.median (fun (_, _, r) -> r)) in
  (* группируем сначала по шахтёру через прогон вручную *)
  let m1 = List.filter (fun (l, _, _) -> l = "M1") readings in
  let m1_medians = Agg.run per_miner m1 in
  let m1_top2 = List.sort compare (
    Agg.run (Agg.top_k_by 2
               ~by:(fun (_, m) -> Option.value m ~default:neg_infinity))
            m1_medians) in
  check "M1 top-2 medians: B1=-46 (best), B2=-52"
    (m1_top2 = [("B1", Some (-46.)); ("B2", Some (-52.))])

let () =
  test_group_by_basic ();
  test_top_k_by ();
  test_bottom_k_by ();
  test_group_by_compose_top ()
