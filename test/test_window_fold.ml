(* Тест инкрементальной агрегации окон (window_fold). Главное —
   результат должен совпадать с материализованным window |> aggregate,
   но без хранения списка. Проверяем сумму, счёт, max и эквивалентность. *)

open Domain
open Time

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let tel ?(speed=0.) id ts = { device_id = id; speed_kmh = speed; fuel_pct = 0.;
                              position = { lat = 0.; lon = 0. }; ts; device = None }

let data_out s =
  Stream.to_list s |> List.filter_map (function
    | Mf_event.Data (v, _) -> Some v | _ -> None)

(* ── window_fold: сумма скоростей за окно ──────────────────── *)
let test_fold_sum () =
  Printf.printf "\n-- window_fold: incremental sum\n";
  let events = [
    tel ~speed:10. "A" (seconds 1); tel ~speed:20. "A" (seconds 2);
    tel ~speed:30. "A" (seconds 3);
  ] in
  let out = Stream.of_list (List.map (fun (t:telemetry) -> Mf_event.data t t.ts) events)
    |> Mf_event.with_watermarks ~latency:0
    |> Pipe.window_fold (module Telemetry) (Pipe.tumbling (seconds 10))
         ~init:(fun () -> 0.)
         ~add:(fun acc t -> acc +. t.speed_kmh)
    |> data_out in
  check "sum = 60" (out = [("A", 60.)])

(* ── эквивалентность window|>aggregate и window_fold ───────── *)
let test_fold_equivalence () =
  Printf.printf "\n-- window_fold == window |> aggregate (same result)\n";
  let events = List.init 20 (fun i ->
    tel ~speed:(float_of_int (i * 5)) "A" (seconds (i mod 3))) in
  let mk () = Stream.of_list (List.map (fun (t:telemetry) -> Mf_event.data t t.ts) events)
    |> Mf_event.with_watermarks ~latency:0 in
  (* материализованный путь *)
  let materialized = mk ()
    |> Pipe.window (module Telemetry) (Pipe.tumbling (seconds 10))
    |> Pipe.aggregate (fun _k vs ->
         List.fold_left (fun a t -> a +. t.speed_kmh) 0. vs)
    |> data_out in
  (* инкрементальный путь *)
  let incremental = mk ()
    |> Pipe.window_fold (module Telemetry) (Pipe.tumbling (seconds 10))
         ~init:(fun () -> 0.)
         ~add:(fun acc t -> acc +. t.speed_kmh)
    |> data_out in
  (* оба должны дать одинаковую сумму на одно окно *)
  let sum_mat = List.fold_left (fun a s -> a +. s) 0. materialized in
  let sum_inc = List.fold_left (fun a (_, s) -> a +. s) 0. incremental in
  check "totals match" (sum_mat = sum_inc);
  check "same number of windows" (List.length materialized = List.length incremental)

(* ── max через fold ────────────────────────────────────────── *)
let test_fold_max () =
  Printf.printf "\n-- window_fold: max\n";
  let events = [
    tel ~speed:30. "A" (seconds 1); tel ~speed:90. "A" (seconds 2);
    tel ~speed:50. "A" (seconds 3);
  ] in
  let out = Stream.of_list (List.map (fun (t:telemetry) -> Mf_event.data t t.ts) events)
    |> Mf_event.with_watermarks ~latency:0
    |> Pipe.window_fold (module Telemetry) (Pipe.tumbling (seconds 10))
         ~init:(fun () -> neg_infinity)
         ~add:(fun acc t -> Float.max acc t.speed_kmh)
    |> data_out in
  check "max = 90" (out = [("A", 90.)])

(* ── среднее как (сумма, счёт) ─────────────────────────────── *)
let test_fold_average () =
  Printf.printf "\n-- window_fold: average as (sum, count) accumulator\n";
  let events = [
    tel ~speed:10. "A" (seconds 1); tel ~speed:20. "A" (seconds 2);
    tel ~speed:30. "A" (seconds 3);
  ] in
  let out = Stream.of_list (List.map (fun (t:telemetry) -> Mf_event.data t t.ts) events)
    |> Mf_event.with_watermarks ~latency:0
    |> Pipe.window_fold (module Telemetry) (Pipe.tumbling (seconds 10))
         ~init:(fun () -> (0., 0))
         ~add:(fun (sum, cnt) t -> (sum +. t.speed_kmh, cnt + 1))
    |> data_out in
  match out with
  | [("A", (sum, cnt))] ->
    check "sum=60, count=3" (sum = 60. && cnt = 3);
    check "average = 20" (sum /. float_of_int cnt = 20.)
  | _ -> fail "expected one window"

(* ── пустой поток / per-key ────────────────────────────────── *)
let test_fold_edge () =
  Printf.printf "\n-- window_fold: empty stream and per-key\n";
  let empty = Stream.empty
    |> Pipe.window_fold (module Telemetry) (Pipe.tumbling (seconds 10))
         ~init:(fun () -> 0) ~add:(fun a _ -> a + 1)
    |> data_out in
  check "empty stream → no windows" (empty = []);
  let events = [ tel "A" (seconds 1); tel "B" (seconds 2); tel "A" (seconds 3) ] in
  let out = Stream.of_list (List.map (fun (t:telemetry) -> Mf_event.data t t.ts) events)
    |> Mf_event.with_watermarks ~latency:0
    |> Pipe.window_fold (module Telemetry) (Pipe.tumbling (seconds 10))
         ~init:(fun () -> 0) ~add:(fun a _ -> a + 1)
    |> data_out in
  check "A counted 2, B counted 1"
    (List.sort compare out = [("A", 2); ("B", 1)])

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Incremental window aggregation (window_fold)\n";
  Printf.printf "==========================================\n";
  test_fold_sum ();
  test_fold_equivalence ();
  test_fold_max ();
  test_fold_average ();
  test_fold_edge ();
  Printf.printf "\nAll window_fold tests passed.\n"
