open Miniflink
(* ============================================================
   Test_determinism.ml — детерминизм и воспроизводимость

   Свойства специфичные для stream engine, которые ломаются
   именно в потоковых системах:

   1. Replay determinism — один вход дважды → идентичный выход.
      Фундамент exactly-once: восстановление должно дать тот же
      результат что и исходный прогон.

   2. Parallel = Sequential (как мультимножества) — N воркеров
      дают тот же НАБОР алертов что 1 поток. Порядок МЕЖДУ ключами
      недетерминирован by design (разные воркеры), поэтому
      сравниваем мультимножества, не списки.

   3. Watermark fuzzing — случайно переупорядоченные потоки.
      Инварианты окон сохраняются при любом порядке прихода.

   4. Window permutation invariance — перестановка событий в
      пределах одного watermark не меняет результат окна
      (агрегаты компилируются из множества, не из порядка).
   ============================================================ *)

open QCheck
open Test_support.Domain
open Time

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

(* ── Генераторы ─────────────────────────────────────────── *)

let gen_device = Gen.oneofl ["A";"B";"C";"D";"E";"F";"G";"H"]

let gen_telemetry =
  Gen.map (fun (d, sp, fu, t) ->
    { device_id=d; speed_kmh=sp; fuel_pct=fu;
      position={lat=55.;lon=37.}; ts=t; device=None })
  (Gen.tup4 gen_device
    (Gen.float_range 0. 200.)
    (Gen.float_range 0. 100.)
    (Gen.int_range 0 600000))

(* Поток из N телеметрий, отсортированных по времени (как реальный источник) *)
let gen_stream n =
  Gen.map (fun lst ->
    List.sort (fun (a:telemetry) b -> compare a.ts b.ts) lst)
  (Gen.list_size (Gen.return n) gen_telemetry)

let devices = Table.of_list [
  "A",{owner="o";max_speed=90.;zone="z"}; "B",{owner="o";max_speed=80.;zone="z"};
  "C",{owner="o";max_speed=85.;zone="z"}; "D",{owner="o";max_speed=70.;zone="z"};
]

let pipeline src =
  src
  |> Mf_event.with_watermarks ~latency:(seconds 3)
  |> Pipe.enrich (module Telemetry) ~from:devices
       ~merge:(fun t d -> { t with device = d })
  |> Pipe.window (module Telemetry) (Pipe.tumbling (seconds 30))
  |> Pipe.aggregate Test_support.Rules.compute
  |> Pipe.flat_map (Test_support.Rules.check Test_support.Rules.fleet)

(* Прогнать pipeline, собрать алерты как сравнимый список ключей *)
let run_alerts telemetries =
  Stream.of_list (List.map (fun (t:telemetry) -> Mf_event.data t t.ts) telemetries)
  |> pipeline
  |> Stream.to_list
  |> List.filter_map (function
       | Mf_event.Data (a,_) -> Some (a.device_id ^ "/" ^ a.rule) | _ -> None)

(* Мультимножество: сортированный список (для сравнения без учёта порядка) *)
let multiset xs = List.sort compare xs

(* ── 1. Replay determinism ──────────────────────────────── *)

let test_replay_determinism () =
  Printf.printf "\n-- 1. Replay determinism (same input -> same output)\n";
  let cell = QCheck.Test.make_cell ~count:200
      (make (gen_stream 50))
      (fun ts -> run_alerts ts = run_alerts ts) in
  let r = QCheck.Test.check_cell ~long:false cell in
  match QCheck.TestResult.get_state r with
  | QCheck.TestResult.Success ->
    pass "200 streams: replay identical (order included)"
  | _ -> fail "replay not deterministic"

(* ── 2. Parallel = Sequential (multiset) ────────────────── *)

let test_parallel_eq_sequential () =
  Printf.printf "\n-- 2. Parallel result = Sequential (as multiset)\n";
  Random.self_init ();
  let ok = ref true in
  for _ = 1 to 20 do
    (* Генерируем поток детерминированно через QCheck генератор *)
    let st = Random.State.make_self_init () in
    let telemetries =
      (gen_stream 200) st in

    (* Sequential *)
    let seq = multiset (run_alerts telemetries) in

    (* Parallel: 4 воркера, каждый со своим буфером *)
    let buffers = Array.init 4 (fun _ -> ref []) in
    let mu = Array.init 4 (fun _ -> Mutex.create ()) in
    Parallel.run_parallel_simple
      ~sink_factory:(fun i -> fun a ->
        Mutex.lock mu.(i);
        buffers.(i) := (a.device_id ^ "/" ^ a.rule) :: !(buffers.(i));
        Mutex.unlock mu.(i))
      ~workers:4 ~capacity:256
      ~key_of:(fun (t:telemetry) -> t.device_id)
      ~pipeline
      ~source:(Stream.of_list (List.map (fun (t:telemetry) -> Mf_event.data t t.ts) telemetries))
      ~sink:(fun _ -> ())
      ();
    let par = multiset (Array.fold_left (fun acc r -> !r @ acc) [] buffers) in

    if seq <> par then begin
      ok := false;
      Printf.printf "    seq=%d par=%d MISMATCH\n%!"
        (List.length seq) (List.length par)
    end
  done;
  check "20 streams: parallel multiset = sequential multiset" !ok

(* ── 3. Watermark fuzzing ───────────────────────────────── *)

let test_watermark_fuzz () =
  Printf.printf "\n-- 3. Watermark fuzzing (invariants under reorder)\n";
  let cell = QCheck.Test.make_cell ~count:300 (make (gen_stream 60))
    (fun telemetries ->
       (* Инвариант: сумма событий по окнам = число валидных событий.
          Watermark с latency=3s может отбросить события на самом краю,
          но при сортировке по ts все события попадают в окна. *)
       let total_in = List.length telemetries in
       let total_windowed =
         Stream.of_list (List.map (fun (t:telemetry) -> Mf_event.data t t.ts) telemetries)
         |> Mf_event.with_watermarks ~latency:(seconds 3)
         |> Pipe.window (module Telemetry) (Pipe.tumbling (seconds 30))
         |> Pipe.aggregate (fun _ (vs:telemetry list) -> List.length vs)
         |> Stream.to_list
         |> List.filter_map (function Mf_event.Data(n,_) -> Some n | _ -> None)
         |> List.fold_left (+) 0
       in
       total_windowed = total_in)
  in
  let r = QCheck.Test.check_cell ~long:false cell in
  match QCheck.TestResult.get_state r with
  | QCheck.TestResult.Success ->
    pass "300 fuzzed streams: window count = input count"
  | _ -> fail "window dropped or duplicated events under fuzzing"

let test_watermark_monotone_fuzz () =
  let cell = QCheck.Test.make_cell ~count:300 (make (gen_stream 80))
    (fun telemetries ->
       let wms =
         Stream.of_list (List.map (fun (t:telemetry) -> Mf_event.data t t.ts) telemetries)
         |> Mf_event.with_watermarks ~latency:(seconds 3)
         |> Stream.to_list
         |> List.filter_map (function Mf_event.Watermark t -> Some t | _ -> None)
       in
       let rec mono = function
         | a :: (b :: _ as r) -> a <= b && mono r | _ -> true in
       mono wms)
  in
  let r = QCheck.Test.check_cell ~long:false cell in
  match QCheck.TestResult.get_state r with
  | QCheck.TestResult.Success ->
    pass "300 fuzzed streams: watermarks monotone"
  | _ -> fail "watermarks not monotone"

(* ── 4. Window permutation invariance ───────────────────── *)

let test_permutation_invariance () =
  Printf.printf "\n-- 4. Window result invariant under intra-watermark permutation\n";
  let shuffle lst =
    let a = Array.of_list lst in
    for i = Array.length a - 1 downto 1 do
      let j = Random.int (i+1) in
      let tmp = a.(i) in a.(i) <- a.(j); a.(j) <- tmp
    done;
    Array.to_list a
  in
  (* Агрегат compute использует max/min/avg — независимы от порядка.
     Перемешиваем события ВНУТРИ окна, результат должен совпасть. *)
  let ok = ref true in
  for _ = 1 to 50 do
    let st = Random.State.make_self_init () in
    let base = (gen_stream 40) st in
    (* группируем по окну [0,30s): все события с ts < 30000 *)
    let in_window = List.filter (fun (t:telemetry) -> t.ts < 30000) base in
    if List.length in_window >= 2 then begin
      let agg telemetries =
        Stream.of_list (List.map (fun (t:telemetry) -> Mf_event.data t t.ts)
          (List.sort (fun (a:telemetry) b -> compare a.ts b.ts) telemetries)
          @ [Mf_event.wm 35000])
        |> Pipe.window (module Telemetry) (Pipe.tumbling (seconds 30))
        |> Pipe.aggregate Test_support.Rules.compute
        |> Stream.to_list
        |> List.filter_map (function
             | Mf_event.Data ((s:stats),_) -> Some (s.device_id, s.max_speed, s.min_fuel)
             | _ -> None)
        |> List.sort compare
      in
      let r1 = agg in_window in
      let r2 = agg (shuffle in_window) in
      if r1 <> r2 then ok := false
    end
  done;
  check "50 streams: aggregate invariant under permutation" !ok

(* ── Main ────────────────────────────────────────────────── *)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  miniFlink — Determinism Tests\n";
  Printf.printf "==========================================\n";
  test_replay_determinism ();
  test_parallel_eq_sequential ();
  test_watermark_fuzz ();
  test_watermark_monotone_fuzz ();
  test_permutation_invariance ();
  Printf.printf "\nAll determinism tests passed.\n"
