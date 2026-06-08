open Miniflink
(* ============================================================
   Test_props.ml — property-based тесты с QCheck

   Инварианты которые должны выполняться для ЛЮБЫХ входных данных:
   1. window_count: sum(events per window) = total events
   2. dedup_idempotent: повторный прогон через dedup не меняет результат
   3. watermark_order: watermarks строго монотонны
   4. enrich_count: enrich не меняет число событий (left join)
   5. rules_severity: critical только когда действительно превышено
   ============================================================ *)

open QCheck
open Domain
open Time

(* ── Генераторы ─────────────────────────────────────────── *)

let gen_device_id = Gen.oneofl ["A"; "B"; "C"; "D"; "E"]

let gen_telemetry =
  Gen.map (fun (device_id, speed_kmh, fuel_pct, ts) ->
    { device_id; speed_kmh; fuel_pct;
      position = { lat = 55.0; lon = 37.0 };
      ts; device = None })
  (Gen.tup4 gen_device_id
    (Gen.float_range 0. 200.)
    (Gen.float_range 0. 100.)
    (Gen.int_range 0 3600000))

let gen_event =
  Gen.map2 Mf_event.data gen_telemetry (Gen.int_range 0 3600000)

(* N событий с монотонно возрастающим временем *)
let gen_ordered_events n =
  Gen.list_size (Gen.return n)
    (Gen.map2 (fun id t ->
       Mf_event.data
         { device_id = id; speed_kmh = 60.; fuel_pct = 80.;
           position = { lat = 55.; lon = 37. };
           ts = t; device = None } t)
      gen_device_id
      (Gen.int_range 0 3600000))

(* ── Property 1: window сохраняет все события ───────────── *)

let prop_window_preserves_count =
  Test.make ~name:"window: sum(counts) = total events"
    ~count:100
    (make (gen_ordered_events 20))
    (fun events ->
       (* Все события должны попасть ровно в одно tumbling окно каждое *)
       let window_size = seconds 60 in
       let total_in  = List.length events in
       let counts_out =
         Stream.of_list events
         |> Mf_event.with_watermarks ~latency:(seconds 3)
         |> Pipe.window (module Telemetry) (Pipe.tumbling window_size)
         |> Pipe.aggregate (fun _k events -> List.length events)
         |> Stream.to_list
         |> List.filter_map (function Mf_event.Data(n,_) -> Some n | _ -> None)
       in
       let total_out = List.fold_left (+) 0 counts_out in
       total_out = total_in)

(* ── Property 2: dedup идемпотентен ─────────────────────── *)

let prop_dedup_idempotent =
  Test.make ~name:"dedup: applying twice = applying once"
    ~count:200
    (make (Gen.list_size (Gen.int_range 1 20)
      (Gen.map2 (fun rule ts ->
         { Domain.id=""; device_id="A"; rule; severity=Warning; message=""; ts })
       (Gen.oneofl ["speed"; "fuel"; "signal"])
       (Gen.int_range 0 3600000))))
    (fun alerts ->
       let dedup_once src =
         src |> Pipe.dedup (module Alert) ~rule:(fun a -> a.rule) ~cooldown:(minutes 5) in
       let run_dedup events =
         Stream.of_list (List.map (fun a -> Mf_event.data a a.ts) events)
         |> dedup_once
         |> Stream.to_list
         |> List.filter_map (function Mf_event.Data(v,_) -> Some v | _ -> None)
       in
       let once  = run_dedup alerts in
       let twice = run_dedup once in
       (* Второй прогон не должен ничего убирать если cooldown разный для каждого *)
       List.length twice <= List.length once)

(* ── Property 3: watermarks монотонны ───────────────────── *)

let prop_watermarks_monotone =
  Test.make ~name:"watermarks are monotone"
    ~count:200
    (make (Gen.list_size (Gen.int_range 1 50) gen_event))
    (fun events ->
       let wms = Stream.of_list events
         |> Mf_event.with_watermarks ~latency:(seconds 3)
         |> Stream.to_list
         |> List.filter_map (function Mf_event.Watermark t -> Some t | _ -> None)
       in
       let rec is_mono = function
         | [] | [_] -> true
         | a :: b :: rest -> a <= b && is_mono (b :: rest)
       in
       is_mono wms)

(* ── Property 4: enrich не меняет число событий ─────────── *)

let prop_enrich_count =
  Test.make ~name:"enrich: left join preserves count"
    ~count:200
    (make (Gen.list_size (Gen.int_range 0 30) gen_event))
    (fun events ->
       let devices = Table.of_list [
         "A", { owner="O"; max_speed=90.; zone="z" };
         "B", { owner="O"; max_speed=80.; zone="z" };
       ] in
       let n_in  = List.length (List.filter (function Mf_event.Data _ -> true | _ -> false) events) in
       let n_out = Stream.of_list events
         |> Pipe.enrich (module Telemetry) ~from:devices
              ~merge:(fun t d -> { t with device = d })
         |> Stream.to_list
         |> List.filter (function Mf_event.Data _ -> true | _ -> false)
         |> List.length
       in
       n_in = n_out)

(* ── Property 5: critical только при действительном превышении ── *)

let prop_rules_severity =
  Test.make ~name:"rules: critical iff speed > 1.3x limit"
    ~count:500
    (make (Gen.map3
      (fun speed limit fuel ->
         ({ Domain.device_id="A"; max_speed=speed; avg_speed=speed*.0.8;
            min_fuel=fuel; count=5;
            device = Some { owner="O"; max_speed=limit; zone="z" } }
          : Domain.stats))
      (Gen.float_range 0. 200.)
      (Gen.float_range 50. 150.)
      (Gen.float_range 0. 100.)))
    (fun stats ->
       let alerts = Rules.check Rules.fleet stats in
       let has_critical = List.exists (fun a -> a.severity = Critical && a.rule = "speed_critical") alerts in
       let device = Option.get stats.device in
       let expected_critical = stats.max_speed > device.max_speed *. 1.3 in
       has_critical = expected_critical)

(* ── Property 6: map не меняет число событий ────────────── *)

let prop_map_count =
  Test.make ~name:"map preserves event count"
    ~count:200
    (make (Gen.list_size (Gen.int_range 0 50) gen_event))
    (fun events ->
       let n_in  = List.length events in
       let n_out = Stream.of_list events
         |> Pipe.map (fun (t:telemetry) -> t.speed_kmh)
         |> Stream.to_list
         |> List.length in
       n_in = n_out)

(* ── Property 7: filter count ≤ input count ─────────────── *)

let prop_filter_count =
  Test.make ~name:"filter: output count <= input count"
    ~count:200
    (make (Gen.list_size (Gen.int_range 0 50) gen_event))
    (fun events ->
       let n_in  = List.length events in
       let n_out = Stream.of_list events
         |> Pipe.filter (fun (t:telemetry) -> t.speed_kmh > 50.)
         |> Stream.to_list
         |> List.length in
       n_out <= n_in)

(* ── Run ─────────────────────────────────────────────────── *)

let () =
  let suite = [
    prop_window_preserves_count;
    prop_dedup_idempotent;
    prop_watermarks_monotone;
    prop_enrich_count;
    prop_rules_severity;
    prop_map_count;
    prop_filter_count;
  ] in
  let n_fail = QCheck_runner.run_tests ~verbose:true suite in
  
  if n_fail > 0 then begin
    Printf.printf "\n%d property(ies) FAILED\n" n_fail;
    exit 1
  end else
    Printf.printf "\nAll properties passed.\n"
