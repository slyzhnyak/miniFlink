open Miniflink
(* Unit тесты для ядра miniFlink *)

open Test_support.Domain
open Time

let pass name = Printf.printf "  OK %s\n%!" name
let fail name msg = Printf.printf "  FAIL %s: %s\n%!" name msg; exit 1
let check name cond = if cond then pass name else fail name "assertion failed"
let check_eq name a b = if a = b then pass name
  else fail name (Printf.sprintf "expected %d, got %d" b a)

let devices = Table.of_list [
  "A", { owner="O"; max_speed=90.; zone="z" };
  "B", { owner="O"; max_speed=80.; zone="z" };
]

(* ── 1. Stream laws ─────────────────────────────────────── *)
let test_stream () =
  Printf.printf "\n-- Stream laws\n";
  let xs = [1;2;3;4;5] in
  let r = Stream.of_list xs |> Stream.map Fun.id |> Stream.to_list in
  check "map id = id" (r = xs);
  let r = Stream.of_list xs |> Stream.filter (fun _ -> true) |> Stream.to_list in
  check "filter true = id" (r = xs);
  let r = Stream.of_list xs |> Stream.filter (fun _ -> false) |> Stream.to_list in
  check "filter false = []" (r = []);
  let f x = x * 2 and g x = x + 1 in
  let r1 = Stream.of_list xs |> Stream.map g |> Stream.map f |> Stream.to_list in
  let r2 = Stream.of_list xs |> Stream.map (fun x -> f (g x)) |> Stream.to_list in
  check "map composition" (r1 = r2);
  let n = Stream.of_list xs |> Stream.flat_map (fun x -> [x;x])
          |> Stream.fold (fun a _ -> a+1) 0 in
  check_eq "flat_map doubles count" n (List.length xs * 2)

(* ── 2. Watermark monotonicity ──────────────────────────── *)
let test_watermarks () =
  Printf.printf "\n-- Watermark monotonicity\n";
  let events = [
    Mf_event.data "a" 5000;
    Mf_event.data "b" 3000;
    Mf_event.data "c" 8000;
    Mf_event.data "d" 1000;
    Mf_event.data "e" 10000;
  ] in
  let wms = Stream.of_list events
    |> Mf_event.with_watermarks ~latency:(seconds 2)
    |> Stream.to_list
    |> List.filter_map (function Mf_event.Watermark t -> Some t | _ -> None) in
  let is_monotone = function
    | [] | [_] -> true
    | lst ->
      List.for_all2 (fun a b -> a <= b)
        (List.filteri (fun i _ -> i < List.length lst - 1) lst)
        (List.tl lst)
  in
  check "watermarks monotone" (is_monotone wms);
  (* Все watermarks <= max_ts (никогда не опережают виденное время).
     Промежуточные = max-latency; финальный flush на конце потока = max_ts. *)
  check "watermarks never exceed max_ts" (List.for_all (fun t -> t <= 10000) wms);
  (* Хотя бы один промежуточный watermark учитывает latency *)
  check "intermediate watermarks respect latency"
    (List.exists (fun t -> t <= 10000 - seconds 2) wms)

(* ── 3. Window correctness ──────────────────────────────── *)
let test_window () =
  Printf.printf "\n-- Window correctness\n";
  let mk t s = { Test_support.Domain.device_id="A"; speed_kmh=s; fuel_pct=80.; position={Test_support.Domain.lat=55.;lon=37.}; ts=t*1000; device=None } in

  (* 3 events in one 30s window *)
  let ctx = Test_support.Harness.create (fun src ->
    src
    |> Mf_event.with_watermarks ~latency:(seconds 3)
    |> Pipe.window (module Telemetry) (Pipe.tumbling (seconds 30))
    |> Pipe.aggregate (fun _k events -> List.length events)) in
  Test_support.Harness.push ctx (mk 5 60.) ~ts:5000;
  Test_support.Harness.push ctx (mk 10 65.) ~ts:10000;
  Test_support.Harness.push ctx (mk 20 70.) ~ts:20000;
  Test_support.Harness.push_wm ctx ~ts:35000;
  let counts = Test_support.Harness.run ctx in
  check_eq "3 events in 1 window" (List.hd counts) 3;

  (* Events split across 2 windows *)
  let ctx2 = Test_support.Harness.create (fun src ->
    src
    |> Mf_event.with_watermarks ~latency:(seconds 3)
    |> Pipe.window (module Telemetry) (Pipe.tumbling (seconds 30))
    |> Pipe.aggregate (fun k _ev -> k)) in
  Test_support.Harness.push ctx2 (mk 5 60.) ~ts:5000;
  Test_support.Harness.push ctx2 (mk 35 65.) ~ts:35000;
  Test_support.Harness.push_wm ctx2 ~ts:65000;
  let wins = Test_support.Harness.run ctx2 in
  check_eq "events in 2 windows" (List.length wins) 2

(* ── 4. Dedup cooldown ──────────────────────────────────── *)
let test_dedup () =
  Printf.printf "\n-- Dedup cooldown\n";
  let mk rule ts =
    { Test_support.Domain.id=""; device_id="A"; rule; severity=Warning; message=""; ts } in
  let ctx = Test_support.Harness.create (fun src ->
    src |> Pipe.dedup (module Alert) ~rule:(fun a -> a.rule) ~cooldown:(minutes 5)) in
  Test_support.Harness.push ctx (mk "speed" 1000)   ~ts:1000;    (* passes *)
  Test_support.Harness.push ctx (mk "speed" 61000)  ~ts:61000;   (* suppressed: 1 min < 5 min *)
  Test_support.Harness.push ctx (mk "speed" 361000) ~ts:361000;  (* passes: 6 min > 5 min *)
  Test_support.Harness.push ctx (mk "fuel"  2000)   ~ts:2000;    (* passes: different rule *)
  let r = Test_support.Harness.run ctx in
  check_eq "dedup: 4 in, 3 out" (List.length r) 3;
  check "dedup: first passes"    (List.exists (fun a -> a.ts = 1000)   r);
  check "dedup: suppressed"      (not (List.exists (fun a -> a.ts = 61000) r));
  check "dedup: after cooldown"  (List.exists (fun a -> a.ts = 361000) r)

(* ── 5. Enrich left join ────────────────────────────────── *)
let test_enrich () =
  Printf.printf "\n-- Enrich left join\n";
  let ctx = Test_support.Harness.create (fun src ->
    src |> Pipe.enrich (module Telemetry) ~from:devices
           ~merge:(fun t d -> { t with device = d })) in
  Test_support.Harness.push ctx { Test_support.Domain.device_id="A"; speed_kmh=60.; fuel_pct=80.; position={Test_support.Domain.lat=55.;lon=37.}; ts=1000; device=None } ~ts:1000;
  Test_support.Harness.push ctx { Test_support.Domain.device_id="B"; speed_kmh=70.; fuel_pct=70.; position={Test_support.Domain.lat=55.;lon=37.}; ts=2000; device=None } ~ts:2000;
  Test_support.Harness.push ctx { Test_support.Domain.device_id="X"; speed_kmh=50.; fuel_pct=90.; position={Test_support.Domain.lat=55.;lon=37.}; ts=3000; device=None } ~ts:3000;  (* unknown device *)
  let r = Test_support.Harness.run ctx in
  check_eq "enrich: all 3 pass" (List.length r) 3;
  check "enrich: A has device" ((List.nth r 0).device <> None);
  check "enrich: X has None"   ((List.nth r 2).device = None);
  check "enrich: A max_speed=90"
    (Option.get (List.nth r 0).device |> fun d -> d.max_speed = 90.)

(* ── 6. Rules correctness ───────────────────────────────── *)
let test_rules () =
  Printf.printf "\n-- Rules\n";
  let mk_stats max_speed min_fuel device =
    { Test_support.Domain.device_id="A"; max_speed; avg_speed=max_speed*.0.8;
      min_fuel; count=5; device } in

  let r1 = Test_support.Rules.check Test_support.Rules.fleet
    (mk_stats 95. 50. (Some { owner="O"; max_speed=90.; zone="z" })) in
  check "speed > limit → warning" (List.exists (fun a -> a.rule = "speed_warning") r1);

  let r2 = Test_support.Rules.check Test_support.Rules.fleet
    (mk_stats 120. 50. (Some { owner="O"; max_speed=90.; zone="z" })) in
  check "speed > 1.3x → critical" (List.exists (fun a -> a.rule = "speed_critical") r2);

  let r3 = Test_support.Rules.check Test_support.Rules.fleet (mk_stats 60. 15. None) in
  check "fuel < 20% → warning" (List.exists (fun a -> a.rule = "fuel_warning") r3);

  let r4 = Test_support.Rules.check Test_support.Rules.fleet (mk_stats 60. 8. None) in
  check "fuel < 10% → critical" (List.exists (fun a -> a.rule = "fuel_critical") r4);

  let r5 = Test_support.Rules.check Test_support.Rules.fleet
    (mk_stats 80. 50. (Some { owner="O"; max_speed=90.; zone="z" })) in
  check "normal → no alerts" (r5 = [])

(* ── 7. Full pipeline integration ──────────────────────── *)
let test_full_pipeline () =
  Printf.printf "\n-- Full pipeline\n";
  let pipeline src =
    src
    |> Mf_event.with_watermarks ~latency:(seconds 3)
    |> Pipe.enrich (module Telemetry) ~from:devices
         ~merge:(fun t d -> { t with device = d })
    |> Pipe.window (module Telemetry) (Pipe.tumbling (seconds 30))
    |> Pipe.aggregate Test_support.Rules.compute
    |> Pipe.flat_map (Test_support.Rules.check Test_support.Rules.fleet)
    |> Pipe.dedup (module Alert) ~rule:(fun a -> a.rule) ~cooldown:(minutes 5)
  in
  let ctx = Test_support.Harness.create pipeline in
  List.iter (fun (id,t,s,f) ->
    Test_support.Harness.push ctx { Test_support.Domain.device_id=id; speed_kmh=s; fuel_pct=f; position={Test_support.Domain.lat=55.;lon=37.}; ts=t*1000; device=None } ~ts:(t*1000)
  ) [
    "A",  0,  60., 80.;
    "B",  2, 100., 60.;
    "A",  5,  65., 79.;
    "B",  7, 145., 58.;
    "A", 60,  68., 75.;
  ];
  Test_support.Harness.push_wm ctx ~ts:65000;
  let alerts = Test_support.Harness.run ctx in
  check "got alerts" (alerts <> []);
  check "truck_B speed_critical"
    (List.exists (fun a -> a.device_id = "B" && a.rule = "speed_critical") alerts);
  Printf.printf "  alerts: [%s]\n%!"
    (String.concat " " (List.map (fun a ->
      Printf.sprintf "%s/%s" a.device_id a.rule) alerts))

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  miniFlink v3 - Unit Tests\n";
  Printf.printf "==========================================\n";
  test_stream ();
  test_watermarks ();
  test_window ();
  test_dedup ();
  test_enrich ();
  test_rules ();
  test_full_pipeline ();
  Printf.printf "\nAll tests passed.\n"
