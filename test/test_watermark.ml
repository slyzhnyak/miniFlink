open Miniflink
(* Тест 1.3: периодическая эмиссия watermark + финальный flush *)
open Time

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name
let check_eq name a b = if a=b then pass name
  else fail (Printf.sprintf "%s: expected %d got %d" name b a)

let wms_of stream =
  Stream.to_list stream
  |> List.filter_map (function Mf_event.Watermark t -> Some t | _ -> None)

(* ── Периодичность: interval уменьшает число watermark-ов ──── *)
let test_interval () =
  Printf.printf "\n-- Periodic emission reduces watermark count\n";
  (* 100 событий с шагом 1с, latency 0 *)
  let events = List.init 100 (fun i -> Mf_event.data "k" (i * 1000)) in

  (* interval=0 (старое поведение): watermark на каждый максимум *)
  let n_dense = List.length (wms_of
    (Stream.of_list events |> Mf_event.with_watermarks_ext ~latency:0 ~interval:0)) in

  (* interval=10s: watermark раз в ~10 событий *)
  let n_sparse = List.length (wms_of
    (Stream.of_list events |> Mf_event.with_watermarks_ext ~latency:0 ~interval:(seconds 10))) in

  Printf.printf "    dense=%d sparse=%d\n%!" n_dense n_sparse;
  check "interval=0 emits many watermarks" (n_dense >= 90);
  check "interval=10s emits far fewer" (n_sparse <= 15);
  check "sparse < dense" (n_sparse < n_dense)

(* ── Финальный flush: последнее окно закрывается ──────────── *)
let test_final_flush () =
  Printf.printf "\n-- Final watermark flushes trailing window\n";
  let tlm t = { Test_support.Domain.device_id="A"; speed_kmh=60.; fuel_pct=80.;
    position={Test_support.Domain.lat=55.;lon=37.}; ts=t; device=None } in
  let events = [
    Mf_event.data (tlm 5000) 5000;
    Mf_event.data (tlm 27000) 27000;   (* max_seen=27000 *)
  ] in
  let out = Stream.of_list events
    |> Mf_event.with_watermarks_ext ~latency:(seconds 3) ~interval:0
    |> Pipe.window (module Test_support.Domain.Telemetry) (Pipe.tumbling (seconds 30))
    |> Stream.to_list in
  let wms = List.filter_map (function Mf_event.Watermark t -> Some t | _ -> None) out in
  check "final watermark = max_seen (27000)" (List.mem 27000 wms)

(* ── Монотонность сохраняется с interval ──────────────────── *)
let test_monotone () =
  Printf.printf "\n-- Watermarks monotone with interval\n";
  let events = List.init 50 (fun i ->
    Mf_event.data "k" ((i * 137 + 1000) mod 50000)) in  (* перемешанное время *)
  let wms = wms_of (Stream.of_list events
    |> Mf_event.with_watermarks_ext ~latency:(seconds 2) ~interval:(seconds 5)) in
  let rec mono = function a::(b::_ as r) -> a<=b && mono r | _ -> true in
  check "monotone under interval + reorder" (mono wms)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Watermark: periodic emission + flush\n";
  Printf.printf "==========================================\n";
  test_interval ();
  test_final_flush ();
  test_monotone ();
  Printf.printf "\nAll watermark tests passed.\n"
