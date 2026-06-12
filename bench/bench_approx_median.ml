(** Бенчмарк: Agg.median vs Agg.approx_median на полном пайплайне.

    Меняем ТОЛЬКО агрегат в окне, остальное идентично median_rssi.
    Проверяем что:
    - approx_median даёт ускорение;
    - результаты в пределах ожидаемой погрешности (p² <5% на >100 событиях). *)

open Miniflink
open Time
open Ex07_location_lib
open Domain

let bench_config = {
  Mock_source.Large_mine.default_config with
  horizons = 4; beacons_per_horizon = 16;
  miners_per_horizon = 32; simulation_minutes = 15
}

module Src = Mock_source.Large_mine.Make (struct let config = bench_config end)
module ByLamp = Keyed.Make (struct type t = packet let key p = p.lamp end)

let runs = 3

let prepare () =
  Src.read ()
  |> Pipe.dedup (module ByLamp)
       ~rule:(fun p -> string_of_int p.ts)
       ~cooldown:(seconds 120)
  |> Pipe.flat_map (fun p ->
       List.map (fun (b, rssi) ->
         { r_lamp = p.lamp; r_beacon = b; r_rssi = rssi; r_ts = p.ts })
         p.readings)
  |> Pipe.event_time ~lateness:(seconds 1)

let bench_exact () =
  prepare ()
  |> Pipe.window_agg_keyed ~by:(fun r -> r.r_lamp)
       ~allowed_lateness:(seconds 60)
       (Pipe.sliding (seconds 60) (seconds 5))
       Agg.(
         group_by
           ~key:(fun r -> r.r_beacon)
           ~inner:(median (fun r -> r.r_rssi)))
  |> Stream.to_list

let bench_approx () =
  prepare ()
  |> Pipe.window_agg_keyed ~by:(fun r -> r.r_lamp)
       ~allowed_lateness:(seconds 60)
       (Pipe.sliding (seconds 60) (seconds 5))
       Agg.(
         group_by
           ~key:(fun r -> r.r_beacon)
           ~inner:(approx_median (fun r -> r.r_rssi)))
  |> Stream.to_list

(* Сравнить результаты: для каждой пары (lamp, wend) посмотреть
   суммарное отклонение медианы по биконам. *)
let compare_results exact approx =
  let extract evs =
    List.filter_map (function
      | Mf_event.Data (v, _) -> Some v
      | _ -> None) evs in
  let e = extract exact and a = extract approx in
  if List.length e <> List.length a then
    Printf.printf "  ⚠ DIFF в количестве эмиссий: exact=%d, approx=%d\n"
      (List.length e) (List.length a)
  else begin
    let max_err = ref 0. in
    let sum_err = ref 0. in
    let n = ref 0 in
    List.iter2 (fun (_, e_groups) (_, a_groups) ->
      List.iter2 (fun (_, e_med) (_, a_med) ->
        match e_med, a_med with
        | Some e_v, Some a_v ->
          let err = abs_float (a_v -. e_v) in
          if err > !max_err then max_err := err;
          sum_err := !sum_err +. err;
          incr n
        | _ -> ()
      ) e_groups a_groups
    ) e a;
    Printf.printf "  Сравнение медиан: %d пар (lamp,beacon,окно)\n" !n;
    Printf.printf "  Max ошибка: %.3f dB\n" !max_err;
    Printf.printf "  Avg ошибка: %.3f dB\n" (!sum_err /. float !n)
  end

let () =
  Printf.printf "═══════════════════════════════════════════════════════════════\n";
  Printf.printf "  median vs approx_median на ex07-пайплайне\n";
  Printf.printf "═══════════════════════════════════════════════════════════════\n\n";

  List.iter (fun s -> Printf.printf "  %s\n" s) (Src.stats ());
  print_newline ();

  let res_exact, st_exact =
    Bench_stats.run_many ~name:"Agg.median (exact)" ~runs bench_exact in
  Bench_stats.print_stats st_exact;

  let res_approx, st_approx =
    Bench_stats.run_many ~name:"Agg.approx_median (p²)" ~runs bench_approx in
  Bench_stats.print_stats st_approx;

  print_newline ();
  compare_results res_exact res_approx;
  print_newline ();

  let speedup = st_exact.s_median /. st_approx.s_median in
  Printf.printf "Speedup: %.2fx\n" speedup;
  let alloc_reduction =
    1. -. st_approx.s_mean_alloc_mb /. st_exact.s_mean_alloc_mb in
  Printf.printf "Снижение аллокаций: %.1f%% (%.0fМБ → %.0fМБ)\n"
    (alloc_reduction *. 100.) st_exact.s_mean_alloc_mb st_approx.s_mean_alloc_mb;
  print_newline ();

  (* ── Большие окна: где approx_median должен выиграть ────────
     В ex07-сценарии окна маленькие (~12 показаний на бикон в окне),
     p² не окупается. На больших окнах (тысячи событий) точная
     медиана платит O(n log n) при каждом закрытии, а p² — O(1).
     Проверим на синтетическом datasete: один ключ, 10000 точек
     в одном окне. *)
  Printf.printf "─── Большие окна (10K точек в окне, синтетика) ───\n";
  let big_data () =
    List.init 10000 (fun i ->
      Mf_event.data (float (i mod 1000)) (i * 100)) in
  let bench_exact_big () =
    Stream.of_list (big_data ())
    |> Pipe.event_time ~lateness:0
    |> Pipe.window_agg_keyed ~by:(fun _ -> "all")
         (Pipe.tumbling 1_000_000)
         (Agg.median (fun x -> x))
    |> Stream.to_list in
  let bench_approx_big () =
    Stream.of_list (big_data ())
    |> Pipe.event_time ~lateness:0
    |> Pipe.window_agg_keyed ~by:(fun _ -> "all")
         (Pipe.tumbling 1_000_000)
         (Agg.approx_median (fun x -> x))
    |> Stream.to_list in
  let _, st_e_big = Bench_stats.run_many ~name:"big median" ~runs bench_exact_big in
  let _, st_a_big = Bench_stats.run_many ~name:"big approx_median" ~runs bench_approx_big in
  Bench_stats.print_stats st_e_big;
  Bench_stats.print_stats st_a_big;
  Printf.printf "  Speedup (big window): %.2fx\n"
    (st_e_big.s_median /. st_a_big.s_median);
  Printf.printf "  Allocations: %.0fМБ → %.0fМБ\n"
    st_e_big.s_mean_alloc_mb st_a_big.s_mean_alloc_mb;

  Printf.printf "═══════════════════════════════════════════════════════════════\n"
