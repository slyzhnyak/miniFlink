(** Микро-бенчмарки операторов miniFlink на ex07-сценарии.

    Цель — найти что именно жрёт время в [median_rssi] пайплайне,
    разбив его на составляющие. Каждый замер — отдельный оператор
    «в одиночку», на одном и том же датасете. Получаем breakdown
    «сколько % времени уходит куда» — основа для решения, что
    оптимизировать первым.

    Каждый микро-бенчмарк замеряется с warmup + 3 прогонами через
    {!Bench_stats}; отчёт — min/median/p95.

    Дизайнерское решение: НЕ изолировать операторы от их предусловий.
    Например, [window_agg_keyed] требует watermarks, поэтому в его
    замер включён [event_time]. Так измеряем «реальную цену», а не
    цену оператора в вакууме. *)

open Miniflink
open Time
open Ex07_location_lib
open Domain

(* Small-конфигурация для итерации: 4×16 биконов + 4×32 шахтёров × 15 мин *)
let bench_config = {
  Mock_source.Large_mine.default_config with
  horizons = 4; beacons_per_horizon = 16;
  miners_per_horizon = 32; simulation_minutes = 15
}

module Src = Mock_source.Large_mine.Make (struct let config = bench_config end)

let runs = 3

(* Кэшируем список пакетов один раз — он одинаков во всех прогонах *)
let packets = lazy (Pipe.collect (Src.read ()))

let total_events () = List.length (Lazy.force packets)

module ByLamp = Keyed.Make (struct type t = Domain.packet let key p = p.Domain.lamp end)

let () =
  Printf.printf "═══════════════════════════════════════════════════════════════\n";
  Printf.printf "  Микро-бенчмарки операторов на ex07-сценарии\n";
  Printf.printf "═══════════════════════════════════════════════════════════════\n\n";

  Printf.printf "Конфигурация:\n";
  List.iter (fun s -> Printf.printf "  %s\n" s) (Src.stats ());
  let n = total_events () in
  Printf.printf "  пакетов в потоке: %d\n\n" n;

  Printf.printf "Микро-бенчмарки (median, ev/s):\n\n";

  (* 1. Чистый source — нижняя граница, цена итерации через Stream *)
  let _, st_source =
    Bench_stats.run_many ~name:"1. source.read + collect" ~runs (fun () ->
      Src.read () |> Pipe.collect) in
  Bench_stats.print_stats st_source;

  (* 2. + dedup — фильтрация дублей by (lamp, ts) *)
  let _, st_dedup =
    Bench_stats.run_many ~name:"2. + dedup" ~runs (fun () ->
      Src.read ()
      |> Pipe.dedup (module ByLamp)
           ~rule:(fun p -> string_of_int p.Domain.ts)
           ~cooldown:(seconds 120)
      |> Pipe.collect) in
  Bench_stats.print_stats st_dedup;

  (* 3. + flat_map (разворот пакета в reading'и) — каждый pkt в 5 reading *)
  let _, st_flatmap =
    Bench_stats.run_many ~name:"3. + flat_map (× 5 readings)" ~runs (fun () ->
      Src.read ()
      |> Pipe.dedup (module ByLamp)
           ~rule:(fun p -> string_of_int p.Domain.ts)
           ~cooldown:(seconds 120)
      |> Pipe.flat_map (fun p ->
           List.map (fun (b, rssi) ->
             { Domain.r_lamp = p.lamp; r_beacon = b; r_rssi = rssi; r_ts = p.ts })
             p.readings)
      |> Pipe.collect) in
  Bench_stats.print_stats st_flatmap;

  (* 4. + event_time — вставка watermarks *)
  let _, st_evtime =
    Bench_stats.run_many ~name:"4. + event_time" ~runs (fun () ->
      Src.read ()
      |> Pipe.dedup (module ByLamp)
           ~rule:(fun p -> string_of_int p.Domain.ts)
           ~cooldown:(seconds 120)
      |> Pipe.flat_map (fun p ->
           List.map (fun (b, rssi) ->
             { Domain.r_lamp = p.lamp; r_beacon = b; r_rssi = rssi; r_ts = p.ts })
             p.readings)
      |> Pipe.event_time ~lateness:(seconds 1)
      |> Pipe.collect) in
  Bench_stats.print_stats st_evtime;

  (* 5. + window_agg_keyed с count_if — самое простое окно (без group_by) *)
  let _, st_window_simple =
    Bench_stats.run_many ~name:"5. + window_agg count (simple)" ~runs (fun () ->
      Src.read ()
      |> Pipe.dedup (module ByLamp)
           ~rule:(fun p -> string_of_int p.Domain.ts)
           ~cooldown:(seconds 120)
      |> Pipe.flat_map (fun p ->
           List.map (fun (b, rssi) ->
             { Domain.r_lamp = p.lamp; r_beacon = b; r_rssi = rssi; r_ts = p.ts })
             p.readings)
      |> Pipe.event_time ~lateness:(seconds 1)
      |> Pipe.window_agg_keyed ~by:(fun r -> r.Domain.r_lamp)
           ~allowed_lateness:(seconds 60)
           (Pipe.sliding (seconds 60) (seconds 5))
           (Agg.count_if (fun _ -> true))
      |> Stream.to_list) in
  Bench_stats.print_stats st_window_simple;

  (* 6. + window_agg с median (одиночный) — без group_by *)
  let _, st_window_median =
    Bench_stats.run_many ~name:"6. + window_agg median (no group_by)" ~runs (fun () ->
      Src.read ()
      |> Pipe.dedup (module ByLamp)
           ~rule:(fun p -> string_of_int p.Domain.ts)
           ~cooldown:(seconds 120)
      |> Pipe.flat_map (fun p ->
           List.map (fun (b, rssi) ->
             { Domain.r_lamp = p.lamp; r_beacon = b; r_rssi = rssi; r_ts = p.ts })
             p.readings)
      |> Pipe.event_time ~lateness:(seconds 1)
      |> Pipe.window_agg_keyed ~by:(fun r -> r.Domain.r_lamp)
           ~allowed_lateness:(seconds 60)
           (Pipe.sliding (seconds 60) (seconds 5))
           (Agg.median (fun r -> r.Domain.r_rssi))
      |> Stream.to_list) in
  Bench_stats.print_stats st_window_median;

  (* 7. + group_by + median per beacon (без top_k_by) — добавляем уровень вложенности *)
  let _, st_groupby =
    Bench_stats.run_many ~name:"7. + group_by(median per beacon)" ~runs (fun () ->
      Src.read ()
      |> Pipe.dedup (module ByLamp)
           ~rule:(fun p -> string_of_int p.Domain.ts)
           ~cooldown:(seconds 120)
      |> Pipe.flat_map (fun p ->
           List.map (fun (b, rssi) ->
             { Domain.r_lamp = p.lamp; r_beacon = b; r_rssi = rssi; r_ts = p.ts })
             p.readings)
      |> Pipe.event_time ~lateness:(seconds 1)
      |> Pipe.window_agg_keyed ~by:(fun r -> r.Domain.r_lamp)
           ~allowed_lateness:(seconds 60)
           (Pipe.sliding (seconds 60) (seconds 5))
           Agg.(group_by
                  ~key:(fun r -> r.Domain.r_beacon)
                  ~inner:(median (fun r -> r.Domain.r_rssi)))
      |> Stream.to_list) in
  Bench_stats.print_stats st_groupby;

  (* 8. ПОЛНЫЙ median_rssi из Pipelines — для сравнения *)
  let _, st_full =
    Bench_stats.run_many ~name:"8. = Pipelines.median_rssi (full)" ~runs (fun () ->
      Src.read () |> Pipelines.median_rssi |> Stream.to_list) in
  Bench_stats.print_stats st_full;

  (* 9. connectivity_alerts (для contrast) *)
  let _, st_alerts =
    Bench_stats.run_many ~name:"9. Pipelines.connectivity_alerts" ~runs (fun () ->
      Src.read () |> Pipelines.connectivity_alerts |> Pipe.collect) in
  Bench_stats.print_stats st_alerts;

  print_newline ();
  Printf.printf "Cumulative breakdown (вклад каждого слоя):\n";
  let pct s = s.Bench_stats.s_median /. st_full.Bench_stats.s_median *. 100. in
  Printf.printf "  source.read+collect:     %5.1f%%  (cost of consuming the stream)\n" (pct st_source);
  Printf.printf "  + dedup:                 %5.1f%%\n" (pct st_dedup);
  Printf.printf "  + flat_map:              %5.1f%%\n" (pct st_flatmap);
  Printf.printf "  + event_time:            %5.1f%%\n" (pct st_evtime);
  Printf.printf "  + window count:          %5.1f%%\n" (pct st_window_simple);
  Printf.printf "  + window median (flat):  %5.1f%%\n" (pct st_window_median);
  Printf.printf "  + window group_by+med:   %5.1f%%\n" (pct st_groupby);
  Printf.printf "  full pipeline:           %5.1f%%\n\n" (pct st_full);

  Printf.printf "Δ-вклад (incremental: что добавил каждый шаг):\n";
  let delta a b = b.Bench_stats.s_median -. a.Bench_stats.s_median in
  let pct_delta a b = delta a b /. st_full.Bench_stats.s_median *. 100. in
  Printf.printf "  dedup:               +%5.1f%% (%.3fс)\n" (pct_delta st_source st_dedup) (delta st_source st_dedup);
  Printf.printf "  flat_map:            +%5.1f%% (%.3fс)\n" (pct_delta st_dedup st_flatmap) (delta st_dedup st_flatmap);
  Printf.printf "  event_time:          +%5.1f%% (%.3fс)\n" (pct_delta st_flatmap st_evtime) (delta st_flatmap st_evtime);
  Printf.printf "  window (count):      +%5.1f%% (%.3fс)\n" (pct_delta st_evtime st_window_simple) (delta st_evtime st_window_simple);
  Printf.printf "  median agg:          +%5.1f%% (%.3fс)\n" (pct_delta st_window_simple st_window_median) (delta st_window_simple st_window_median);
  Printf.printf "  group_by overhead:   +%5.1f%% (%.3fс)\n" (pct_delta st_window_median st_groupby) (delta st_window_median st_groupby);
  Printf.printf "  top_k_by + map:      +%5.1f%% (%.3fс)\n" (pct_delta st_groupby st_full) (delta st_groupby st_full);
  print_newline ();

  Printf.printf "═══════════════════════════════════════════════════════════════\n"
