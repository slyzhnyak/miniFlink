(** Параллельный ex07 через fan_out.

    Применяем {!Parallel.run_parallel_simple} к пайплайнам ex07:
    события партиционируются по [lamp], каждый воркер прогоняет
    {!Pipelines.median_rssi} или {!Pipelines.connectivity_alerts}
    независимо. На i7-8700 (12 ядер) ожидается ~4-5x ускорение
    `connectivity_alerts`; `median_rssi` лимитирован GC из-за окон,
    масштабирование может быть слабее.

    Sink: счётчик через {!Atomic} (не mutex — иначе колено как в
    bench_scaling). Atomic.incr на OCaml 5 lock-free, не сериализует
    воркеров. Это меняет шкалу — мерим именно pipeline, не sink.

    Заметка: {!Pipelines.median_rssi} и {!Pipelines.connectivity_alerts}
    в библиотеке остаются sequential. Параллельность — инфраструктурная
    забота, не часть бизнес-логики. В прода-сервисе пользователь сам
    выбирает: один поток для простоты, или fan_out с N воркеров для
    throughput. *)

open Miniflink
open Ex07_location_lib
open Domain

(* Конфигурация Large_mine. Полная (16x64 + 16x256) для реалистичной
   нагрузки; на CI можно временно уменьшить через scale. *)
let scale = `Full

let bench_config = match scale with
  | `Smoke ->
    { Mock_source.Large_mine.default_config with
      horizons = 4; beacons_per_horizon = 16;
      miners_per_horizon = 32; simulation_minutes = 15 }
  | `Full -> Mock_source.Large_mine.default_config

module Src = Mock_source.Large_mine.Make (struct let config = bench_config end)

let runs = 3   (* + 1 warmup *)

let n_packets () =
  bench_config.horizons
  * bench_config.miners_per_horizon
  * (bench_config.simulation_minutes * 60 / bench_config.step_seconds)

(* ─── Sequential baseline ─────────────────────────────────── *)

let bench_sequential_alerts () =
  let n = ref 0 in
  Src.read ()
  |> Pipelines.connectivity_alerts
  |> Stream.iter (function
       | Mf_event.Data _ -> incr n
       | _ -> ());
  !n

let bench_sequential_median () =
  let n = ref 0 in
  Src.read ()
  |> Pipelines.median_rssi
  |> Stream.iter (function
       | Mf_event.Data _ -> incr n
       | _ -> ());
  !n

(* ─── Parallel через run_parallel_simple ───────────────────── *)

(* Pipeline-функция должна принимать packet-стрим и выдавать стрим
   результата. Для connectivity_alerts — alert. Для median_rssi — location. *)

let bench_parallel_alerts ~workers () =
  let n = Atomic.make 0 in
  Parallel.run_parallel_simple
    ~workers
    ~capacity:4096
    ~key_of:(fun (p : packet) -> p.lamp)
    ~pipeline:Pipelines.connectivity_alerts
    ~source:(Src.read ())
    ~sink:(fun _alert -> Atomic.incr n)
    ();
  Atomic.get n

let bench_parallel_median ~workers () =
  let n = Atomic.make 0 in
  Parallel.run_parallel_simple
    ~workers
    ~capacity:4096
    ~key_of:(fun (p : packet) -> p.lamp)
    ~pipeline:Pipelines.median_rssi
    ~source:(Src.read ())
    ~sink:(fun _loc -> Atomic.incr n)
    ();
  Atomic.get n

(* ─── Main ─────────────────────────────────────────────────── *)

let () =
  Printf.printf "═══════════════════════════════════════════════════════════════\n";
  Printf.printf "  Параллельный ex07 через fan_out\n";
  Printf.printf "  OCaml версия: %s\n" Sys.ocaml_version;
  Printf.printf "═══════════════════════════════════════════════════════════════\n\n";

  List.iter (fun s -> Printf.printf "  %s\n" s) (Src.stats ());
  Printf.printf "  прогонов: %d (+ 1 warmup) на каждую конфигурацию\n\n" runs;

  let n_pkts = n_packets () in
  let report name n_ev st =
    let tput = float_of_int n_ev /. st.Bench_stats.s_median in
    Printf.printf "  %-30s  med %.2fс  p95 %.2fс  %s ev/s\n"
      name st.s_median st.s_p95
      (if tput >= 1e6 then Printf.sprintf "%.2fM" (tput /. 1e6)
       else Printf.sprintf "%.0fK" (tput /. 1000.))
  in

  (* ─── connectivity_alerts ──────────────────────────────── *)
  Printf.printf "── connectivity_alerts ────────────────────────────────────────\n";
  let _, st_seq_a =
    Bench_stats.run_many ~name:"sequential" ~runs bench_sequential_alerts in
  report "sequential" n_pkts st_seq_a;
  let tput_seq_a = float_of_int n_pkts /. st_seq_a.s_median in

  let workers_list = [1; 2; 4; 6; 8; 12] in
  let alerts_results = List.map (fun w ->
    let name = Printf.sprintf "parallel %2d" w in
    let _, st = Bench_stats.run_many ~name ~runs (bench_parallel_alerts ~workers:w) in
    report name n_pkts st;
    (w, st)
  ) workers_list in

  Printf.printf "\n  Таблица:\n";
  Printf.printf "  | воркеров   | время (с) | ev/s   | speedup |\n";
  Printf.printf "  |------------|-----------|--------|---------|\n";
  Printf.printf "  | sequential | %8.2f  | %5.0fK | 1.00x   |\n"
    st_seq_a.s_median (tput_seq_a /. 1000.);
  List.iter (fun (w, st) ->
    let tput = float_of_int n_pkts /. st.Bench_stats.s_median in
    let speedup = st_seq_a.s_median /. st.Bench_stats.s_median in
    let tput_str =
      if tput >= 1e6 then Printf.sprintf "%4.2fM" (tput /. 1e6)
      else Printf.sprintf "%5.0fK" (tput /. 1000.) in
    Printf.printf "  | %2d         | %8.2f  | %s | %4.2fx  |\n"
      w st.s_median tput_str speedup
  ) alerts_results;

  (* ─── median_rssi ──────────────────────────────────────── *)
  Printf.printf "\n── median_rssi ────────────────────────────────────────────────\n";
  let _, st_seq_m =
    Bench_stats.run_many ~name:"sequential" ~runs bench_sequential_median in
  report "sequential" n_pkts st_seq_m;
  let tput_seq_m = float_of_int n_pkts /. st_seq_m.s_median in

  let median_results = List.map (fun w ->
    let name = Printf.sprintf "parallel %2d" w in
    let _, st = Bench_stats.run_many ~name ~runs (bench_parallel_median ~workers:w) in
    report name n_pkts st;
    (w, st)
  ) workers_list in

  Printf.printf "\n  Таблица:\n";
  Printf.printf "  | воркеров   | время (с) | ev/s   | speedup |\n";
  Printf.printf "  |------------|-----------|--------|---------|\n";
  Printf.printf "  | sequential | %8.2f  | %5.0fK | 1.00x   |\n"
    st_seq_m.s_median (tput_seq_m /. 1000.);
  List.iter (fun (w, st) ->
    let tput = float_of_int n_pkts /. st.Bench_stats.s_median in
    let speedup = st_seq_m.s_median /. st.Bench_stats.s_median in
    let tput_str =
      if tput >= 1e6 then Printf.sprintf "%4.2fM" (tput /. 1e6)
      else Printf.sprintf "%5.0fK" (tput /. 1000.) in
    Printf.printf "  | %2d         | %8.2f  | %s | %4.2fx  |\n"
      w st.s_median tput_str speedup
  ) median_results;

  Printf.printf "\n═══════════════════════════════════════════════════════════════\n"
