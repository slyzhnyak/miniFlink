(** Бенчмарк ex07 на большой шахте.

    Конфигурация по умолчанию: 16 горизонтов × 64 биконов = 1024
    биконов; 16 × 256 = 4096 шахтёров; ~1 час симуляции = ~1M пакетов.

    Прогон: 1 warmup + N измерений. На каждом — wall-time, allocated,
    heap. Доверять стоит min/median/p95, не среднему: GC-пики иногда
    задирают max. Для regression-сравнения «было vs стало» сравнивать
    p95 (стабильнее median при шумах). *)

open Miniflink
open Ex07_location_lib

(* Конфигурация: переключи между [`Small] и [`Full].

   Small — 4×16 биконов + 4×32 шахтёров × 15 мин ≈ 8K пакетов.
     Прогон <1 сек. Используется для итерации, CI, регрессии.
   Full  — 16×64 + 16×256 × 60 мин ≈ ~950K пакетов.
     Прогон ~90 сек на пайплайн × N измерений = несколько минут.
     Используется когда нужны цифры под отчёт.

   Между ними важно: разница НЕ только в скорости. Small может
   скрыть проблемы масштаба (квадратичные алгоритмы заметны только
   на Full). Перед оптимизацией смотреть ОБА. *)
let scale = `Small

let bench_config = match scale with
  | `Small ->
    { Mock_source.Large_mine.default_config with
      horizons = 4; beacons_per_horizon = 16;
      miners_per_horizon = 32; simulation_minutes = 15 }
  | `Full -> Mock_source.Large_mine.default_config

module Src = Mock_source.Large_mine.Make (struct let config = bench_config end)

let runs = 3   (* плюс 1 warmup; больше для CI стабильности, мы экономим время *)

let count_by_type alerts =
  let c_no_pkt   = ref 0 in
  let c_no_rd    = ref 0 in
  let c_no_mot   = ref 0 in
  let c_low_v    = ref 0 in
  let c_v_ok     = ref 0 in
  let c_sos      = ref 0 in
  List.iter (function
    | Domain.No_packets _  -> incr c_no_pkt
    | Domain.No_readings _ -> incr c_no_rd
    | Domain.No_motion _   -> incr c_no_mot
    | Domain.Low_voltage _ -> incr c_low_v
    | Domain.Voltage_ok _  -> incr c_v_ok
    | Domain.Sos _         -> incr c_sos) alerts;
  (!c_no_pkt, !c_no_rd, !c_no_mot, !c_low_v, !c_v_ok, !c_sos)

let count_retracts events =
  List.length (List.filter
    (function Mf_event.Retract _ -> true | _ -> false) events)

let () =
  Printf.printf "═══════════════════════════════════════════════════════════════\n";
  Printf.printf "  Бенчмарк ex07 — большая шахта\n";
  Printf.printf "═══════════════════════════════════════════════════════════════\n\n";

  Printf.printf "Конфигурация:\n";
  List.iter (fun s -> Printf.printf "  %s\n" s) (Src.stats ());
  print_newline ();

  Printf.printf "Прогон: %d измерений (+1 warmup)\n\n" runs;

  (* Пайплайн алертов *)
  let alerts, st_alerts =
    Bench_stats.run_many ~name:"connectivity_alerts" ~runs (fun () ->
      Src.read ()
      |> Pipelines.connectivity_alerts
      |> Pipe.collect) in
  Bench_stats.print_stats st_alerts;

  (* Пайплайн локаций *)
  let location_events, st_loc =
    Bench_stats.run_many ~name:"median_rssi" ~runs (fun () ->
      Src.read ()
      |> Pipelines.median_rssi
      |> Stream.to_list) in
  Bench_stats.print_stats st_loc;

  print_newline ();

  (* Сводка по алертам *)
  let n_no_pkt, n_no_rd, n_no_mot, n_low_v, n_v_ok, n_sos = count_by_type alerts in
  Printf.printf "Алерты (всего %d):\n" (List.length alerts);
  Printf.printf "  No_packets:   %6d\n" n_no_pkt;
  Printf.printf "  No_readings:  %6d\n" n_no_rd;
  Printf.printf "  No_motion:    %6d\n" n_no_mot;
  Printf.printf "  Low_voltage:  %6d\n" n_low_v;
  Printf.printf "  Voltage_ok:   %6d\n" n_v_ok;
  Printf.printf "  Sos:          %6d\n" n_sos;
  print_newline ();

  Printf.printf "Локация:\n";
  Printf.printf "  событий окон:   %6d\n"
    (List.length (List.filter
       (function Mf_event.Data _ -> true | _ -> false) location_events));
  Printf.printf "  ретракций:      %6d\n" (count_retracts location_events);
  print_newline ();

  let n_packets = bench_config.horizons
                  * bench_config.miners_per_horizon
                  * (bench_config.simulation_minutes * 60 / bench_config.step_seconds) in
  let tput_alerts = Bench_stats.throughput_ev_per_sec ~n_events:n_packets st_alerts in
  let tput_loc    = Bench_stats.throughput_ev_per_sec ~n_events:n_packets st_loc in
  Printf.printf "Пропускная способность (median, ev/s):\n";
  Printf.printf "  connectivity_alerts: %s ev/s\n"
    (Printf.sprintf "%.0fK" (tput_alerts /. 1000.));
  Printf.printf "  median_rssi:         %s ev/s\n"
    (Printf.sprintf "%.0fK" (tput_loc /. 1000.));
  Printf.printf "\nДля regression-сравнения «было vs стало» используй p95,\n";
  Printf.printf "median может шуметь на ±5-10%%.\n\n";

  Printf.printf "═══════════════════════════════════════════════════════════════\n"
