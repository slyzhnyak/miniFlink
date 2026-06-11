(** Бенчмарк ex07 на большой шахте.

    Конфигурация по умолчанию: 16 горизонтов × 64 биконов = 1024
    биконов; 16 × 256 = 4096 шахтёров; ~1 час симуляции = ~1M пакетов.

    Что меряется:
    - throughput каждого пайплайна (events/sec);
    - агрегированные счётчики алертов по типам;
    - ретракции (вклад опоздавших пакетов);
    - дубли (вклад at-least-once канала);
    - пиковая куча через Gc.stat.

    На печать выводится сводная таблица, а не миллионы строк (sink в
    бенчмарке агрегирует — diff с дисплеем диспетчерского пульта, где
    показываются только активные алерты). *)

open Miniflink
open Ex07_location_lib

(* Полная конфигурация: 16 × 64 биконов = 1024; 16 × 256 = 4096 шахтёров;
   ~1 час симуляции ≈ ~1М пакетов. Если бенчмарк слишком долгий на
   твоём ноутбуке — уменьшай horizons или simulation_minutes. *)
let bench_config = Mock_source.Large_mine.default_config

module Src = Mock_source.Large_mine.Make (struct let config = bench_config end)

(** Хронометраж + результат пайплайна. *)
let time_pipeline ~name f =
  Gc.compact ();
  let g0 = Gc.stat () in
  let t0 = Unix.gettimeofday () in
  let result = f () in
  let t1 = Unix.gettimeofday () in
  let g1 = Gc.stat () in
  let dt = t1 -. t0 in
  let heap_mb = float_of_int g1.heap_words *. 8. /. 1024. /. 1024. in
  let allocated_mb =
    (g1.minor_words +. g1.major_words -. g0.minor_words -. g0.major_words)
    *. 8. /. 1024. /. 1024. in
  Printf.printf "  %-30s %.2f сек   куча %.0f МБ   алок %.0f МБ\n"
    name dt heap_mb allocated_mb;
  result, dt

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

  Printf.printf "Прогон:\n";

  (* Пайплайн алертов *)
  let alerts, dt_alerts =
    time_pipeline ~name:"connectivity_alerts" (fun () ->
      Src.read ()
      |> Pipelines.connectivity_alerts
      |> Pipe.collect) in

  (* Пайплайн локаций *)
  let location_events, dt_loc =
    time_pipeline ~name:"median_rssi" (fun () ->
      Src.read ()
      |> Pipelines.median_rssi
      |> Stream.to_list) in

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
  let tput_alerts = float_of_int n_packets /. dt_alerts in
  let tput_loc    = float_of_int n_packets /. dt_loc in
  Printf.printf "Пропускная способность (по событиям источника, не считая разворот в reading):\n";
  Printf.printf "  connectivity_alerts: %s ev/s\n"
    (Printf.sprintf "%.0fK" (tput_alerts /. 1000.));
  Printf.printf "  median_rssi:         %s ev/s\n"
    (Printf.sprintf "%.0fK" (tput_loc /. 1000.));
  print_newline ();

  Printf.printf "═══════════════════════════════════════════════════════════════\n"
