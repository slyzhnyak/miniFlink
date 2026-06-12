(** A/B сравнение window: WMap (Map.Make) vs Hashtbl.

    Эксперимент: меняем ТОЛЬКО структуру хранения окон и смотрим
    разницу. Один и тот же датасет, агрегат идентичный, sliding
    параметры одинаковые.

    Это тест гипотезы из bench_ops: «WMap — главное узкое место
    в lib/window.ml». Если Hashtbl даёт значительное (>20%) ускорение,
    гипотеза подтверждена и стоит мигрировать lib/window.ml.

    ВАЖНО: проверяется только производительность. Корректность
    отдельно — мы сравним конечные эмиссии обоих окон (must be equal). *)

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
module ByReading = Keyed.Make
  (struct type t = reading let key r = r.r_lamp end)

let runs = 4

(* Подготовим одинаковую цепочку входа: source → dedup → flat_map → event_time *)
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

(* Тривиальный агрегат — count_if (fun _ -> true) — изолируем накладные
   расходы оконного механизма от агрегата. *)
let bench_wmap () =
  prepare ()
  |> Pipe.window_agg_keyed ~by:(fun r -> r.r_lamp)
       ~allowed_lateness:(seconds 60)
       (Pipe.sliding (seconds 60) (seconds 5))
       (Agg.count_if (fun _ -> true))
  |> Stream.to_list

(* Hashtbl-вариант: его API возвращает (string * a list); для эквивалента
   нужно ту же count-агрегацию. Сделаю count руками. *)
let bench_hashtbl () =
  prepare ()
  |> Window_hashtbl.window_hashtbl (module ByReading)
       ~size:(seconds 60) ~step:(seconds 5)
       ~latency:0
       ~allowed_lateness:(seconds 60)
  |> Pipe.map (fun (k, vs) -> (k, List.length vs))
  (* Pipe.map для Mf_event требует map по типу Stream — но window_hashtbl
     уже Mf_event-стрим; map даст ошибку. Используем emap или прямой Stream.map *)
  |> Stream.to_list

let count_data evs =
  List.length (List.filter (function Mf_event.Data _ -> true | _ -> false) evs)
let count_retracts evs =
  List.length (List.filter (function Mf_event.Retract _ -> true | _ -> false) evs)

let () =
  Printf.printf "═══════════════════════════════════════════════════════════════\n";
  Printf.printf "  A/B: window WMap vs Hashtbl (тривиальный count_if агрегат)\n";
  Printf.printf "═══════════════════════════════════════════════════════════════\n\n";

  List.iter (fun s -> Printf.printf "  %s\n" s) (Src.stats ());
  print_newline ();

  let res_wmap, st_wmap =
    Bench_stats.run_many ~name:"WMap (Map.Make)" ~runs bench_wmap in
  Bench_stats.print_stats st_wmap;

  let res_ht, st_ht =
    Bench_stats.run_many ~name:"Hashtbl" ~runs bench_hashtbl in
  Bench_stats.print_stats st_ht;

  print_newline ();

  (* Проверка эквивалентности результатов (общее число Data-эмиссий) *)
  let d_wmap = count_data res_wmap in
  let r_wmap = count_retracts res_wmap in
  let d_ht = count_data res_ht in
  let r_ht = count_retracts res_ht in
  Printf.printf "Эмиссии: WMap %d Data + %d Retract, Hashtbl %d Data + %d Retract\n"
    d_wmap r_wmap d_ht r_ht;
  if d_wmap <> d_ht then
    Printf.printf "  ⚠ DIFF в числе Data-эмиссий — реализации не эквивалентны!\n";
  print_newline ();

  let speedup = st_wmap.s_median /. st_ht.s_median in
  Printf.printf "Speedup (median): %.2fx (Hashtbl быстрее в %.1f раз)\n" speedup speedup;
  let alloc_reduction =
    1. -. st_ht.s_mean_alloc_mb /. st_wmap.s_mean_alloc_mb in
  Printf.printf "Снижение аллокаций: %.1f%% (%.0fМБ → %.0fМБ)\n"
    (alloc_reduction *. 100.) st_wmap.s_mean_alloc_mb st_ht.s_mean_alloc_mb;
  print_newline ();

  Printf.printf "═══════════════════════════════════════════════════════════════\n"
