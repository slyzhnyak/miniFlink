(** Bench_scaling — масштабирование параллельной обработки по числу
    воркеров. Прогоняет одну и ту же нагрузку через {!Pipe.fan_out} с
    разным числом воркеров (1, 2, 4, 6, 8, 12), считает throughput,
    выводит таблицу для копи-паста в README.

    Использует {!Bench_stats}: warmup + N измерений, p95 для стабильных
    цифр (не median).

    Сравни цифры между OCaml 4.14 и OCaml 5.x switch'ами — там должна
    быть видна разница: на v4 параллельность ограничена runtime-lock
    (max speedup обычно ~2.5-3x независимо от ядер), на v5 с Domain
    масштабирование почти линейное до физических ядер процессора.
*)

open Miniflink
open Test_support.Domain
open Time

(* Конфигурация: переключи `scale` между [`Smoke] (быстро, для проверки
   что код жив) и [`Full] (реальные замеры на твоей машине, ~5-10 минут). *)
let scale = `Full

let n_events  = match scale with `Smoke -> 50_000  | `Full -> 500_000
let n_devices = match scale with `Smoke -> 200     | `Full -> 1_000
let runs      = match scale with `Smoke -> 2       | `Full -> 4

let make_events () =
  Array.init n_events (fun i ->
    let id    = Printf.sprintf "dev_%04d" (i mod n_devices) in
    let ts    = i * 100 in
    let speed = if i mod 10 = 0 then 130. else 60. in
    let fuel  = if i mod 7  = 0 then 15.  else 80. in
    Mf_event.data
      { device_id = id; speed_kmh = speed; fuel_pct = fuel;
        position = { lat = 55.75; lon = 37.61 }; ts; device = None }
      ts)

let make_devs () =
  Table.of_list (List.init n_devices (fun i ->
    Printf.sprintf "dev_%04d" i,
    { owner = "O"; max_speed = 90.; zone = "z" }))

let make_pipeline devs =
  fun source ->
    source
    |> Mf_event.with_watermarks ~latency:(seconds 3)
    |> Pipe.enrich (module Telemetry)
         ~from:devs
         ~merge:(fun t d -> { t with device = d })
    |> Pipe.window (module Telemetry) (Pipe.tumbling (seconds 30))
    |> Pipe.aggregate Test_support.Rules.compute
    |> Pipe.flat_map (Test_support.Rules.check Test_support.Rules.fleet)
    |> Pipe.dedup (module Alert)
         ~rule:(fun a -> a.rule)
         ~cooldown:(minutes 5)

let run_sequential events devs () =
  let count = ref 0 in
  Stream.of_list (Array.to_list events)
  |> make_pipeline devs
  |> Stream.iter (fun _ -> incr count);
  !count

let run_parallel n_workers events devs () =
  let count = ref 0 in
  let mu    = Mutex.create () in
  Parallel.run_parallel_simple
    ~workers:n_workers
    ~capacity:4096
    ~key_of:(fun (t:telemetry) -> t.device_id)
    ~pipeline:(make_pipeline devs)
    ~source:(Stream.of_list (Array.to_list events))
    ~sink:(fun _ -> Mutex.lock mu; incr count; Mutex.unlock mu)
    ();
  !count

let () =
  let events = make_events () in
  let devs   = make_devs () in

  Printf.printf "═══════════════════════════════════════════════════════════════\n";
  Printf.printf "  Параллельное масштабирование на %d событий, %d устройств\n"
    n_events n_devices;
  Printf.printf "  OCaml версия: %s\n" Sys.ocaml_version;
  Printf.printf "═══════════════════════════════════════════════════════════════\n\n";

  Printf.printf "Прогон: %d измерений (+1 warmup) на каждую конфигурацию\n\n" runs;

  (* Baseline: sequential *)
  let _, st_seq =
    Bench_stats.run_many ~name:"sequential" ~runs (run_sequential events devs) in
  Bench_stats.print_stats st_seq;
  let tput_seq = float_of_int n_events /. st_seq.s_median in

  (* Шкала воркеров. До 12 чтобы покрыть HT на i7-8700 (6 физ + HT). *)
  let workers_list = [1; 2; 4; 6; 8; 12] in
  let results = List.map (fun w ->
    let name = Printf.sprintf "parallel %2d" w in
    let _, st = Bench_stats.run_many ~name ~runs (run_parallel w events devs) in
    Bench_stats.print_stats st;
    (w, st)
  ) workers_list in

  print_newline ();
  Printf.printf "═══════════════════════════════════════════════════════════════\n";
  Printf.printf "  Таблица для README (median throughput, speedup vs sequential)\n";
  Printf.printf "═══════════════════════════════════════════════════════════════\n\n";

  Printf.printf "| воркеров | время (с) | ev/s   | speedup |\n";
  Printf.printf "|----------|-----------|--------|---------|\n";
  Printf.printf "| sequential | %8.2f | %5.0fK | 1.00x   |\n"
    st_seq.s_median (tput_seq /. 1000.);
  List.iter (fun (w, st) ->
    let tput = float_of_int n_events /. st.Bench_stats.s_median in
    let speedup = st_seq.s_median /. st.Bench_stats.s_median in
    Printf.printf "| %2d       | %8.2f | %5.0fK | %4.2fx  |\n"
      w st.s_median (tput /. 1000.) speedup
  ) results;

  print_newline ();
  Printf.printf "Для сравнения OCaml 4 vs 5: запустите этот же бенчмарк на\n";
  Printf.printf "обоих switch'ах и сравните speedup. На OCaml 4 параллельность\n";
  Printf.printf "под GIL ограничена ~2.5-3x максимум; на OCaml 5 с Domain\n";
  Printf.printf "ожидается почти линейное масштабирование до числа физических\n";
  Printf.printf "ядер (HT даёт небольшое дополнительное ускорение).\n\n";
  Printf.printf "═══════════════════════════════════════════════════════════════\n"
