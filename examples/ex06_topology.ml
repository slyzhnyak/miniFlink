(* ============================================================
   Пример 6 — модель исполнения B+C: несколько пайплайнов + fan-out.

   Топология мониторинга шахты, собранная из примитивов исполнения:

     2 партиции газа ──merge_partitioned(idle)──┐
                                                 │
                          fan_out ──[Block]──────┴─→ пайплайн АВАРИЙ (Crash_all)
                                  └─[Drop_oldest]──→ пайплайн ДАШБОРДА (Restart)

   Показывает три стратегии, выбранные ПРИ СОЗДАНИИ:
     • merge_partitioned ~idle  — слияние партиций (Wall_clock_timeout:
                                   молчащая партиция не вешает watermark)
     • fan_out outlet            — один источник в две обработки, у каждой
                                   своя backpressure: аварии Block (не
                                   терять), дашборд Drop_oldest (свежесть)
     • supervisor on_failure     — аварии Crash_all (деградация недопустима),
                                   дашборд Restart (перезапустить)

   Здесь источник — симулированные партиции (in-memory), чтобы пример был
   самодостаточным. В проде сюда встаёт Kafka: connectors/kafka даёт
   seekable_source поверх партиций топика (см. connectors/kafka/).
   ============================================================ *)

open Miniflink
open Time

(* ── Доменная модель: показание газоанализатора ───────────── *)
type reading = {
  sensor  : string;
  horizon : int;       (* горизонт шахты, метры: -80 / -160 / -240 *)
  ch4     : float;     (* метан, % *)
  ts      : Time.t;
}

module BySensor = Keyed.Make (struct
  type t = reading
  let key r = r.sensor
end)

let critical_ch4 = 2.0   (* порог аварии по метану, % *)

(* ── Симуляция двух Kafka-партиций газового топика ────────── *)
(* Партиция A — горизонт -160, партиция B — горизонт -240.
   У каждой свой watermark; B «отстаёт» по времени. *)
let partition_a = [
  Mf_event.data { sensor="S1"; horizon=(-160); ch4=0.8; ts=seconds 1 } (seconds 1);
  Mf_event.data { sensor="S1"; horizon=(-160); ch4=1.2; ts=seconds 3 } (seconds 3);
  Mf_event.data { sensor="S2"; horizon=(-160); ch4=2.4; ts=seconds 5 } (seconds 5); (* АВАРИЯ *)
  Mf_event.wm (seconds 10);
]
let partition_b = [
  Mf_event.data { sensor="S3"; horizon=(-240); ch4=0.5; ts=seconds 2 } (seconds 2);
  Mf_event.data { sensor="S4"; horizon=(-240); ch4=3.1; ts=seconds 4 } (seconds 4); (* АВАРИЯ *)
  Mf_event.wm (seconds 10);
]

let () =
  Printf.printf "=== Пример 6: модель исполнения B+C (шахта) ===\n\n";

  (* (1) merge_partitioned: сливаем две партиции газа в один поток.
     Wall_clock_timeout — если партиция замолчит, не вешаем watermark. *)
  Printf.printf "[1] merge_partitioned: 2 партиции газа → один поток\n";
  let merged =
    Merge.merge_partitioned ~idle:(Merge.Wall_clock_timeout 1000)
      [ Stream.of_list partition_a; Stream.of_list partition_b ] in

  (* (2) fan_out: один поток → две обработки, у каждой СВОЯ политика.
     Аварии — Block (не терять ни одного показания).
     Дашборд — Drop_oldest (важна свежесть, переполнение допустимо). *)
  Printf.printf "[2] fan_out: аварии=Block, дашборд=Drop_oldest\n";
  let outlets = Fan_out.[
    { name="alerts";    buffer_cap=64; on_pressure=Block };
    { name="dashboard"; buffer_cap=16; on_pressure=Drop_oldest };
  ] in
  let streams = Fan_out.fan_out merged outlets in
  let alerts_in = List.nth streams 0 in
  let dash_in   = List.nth streams 1 in

  (* собранные результаты обоих пайплайнов *)
  let alarms = ref [] in
  let dash_count = ref 0 in

  (* (3) два пайплайна под supervisor, у каждого своя failure-стратегия *)

  (* пайплайн АВАРИЙ: фильтр критичного метана. Crash_all — если упадёт,
     весь мониторинг должен остановиться (молчать об авариях нельзя). *)
  let alerts_pipeline () =
    alerts_in
    |> Pipe.filter (fun r -> r.ch4 >= critical_ch4)
    |> Pipe.sink (fun r ->
         alarms := (r.sensor, r.ch4, r.horizon) :: !alarms)
  in

  (* пайплайн ДАШБОРДА: средний метан по сенсору в окне. Restart —
     некритично, при сбое просто перезапустить. *)
  let dashboard_pipeline () =
    dash_in
    |> Pipe.window_agg (module BySensor) (Pipe.tumbling (seconds 10))
         (Agg.mean (fun r -> r.ch4))
    |> Pipe.sink (fun _ -> incr dash_count)
  in

  Printf.printf "[3] supervisor: аварии=Crash_all, дашборд=Restart\n\n";
  let specs = Supervisor.[
    { label="alerts";    run=alerts_pipeline;    on_failure=Crash_all };
    { label="dashboard"; run=dashboard_pipeline; on_failure=Restart { max_retries=3; backoff_ms=0 } };
  ] in
  let statuses = Supervisor.supervise_result specs in

  (* ── Результаты ───────────────────────────────────────── *)
  Printf.printf "Статусы пайплайнов:\n";
  List.iter (fun (label, st) ->
    Printf.printf "  %-10s %s\n" label
      (match st with `Ok -> "OK" | `Failed -> "FAILED")) statuses;

  Printf.printf "\nАварии по метану (>= %.1f%%):\n" critical_ch4;
  List.iter (fun (s, ch4, h) ->
    Printf.printf "  %s на горизонте %dм: %.1f%% CH4\n" s h ch4)
    (List.sort compare !alarms);

  Printf.printf "\nДашборд: эмиссий окон %d\n" !dash_count;
  Printf.printf "\nОбе аварии (S2 -160м, S4 -240м) пойманы из РАЗНЫХ партиций\n";
  Printf.printf "через merge → fan_out(Block) → пайплайн под supervisor.\n"
