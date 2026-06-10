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

  (* собираем топологию: 2 партиции газа → merge → fan_out → 2 пайплайна *)

  (* merge_partitioned: сливаем две партиции газа в один поток.
     Wall_clock_timeout — если партиция замолчит, не вешаем watermark. *)
  let merged =
    Merge.merge_partitioned ~idle:(Merge.Wall_clock_timeout 1000)
      [ Stream.of_list partition_a; Stream.of_list partition_b ] in

  (* fan_out: один поток → две обработки, у каждой СВОЯ политика.
     Аварии — Block (не терять ни одного показания).
     Дашборд — Drop_oldest (важна свежесть, переполнение допустимо). *)
  let outlets = Fan_out.[
    { name="alerts";    buffer_cap=64; on_pressure=Block };
    { name="dashboard"; buffer_cap=16; on_pressure=Drop_oldest };
  ] in
  let streams = Fan_out.fan_out merged outlets in
  let alerts_in = List.nth streams 0 in
  let dash_in   = List.nth streams 1 in

  (* Результаты пайплайнов. Под supervisor пайплайн — сервис с побочным
     эффектом (публикует результат), поэтому общая ссылка здесь отражает
     реальную границу: пайплайн пишет наружу. Но ВНУТРИ пайплайна сбор
     декларативен (collect / materialize), без ручных циклов. *)
  let alarms = ref [] in
  let dashboard = ref [] in

  (* два пайплайна под supervisor, у каждого своя failure-стратегия *)

  (* пайплайн АВАРИЙ: критичный метан. collect собирает события
     декларативно. Crash_all — молчать об авариях нельзя. *)
  let alerts_pipeline () =
    alarms :=
      alerts_in
      |> Pipe.filter (fun r -> r.ch4 >= critical_ch4)
      |> Pipe.collect
      |> List.map (fun r -> (r.sensor, r.ch4, r.horizon))
  in

  (* пайплайн ДАШБОРДА: средний метан по сенсору в окне. materialize
     даёт финальное значение каждого окна. Restart — некритично. *)
  let dashboard_pipeline () =
    dashboard :=
      dash_in
      |> Pipe.window_agg (module BySensor) (Pipe.tumbling (seconds 10))
           (Agg.mean (fun r -> r.ch4))
      |> Pipe.materialize ~by:(fun (sensor, _) wend -> (sensor, wend))
      |> List.filter_map (fun (_, (sensor, avg)) ->
           match avg with Some a -> Some (sensor, a) | None -> None)
  in

  let specs = Supervisor.[
    { label="alerts";    run=alerts_pipeline;    on_failure=Crash_all };
    { label="dashboard"; run=dashboard_pipeline; on_failure=Restart { max_retries=3; backoff_ms=0 } };
  ] in
  let statuses = Supervisor.supervise_result specs in

  (* ── Вывод сервиса ────────────────────────────────────── *)
  List.iter (fun (label, st) ->
    if st = `Failed then
      Printf.printf "пайплайн %s остановлен\n" label) statuses;

  Printf.printf "Аварии по метану (>= %.1f%%):\n" critical_ch4;
  (match List.sort compare !alarms with
   | [] -> Printf.printf "  нет\n"
   | xs -> List.iter (fun (s, ch4, h) ->
       Printf.printf "  %s на горизонте %dм: %.1f%% CH4\n" s h ch4) xs);

  Printf.printf "\nДашборд — средний метан по сенсору (▮ = 0.5%%, порог %.0f%%):\n" critical_ch4;
  List.sort compare !dashboard
  |> List.iter (fun (sensor, avg) ->
       let bars = int_of_float (avg /. 0.5 +. 0.5) in
       let bar = String.concat "" (List.init bars (fun _ -> "▮")) in
       let flag = if avg >= critical_ch4 then " ⚠" else "" in
       Printf.printf "  %s │%-8s %.1f%%%s\n" sensor bar avg flag)
