(* ============================================================
   Пример 5 — production-путь: телеметрия флота электробусов.

   ex04 показывает топологию (union/enrich/окна/dedup). Этот пример —
   про PRODUCTION-стек и фичи, добавленные после ex04, которых там нет:

     • Mf_event.of_list ~ts          — поток из списка без бойлерплейта
     • out-of-order + late + dups    — данные приходят вразнобой, с
                                       опоздавшими и дублями; окна
                                       группируют по event-time,
                                       allowed_lateness пере-считывает
                                       опоздавших (retract+новый), итог
                                       совпадает с упорядоченным прогоном
     • safe_map ~on_error            — изоляция битых событий (поток
                                       выживает на плохом показании)
     • Agg + window_agg              — комбинируемые агрегаторы,
                                       несколько метрик за один проход
     • window_fold                   — инкрементальная агрегация (O(1))
     • schema (версионированный codec)— сериализация состояния с версией
     • exactly-once + recovery       — run_exactly_once, имитация краха
                                       процесса и восстановление из
                                       checkpoint без потерь/дублей
     • retry с backoff               — устойчивый «commit» в внешний
                                       приёмник
     • health/config                 — операционные сводки

   Сценарий: автобусы шлют телеметрию (id, скорость, заряд %, депо).
   Часть показаний битые (сенсор сбоит) — их надо пропускать, не роняя
   поток. Считаем по депо: средняя скорость, минимальный заряд, число
   автобусов с низким зарядом. И прогоняем агрегацию по депо через
   exactly-once с устойчивостью к краху воркера.
   ============================================================ *)

open Miniflink
open Time

(* ── 1. Доменная модель ───────────────────────────────────── *)

type telemetry = {
  bus_id : string;
  depot  : string;       (* депо приписки — ключ группировки *)
  speed  : float;        (* км/ч *)
  charge : float;        (* заряд батареи %, 0..100 *)
  ts     : Time.t;
}

(* KEYED-модуль: группируем по депо *)
module ByDepot = Keyed.Make (struct
  type t = telemetry
  let key t = t.depot
end)

(* ── 2. Версионированная схема состояния (schema) ──────────── *)
(* Состояние агрегата по депо сериализуем с версией — на будущее, когда
   формат изменится, decode сможет мигрировать или явно отвергнуть. *)

type depot_stat = { sum_speed : float; n : int; min_charge : float }

let stat_schema : depot_stat Schema_default.t =
  Schema_default.make
    ~version:1
    ~encode:(fun s ->
      Bytes.of_string (Printf.sprintf "%f;%d;%f" s.sum_speed s.n s.min_charge))
    ~decode:(fun b ->
      match String.split_on_char ';' (Bytes.to_string b) with
      | [a; b; c] ->
        (try Ok { sum_speed = float_of_string a;
                  n = int_of_string b;
                  min_charge = float_of_string c }
         with _ -> Error "bad stat encoding")
      | _ -> Error "bad stat field count")
    ()

(* ── 3. Сырые данные (часть битые) ────────────────────────── *)

(* «сырое» показание: заряд может быть мусором (сенсор сбоит) *)
type raw = { r_bus : string; r_depot : string; r_speed : float;
             r_charge : float; r_ts : Time.t }

(* «Сырые» показания. НАМЕРЕННО беспорядочные: event-time скачет назад
   (out-of-order), есть опоздавшие на одно-два окна, есть дубли. Это
   реалистично — сеть/буферизация переставляют события, ретраи шлют
   дубли. Streaming-движок должен дать ПРАВИЛЬНЫЙ результат несмотря на
   это: окна группируют по EVENT-TIME (а не по порядку прихода),
   watermark+allowed_lateness ловят опоздавших, агрегат пересчитывается.

   Окна tumbling по 10с: [0,10), [10,20), [20,30).
   Поле r_ts — настоящее event-time; порядок в списке — порядок ПРИХОДА. *)
let raw_events = [
  (* приходят вразнобой по времени *)
  { r_bus="B1"; r_depot="north"; r_speed=42.; r_charge=80.; r_ts=seconds 2 };
  { r_bus="B3"; r_depot="south"; r_speed=55.; r_charge=95.; r_ts=seconds 23 }; (* окно [20,30) раньше своих *)
  { r_bus="B2"; r_depot="north"; r_speed=38.; r_charge=15.; r_ts=seconds 5 };
  { r_bus="B4"; r_depot="south"; r_speed=60.; r_charge=8.;  r_ts=seconds 12 };
  { r_bus="B1"; r_depot="north"; r_speed=44.; r_charge=70.; r_ts=seconds 1 };  (* out-of-order назад *)
  { r_bus="B1"; r_depot="north"; r_speed=40.; r_charge=(-1.); r_ts=seconds 4 }; (* битый заряд *)
  { r_bus="B3"; r_depot="south"; r_speed=50.; r_charge=90.; r_ts=seconds 21 };
  { r_bus="B2"; r_depot="north"; r_speed=45.; r_charge=12.; r_ts=seconds 6 };
  { r_bus="B2"; r_depot="north"; r_speed=38.; r_charge=15.; r_ts=seconds 5 };  (* ДУБЛЬ события t=5 *)
  { r_bus="B5"; r_depot="south"; r_speed=nan; r_charge=70.; r_ts=seconds 14 }; (* битая скорость *)
  { r_bus="B6"; r_depot="north"; r_speed=30.; r_charge=55.; r_ts=seconds 15 };
  { r_bus="B1"; r_depot="north"; r_speed=42.; r_charge=80.; r_ts=seconds 2 };  (* ДУБЛЬ события t=2 *)
  { r_bus="B7"; r_depot="south"; r_speed=48.; r_charge=33.; r_ts=seconds 18 };
  { r_bus="B4"; r_depot="south"; r_speed=62.; r_charge=9.;  r_ts=seconds 11 };
  (* СИЛЬНО опоздавшие: приходят в самом конце, а время — из ранних окон *)
  { r_bus="B8"; r_depot="north"; r_speed=50.; r_charge=40.; r_ts=seconds 3 };  (* опоздал в [0,10) *)
  { r_bus="B9"; r_depot="south"; r_speed=58.; r_charge=22.; r_ts=seconds 13 }; (* опоздал в [10,20) *)
  { r_bus="B3"; r_depot="south"; r_speed=55.; r_charge=95.; r_ts=seconds 23 }; (* ДУБЛЬ события t=23 *)
  { r_bus="B2"; r_depot="north"; r_speed=41.; r_charge=11.; r_ts=seconds 7 };  (* опоздал в [0,10) *)
]

(* валидация: бросаем исключение на битом показании — safe_map поймает *)
let validate (r : raw) : telemetry =
  if r.r_charge < 0. || r.r_charge > 100. then
    failwith (Printf.sprintf "bus %s: charge out of range (%.0f)" r.r_bus r.r_charge);
  if Float.is_nan r.r_speed || r.r_speed < 0. then
    failwith (Printf.sprintf "bus %s: invalid speed" r.r_bus);
  { bus_id = r.r_bus; depot = r.r_depot; speed = r.r_speed;
    charge = r.r_charge; ts = r.r_ts }

(* ── 4. Пайплайн агрегации (Agg + window_agg + safe_map) ──── *)

let dropped = ref 0

let pipeline source =
  source
  (* of_list уже дал Data-события; safe_map валидирует, битые →
     on_error (счётчик), поток продолжается *)
  |> Pipe.safe_map
       ~on_error:(fun e ->
         incr dropped;
         Log.warn ~fields:[("error", Printexc.to_string e)] "dropped bad reading")
       validate
  |> Mf_event.with_watermarks ~latency:(seconds 1)
  (* окно 10с по депо + ТРИ агрегата за один проход:
     средняя скорость, минимальный заряд, число автобусов с низким зарядом.
     allowed_lateness=30s: окно принимает СИЛЬНО опоздавшие даже после
     закрытия — пере-считывает агрегат (retract старого + новый
     результат). Без этого опоздавшие на закрытое окно потерялись бы. *)
  |> Pipe.window_agg (module ByDepot)
       ~latency:(seconds 1) ~allowed_lateness:(seconds 30)
       (Pipe.tumbling (seconds 10))
       Agg.(both (both (mean (fun t -> t.speed))
                       (min_by (fun t -> t.charge)))
                 (count_if (fun t -> t.charge < 20.)))

(* ── 5. Exactly-once обработка по депо с recovery ──────────── *)
(* Считаем накопительную сумму скоростей по депо через exactly-once
   движок. Имитируем краш процесса на середине и восстанавливаемся из
   checkpoint — итог должен совпасть с прогоном без сбоя. *)

module CP = Checkpoint_parallel

(* process: накапливаем сумму скоростей по депо в стейте (через schema) *)
let eo_process backend ev =
  match ev with
  | Mf_event.Data (t, _) ->
    let cur =
      match State_backend_memory.get backend t.depot with
      | Some b -> (match Schema_default.decode stat_schema b with
                   | Ok s -> s | Error _ -> { sum_speed=0.; n=0; min_charge=100. })
      | None -> { sum_speed = 0.; n = 0; min_charge = 100. } in
    let s' = { sum_speed = cur.sum_speed +. t.speed;
               n = cur.n + 1;
               min_charge = Float.min cur.min_charge t.charge } in
    State_backend_memory.set backend t.depot (Schema_default.encode stat_schema s');
    [(t.depot, s'.sum_speed, s'.n)]
  | _ -> []

let clean_telemetry () =
  (* только валидные показания, как event-список для EO-движка *)
  List.filter_map (fun r ->
    try Some (validate r) with _ -> None) raw_events
  |> Mf_event.of_list ~ts:(fun t -> t.ts)
  |> Stream.to_list

let run_eo_reference events =
  let store = CP.make_store () in
  let out = ref [] in
  let mu = Mutex.create () in
  CP.run_exactly_once
    ~workers:1 ~capacity:64 ~checkpoint_every:3
    ~key_of:(fun t -> t.depot) ~make_state:State_backend_memory.create
    ~process:eo_process
    ~source:(CP.seekable_of_list events)
    ~sink:(CP.idempotent_sink (fun v -> Mutex.lock mu; out := v :: !out; Mutex.unlock mu))
    ~store ();
  (store, List.rev !out)

(* recovery: durable store переживает «смерть», recover поднимает стейт *)
let run_eo_with_recovery events =
  let durable = ref [] in
  let store1 = CP.make_store ~persist:(fun cp -> durable := cp :: !durable) () in
  let out = ref [] in
  let mu = Mutex.create () in
  let collect v = Mutex.lock mu; out := v :: !out; Mutex.unlock mu in
  let half = List.length events / 2 in
  let first = List.filteri (fun i _ -> i < half) events in
  (* фаза 1: обработали половину, оставили durable checkpoint-ы, «умерли» *)
  CP.run_exactly_once
    ~workers:1 ~capacity:64 ~checkpoint_every:2
    ~key_of:(fun t -> t.depot) ~make_state:State_backend_memory.create
    ~process:eo_process
    ~source:(CP.seekable_of_list first)
    ~sink:(CP.idempotent_sink collect)
    ~store:store1 ();
  (* фаза 2: новый процесс, durable checkpoint-ы целы, recover *)
  let store2 = CP.make_store () in
  List.iter (CP.commit store2) (List.rev !durable);
  let full = CP.seekable_of_list events in
  let backends = CP.recover ~workers:1 ~make_state:State_backend_memory.create
                   ~source:full store2 in
  let rec drain () = match full.CP.pull () with
    | None -> () | Some ev -> List.iter collect (eo_process backends.(0) ev); drain ()
  in drain ();
  List.rev !out

(* ── 6. retry: устойчивый commit во внешний приёмник ──────── *)
(* Имитация ненадёжного внешнего приёмника (первые 2 попытки падают). *)

let flaky_counter = ref 0
let flaky_publish payload =
  incr flaky_counter;
  if !flaky_counter < 3 then failwith "external sink temporarily unavailable";
  Printf.printf "    published to external sink: %s\n" payload

(* ── 7. Запуск и вывод ────────────────────────────────────── *)

let () =
  Printf.printf "=== Пример 5: production-путь телеметрии флота ===\n\n";

  (* (A) Агрегация при БЕСПОРЯДКЕ: опоздавшие, дубли, не по порядку.
     Окна группируют по event-time, опоздавшие пере-считывают агрегат
     (retract старого результата + новый), дубли учитываются по
     семантике агрегата. Доказательство корректности: тот же набор
     событий в ПРАВИЛЬНОМ порядке даёт тот же финальный результат. *)
  Printf.printf "[A] window_agg при беспорядке (опоздавшие + дубли + out-of-order)\n";
  Printf.printf "    входной порядок намеренно перемешан, %d событий\n" (List.length raw_events);

  (* собрать ФИНАЛЬНЫЙ результат каждого окна: при retract убираем
     прошлую эмиссию, при data — записываем как текущую. На выходе —
     последнее (правильное) значение каждого (депо, конец_окна). *)
  let final_windows stream =
    let tbl = Hashtbl.create 16 in
    Stream.to_list stream |> List.iter (function
      | Mf_event.Data ((depot, payload), wend) -> Hashtbl.replace tbl (depot, wend) (Some payload)
      | Mf_event.Retract ((depot, _), wend) -> Hashtbl.replace tbl (depot, wend) None
      | _ -> ());
    Hashtbl.fold (fun (d,w) v acc -> match v with Some p -> ((d,w),p)::acc | None -> acc) tbl []
    |> List.sort compare in

  let emissions = ref 0 and retracts = ref 0 in
  let counted =
    Mf_event.of_list ~ts:(fun r -> r.r_ts) raw_events
    |> pipeline
    |> Stream.to_list in
  List.iter (function
    | Mf_event.Data _ -> incr emissions
    | Mf_event.Retract _ -> incr retracts | _ -> ()) counted;

  let out_of_order = final_windows (Stream.of_list counted) in
  List.iter (fun ((depot, wend), ((avg_speed, min_charge), low)) ->
    let avg = match avg_speed with Some a -> Printf.sprintf "%.1f км/ч" a | None -> "—" in
    let mc  = match min_charge with Some c -> Printf.sprintf "%.0f%%" c | None -> "—" in
    Printf.printf "  окно[..%ds] депо %-6s: ср.скорость %-10s мин.заряд %-5s  низкий заряд: %d\n"
      (wend/1000) depot avg mc low) out_of_order;
  Printf.printf "  битых пропущено: %d | эмиссий окон: %d | retract'ов (пере-счёт опоздавших): %d\n"
    !dropped !emissions !retracts;

  (* доказательство: те же события, отсортированные по event-time *)
  dropped := 0;
  let in_order_sorted =
    List.stable_sort (fun a b -> compare a.r_ts b.r_ts) raw_events in
  let in_order = final_windows
    (Mf_event.of_list ~ts:(fun r -> r.r_ts) in_order_sorted |> pipeline) in
  Printf.printf "  === тот же набор в правильном порядке даёт тот же итог: %b ===\n\n"
    (out_of_order = in_order);

  (* (B) Exactly-once: эталон vs recovery — результат совпадает *)
  Printf.printf "[B] exactly-once с recovery после краха\n";
  let events = clean_telemetry () in
  let (ref_store, reference) = run_eo_reference events in
  let recovered = run_eo_with_recovery events in
  (* финальные суммы по депо из обоих прогонов *)
  let finals xs =
    let h = Hashtbl.create 4 in
    List.iter (fun (d, sum, _) ->
      match Hashtbl.find_opt h d with
      | Some p when p >= sum -> () | _ -> Hashtbl.replace h d sum) xs;
    List.sort compare (Hashtbl.fold (fun k v a -> (k,v)::a) h []) in
  Printf.printf "  checkpoint-ов в эталоне: %d\n" (CP.checkpoint_count ref_store);
  List.iter (fun (d, s) -> Printf.printf "  депо %-6s суммарная скорость: %.0f\n" d s)
    (finals reference);
  Printf.printf "  recovery == эталон: %b\n\n" (finals reference = finals recovered);

  (* (C) schema round-trip *)
  Printf.printf "[C] версионированная схема состояния (schema v%d)\n"
    (Schema_default.current_version stat_schema);
  let s = { sum_speed = 123.; n = 3; min_charge = 8. } in
  let enc = Schema_default.encode stat_schema s in
  (match Schema_default.decode stat_schema enc with
   | Ok s' -> Printf.printf "  encode→decode round-trip ok: sum=%.0f n=%d\n\n" s'.sum_speed s'.n
   | Error e -> Printf.printf "  decode error: %s\n\n" e);

  (* (D) retry с backoff во внешний приёмник *)
  Printf.printf "[D] retry с backoff (внешний приёмник нестабилен)\n";
  ignore (Retry.with_retry Retry.default
    ~sleep:(fun _ -> ())   (* в примере не ждём реально *)
    ~on_give_up:(fun _e n -> Printf.printf "    gave up after %d attempts\n" n)
    flaky_publish "depot=north avg=41");
  Printf.printf "    (успех после %d попыток)\n\n" !flaky_counter;

  (* (E) health-сводка *)
  Printf.printf "[E] health-сводка\n";
  let h = Health.check ~state_size:(fun () -> List.length events) () in
  Printf.printf "  %s\n" (Health.to_json h)
