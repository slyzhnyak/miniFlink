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
     • union                         — слияние двух источников в поток
     • update_table + enrich         — stream-table join: таблица
                                       приписки наполняется из ОТДЕЛЬНОГО
                                       потока и эволюционирует, телеметрия
                                       обогащается актуальным депо
                                       (snapshot-семантика, [F])
     • temporal_join                 — депо НА МОМЕНТ события (as-of),
                                       корректно даже при ОПОЗДАВШЕМ
                                       апдейте справочника ([G])
     • dedup                         — подавление задвоенных показаний

   (session/global/count окна и sliding — в ex03/ex04, где они
   центральные; здесь tumbling, чтобы не дублировать.)

   Сценарий: автобусы шлют телеметрию (id, скорость, заряд %, депо).
   Часть показаний битые (сенсор сбоит) — их надо пропускать, не роняя
   поток. Считаем по депо: средняя скорость, минимальный заряд, число
   автобусов с низким зарядом. И прогоняем агрегацию по депо через
   exactly-once с устойчивостью к краху воркера.

   📖 Подробный построчный разбор: docs/example_fleet_walkthrough.md
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

(* KEYED по депо — для оконной агрегации *)
module ByDepot = Keyed.Make (struct
  type t = telemetry
  let key t = t.depot
end)

(* KEYED по bus_id — для enrich (join с таблицей приписки) и dedup *)
module ById = Keyed.Make (struct
  type t = telemetry
  let key t = t.bus_id
end)

(* Поток назначений: «автобус приписан к депо». Приходит ОТДЕЛЬНО и
   МЕНЯЕТСЯ во времени (автобус перевели) — справочные данные, которыми
   обогащается основной поток телеметрии (stream-table join, секция [F]). *)
type assignment = { a_bus : string; a_depot : string; a_ts : Time.t }

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

(* ── 6b. Данные для stream-table join (секция [F]) ─────────── *)
(* Депо в telemetry будет ПЕРЕЗАПИСАНО из таблицы приписки через enrich —
   показания приходят с bus_id, актуальное депо берётся из справочника. *)

(* Поток назначений: B1 сначала в north, потом ПЕРЕВЕД�ён в south.
   Таблица обновляется из этого потока (update_table). *)
let assignments = [
  { a_bus="B1"; a_depot="north"; a_ts=seconds 0 };
  { a_bus="B2"; a_depot="north"; a_ts=seconds 0 };
  { a_bus="B3"; a_depot="south"; a_ts=seconds 0 };
  { a_bus="B1"; a_depot="south"; a_ts=seconds 100 };  (* B1 перевели в south *)
]

(* Два источника телеметрии: городские и пригородные автобусы.
   Сольём их в один поток через union. depot здесь — заглушка "?",
   реальное депо подставит enrich из таблицы приписки. *)
let city_buses = [
  { bus_id="B1"; depot="?"; speed=42.; charge=80.; ts=seconds 2 };
  { bus_id="B2"; depot="?"; speed=38.; charge=60.; ts=seconds 4 };
  { bus_id="B1"; depot="?"; speed=44.; charge=78.; ts=seconds 6 };
  { bus_id="B2"; depot="?"; speed=38.; charge=60.; ts=seconds 4 };  (* ДУБЛЬ *)
]
let suburb_buses = [
  { bus_id="B3"; depot="?"; speed=55.; charge=90.; ts=seconds 3 };
  { bus_id="B1"; depot="?"; speed=40.; charge=75.; ts=seconds 5 };
  { bus_id="B3"; depot="?"; speed=55.; charge=90.; ts=seconds 3 };  (* ДУБЛЬ *)
]

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
  Printf.printf "  %s\n\n" (Health.to_json h);

  (* (F) stream-table join: union + update_table + enrich + dedup.
     Два источника телеметрии сливаются (union); таблица приписки
     автобус→депо наполняется из ОТДЕЛЬНОГО потока назначений
     (update_table) и enrich подставляет актуальное депо; dedup убирает
     задвоенные показания. *)
  Printf.printf "[F] stream-table join (union + update_table + enrich + dedup)\n";

  (* 1. таблица приписки наполняется из потока назначений.
     update_table наполняет Hashtbl по мере прохождения событий; последнее
     назначение по ключу побеждает — B1 окажется в south (его перевели). *)
  let depot_of : (string, assignment) Hashtbl.t = Hashtbl.create 8 in
  Mf_event.of_list ~ts:(fun a -> a.a_ts) assignments
  |> Pipe.update_table depot_of ~key:(fun a -> a.a_bus)
  |> Stream.to_list |> ignore;          (* прогнали — таблица наполнена *)
  Printf.printf "  таблица приписки наполнена из потока: %d автобусов" (Hashtbl.length depot_of);
  Printf.printf " (B1 → %s, его перевели)\n"
    (match Hashtbl.find_opt depot_of "B1" with Some a -> a.a_depot | None -> "?");

  (* 2. union двух источников телеметрии в один поток *)
  let city = Mf_event.of_list ~ts:(fun t -> t.ts) city_buses in
  let suburb = Mf_event.of_list ~ts:(fun t -> t.ts) suburb_buses in
  let merged = Mf_event.union city suburb in

  (* 3. dedup (по bus_id+ts) → enrich (актуальное депо из таблицы) *)
  let depot_table : (string, assignment) Table.t =
    Table.of_hashtbl depot_of in
  let result =
    merged
    |> Mf_event.with_watermarks ~latency:0
    |> Pipe.dedup (module ById)
         ~rule:(fun t -> string_of_int t.ts)   (* ключ дедупа: bus + время события *)
         ~cooldown:(seconds 60)   (* окно дедупа > разброса дублей в потоке *)
    |> Pipe.enrich (module ById)
         ~from:depot_table
         ~merge:(fun t assign ->
           match assign with
           | Some a -> { t with depot = a.a_depot }   (* подставили депо *)
           | None -> t)
    |> Stream.to_list
    |> List.filter_map (function Mf_event.Data (t,_) -> Some t | _ -> None) in
  let total_in = List.length city_buses + List.length suburb_buses in
  Printf.printf "  union: %d событий из двух источников\n" total_in;
  Printf.printf "  после dedup + enrich: %d уникальных показаний с депо:\n" (List.length result);
  List.iter (fun t ->
    Printf.printf "    %s @%ds → депо %s (скорость %.0f)\n"
      t.bus_id (t.ts/1000) t.depot t.speed) result;
  Printf.printf "\n";

  (* (G) TEMPORAL join: депо НА МОМЕНТ показания, корректно даже при
     ОПОЗДАВШЕМ апдейте. Контраст с [F]: там enrich дал «текущее» депо
     (south для всех B1, т.к. таблица отражает финал); здесь показание
     получает депо, актуальное на ЕГО event-time. *)
  Printf.printf "[G] temporal join (депо на момент события; опоздавший апдейт)\n";

  (* Поток апдейтов приписки с watermark. ОПОЗДАВШИЙ порядок: апдейт
     "B1→south с t=100" приходит ПЕРЕД апдейтом "B1→north с t=0". *)
  let assignment_updates = Stream.of_list [
    Mf_event.data { a_bus="B1"; a_depot="south"; a_ts=seconds 100 } (seconds 100);
    Mf_event.data { a_bus="B1"; a_depot="north"; a_ts=seconds 0 }   (seconds 0); (* опоздал! *)
    Mf_event.wm (seconds 200);
  ] in
  (* Показания B1: одно ДО перевода (t=50 → north), одно ПОСЛЕ (t=150 → south) *)
  let b1_readings = Mf_event.of_list ~ts:(fun t -> t.ts) [
    { bus_id="B1"; depot="?"; speed=42.; charge=80.; ts=seconds 50 };
    { bus_id="B1"; depot="?"; speed=44.; charge=78.; ts=seconds 150 };
  ] in
  let temporal_result =
    b1_readings
    |> Mf_event.with_watermarks ~latency:0
    |> Temporal.temporal_join
         ~key_main:(fun t -> t.bus_id)
         ~key_upd:(fun a -> a.a_bus)
         ~valid_from:(fun a -> a.a_ts)
         ~merge:(fun t assign ->
           match assign with Some a -> { t with depot = a.a_depot } | None -> t)
         ~updates:assignment_updates
    |> Stream.to_list
    |> List.filter_map (function Mf_event.Data (t,_) -> Some t | _ -> None) in
  Printf.printf "  апдейт 'B1→north@0' пришёл ПОСЛЕ 'B1→south@100' (опоздал)\n";
  List.iter (fun t ->
    Printf.printf "    B1 @%ds → депо %s  (на момент события)\n" (t.ts/1000) t.depot)
    temporal_result;
  let ok = List.map (fun t -> (t.ts/1000, t.depot)) temporal_result = [(50,"north"); (150,"south")] in
  Printf.printf "  === temporal даёт верное депо НА МОМЕНТ события даже при опоздавшем апдейте: %b ===\n" ok
