(* ============================================================
   Пример 5 — сервис мониторинга флота электробусов.

   Реалистичный сервис: читает телеметрию автобусов (скорость, заряд),
   обрабатывает её production-образом и реагирует на проблемы.

   В отличие от ex01–04 (которые показывают отдельные операторы), здесь —
   как это собирается в работающий сервис:
     • safe_map      — битые показания сенсоров не роняют сервис
     • window_agg    — метрики по депо в окнах (Agg: три за один проход)
     • allowed_lateness — телеметрия приходит с сети вразнобой и с
                          опозданием; окна по event-time это терпят
     • exactly-once  — накопительная статистика по депо ведётся с
                       durable checkpoint-ами (переживает перезапуск)
     • retry         — публикация сводки во внешнюю систему устойчива к
                       временным сбоям

   ex06 показывает как несколько таких сервисов оркеструются (fan_out +
   supervisor). Здесь — один сервис целиком.
   ============================================================ *)

open Miniflink
open Time

(* ── Модель ───────────────────────────────────────────────── *)

type telemetry = {
  bus_id : string;
  depot  : string;
  speed  : float;        (* км/ч *)
  charge : float;        (* заряд батареи %, 0..100 *)
  ts     : Time.t;
}

module ByDepot = Keyed.Make (struct
  type t = telemetry
  let key t = t.depot
end)

let low_charge_threshold = 20.   (* % — ниже этого автобус скоро встанет *)

(* ── Приём сырой телеметрии и валидация ───────────────────── *)
(* Сенсоры иногда сбоят: заряд вне 0..100, скорость NaN. Такие показания
   надо отбросить, не роняя сервис. validate бросает исключение на
   битом — safe_map в пайплайне его поймает. *)

type raw = { r_bus : string; r_depot : string; r_speed : float;
             r_charge : float; r_ts : Time.t }

exception Bad_reading of string

let validate (r : raw) : telemetry =
  if r.r_charge < 0. || r.r_charge > 100. then
    raise (Bad_reading (Printf.sprintf "bus %s: charge %.0f out of range" r.r_bus r.r_charge));
  if Float.is_nan r.r_speed || r.r_speed < 0. then
    raise (Bad_reading (Printf.sprintf "bus %s: invalid speed" r.r_bus));
  { bus_id = r.r_bus; depot = r.r_depot; speed = r.r_speed;
    charge = r.r_charge; ts = r.r_ts }

(* Входящая телеметрия за смену. Приходит с сети — порядок прихода не
   совпадает с event-time (r_ts), бывают опоздавшие, изредка битые. *)
let incoming = [
  { r_bus="B1"; r_depot="north"; r_speed=42.; r_charge=80.; r_ts=seconds 2 };
  { r_bus="B2"; r_depot="north"; r_speed=38.; r_charge=15.; r_ts=seconds 5 };
  { r_bus="B4"; r_depot="south"; r_speed=60.; r_charge=8.;  r_ts=seconds 12 };
  { r_bus="B1"; r_depot="north"; r_speed=44.; r_charge=70.; r_ts=seconds 1 };
  { r_bus="B5"; r_depot="north"; r_speed=40.; r_charge=(-1.); r_ts=seconds 4 }; (* битый заряд *)
  { r_bus="B3"; r_depot="south"; r_speed=55.; r_charge=95.; r_ts=seconds 8 };
  { r_bus="B2"; r_depot="north"; r_speed=45.; r_charge=12.; r_ts=seconds 6 };
  { r_bus="B6"; r_depot="south"; r_speed=nan; r_charge=70.; r_ts=seconds 9 };   (* битая скорость *)
  { r_bus="B7"; r_depot="north"; r_speed=30.; r_charge=55.; r_ts=seconds 15 };
  { r_bus="B4"; r_depot="south"; r_speed=62.; r_charge=9.;  r_ts=seconds 11 };
  { r_bus="B8"; r_depot="south"; r_speed=48.; r_charge=33.; r_ts=seconds 18 };
  { r_bus="B2"; r_depot="north"; r_speed=41.; r_charge=11.; r_ts=seconds 7 };   (* опоздал в окно [0,10) *)
  { r_bus="B3"; r_depot="south"; r_speed=50.; r_charge=18.; r_ts=seconds 16 };
]

(* ── Сервис 1: метрики по депо в окнах ────────────────────── *)
(* По каждому депо за 10-секундное окно: средняя скорость, минимальный
   заряд, сколько автобусов с низким зарядом. Низкий заряд — сигнал
   диспетчеру. *)

let dropped = ref 0

let depot_metrics source =
  source
  |> Pipe.safe_map
       ~on_error:(fun e ->
         incr dropped;
         Log.warn ~fields:[("error", Printexc.to_string e)] "skipped bad reading")
       validate
  |> Pipe.event_time ~lateness:(seconds 1)
  |> Pipe.window_agg (module ByDepot)
       ~allowed_lateness:(seconds 30)
       (Pipe.tumbling (seconds 10))
       Agg.(let+ avg_speed  = mean   (fun t -> t.speed)
            and+ min_charge = min_by (fun t -> t.charge)
            and+ low_count  = count_if (fun t -> t.charge < low_charge_threshold)
            in (avg_speed, min_charge, low_count))

(* ── Сервис 2: накопительная статистика по депо (exactly-once) ─ *)
(* Суммарная статистика по депо ведётся с exactly-once и durable
   checkpoint-ами: при перезапуске процесса состояние поднимается из
   последнего checkpoint без потерь и двойного счёта. *)

module CP = Checkpoint_parallel

type depot_total = { sum_speed : float; n : int }

(* состояние сериализуем с версией — на будущее, когда формат изменится *)
let total_schema : depot_total Schema_default.t =
  Schema_default.make ~version:1
    ~encode:(fun s -> Bytes.of_string (Printf.sprintf "%f;%d" s.sum_speed s.n))
    ~decode:(fun b ->
      match String.split_on_char ';' (Bytes.to_string b) with
      | [a; n] -> (try Ok { sum_speed=float_of_string a; n=int_of_string n }
                   with _ -> Error "bad encoding")
      | _ -> Error "bad field count")
    ()

let accumulate backend ev =
  match ev with
  | Mf_event.Data (t, _) ->
    let cur = match State_backend_memory.get backend t.depot with
      | Some b -> (match Schema_default.decode total_schema b with
                   | Ok s -> s | Error _ -> { sum_speed=0.; n=0 })
      | None -> { sum_speed=0.; n=0 } in
    let s' = { sum_speed = cur.sum_speed +. t.speed; n = cur.n + 1 } in
    State_backend_memory.set backend t.depot (Schema_default.encode total_schema s');
    [(t.depot, s'.sum_speed)]
  | _ -> []

let valid_telemetry () =
  List.filter_map (fun r -> try Some (validate r) with _ -> None) incoming
  |> Mf_event.of_list ~ts:(fun t -> t.ts)
  |> Stream.to_list

(* запустить накопление с durable checkpoint-ами *)
let run_accumulation events ~persist =
  let store = CP.make_store ~persist () in
  let totals = Hashtbl.create 4 in
  let mu = Mutex.create () in
  CP.run_exactly_once
    ~workers:1 ~capacity:64 ~checkpoint_every:3
    ~key_of:(fun t -> t.depot) ~make_state:State_backend_memory.create
    ~process:accumulate
    ~source:(CP.seekable_of_list events)
    ~sink:(CP.idempotent_sink (fun (d, sum) ->
      Mutex.lock mu; Hashtbl.replace totals d sum; Mutex.unlock mu))
    ~store ();
  (store, totals)

(* ── Сервис 3: публикация сводки наружу (с retry) ─────────── *)
(* Внешняя система приёма сводок иногда недоступна — повторяем с backoff. *)

let publish_attempts = ref 0
let publish_summary payload =
  incr publish_attempts;
  if !publish_attempts < 3 then failwith "reporting endpoint unavailable";
  Log.info ~fields:[("summary", payload)] "fleet summary published"

(* ── Сборка сервиса ───────────────────────────────────────── *)

let () =
  Printf.printf "=== Сервис мониторинга флота ===\n\n";

  (* 1. Метрики по депо: считаем окна, реагируем на низкий заряд *)
  Printf.printf "Метрики по депо (окна 10с):\n";
  (* финальное состояние каждого окна (materialize применяет retract'ы
     пере-счёта опоздавших; идентичность записи = (депо, конец окна)) *)
  Mf_event.of_list ~ts:(fun r -> r.r_ts) incoming
  |> depot_metrics
  |> Pipe.materialize ~by:(fun (depot, _) wend -> (depot, wend))
  |> List.sort compare
  |> List.iter (fun ((depot, wend), (_, (avg, min_c, low))) ->
       let avg_s = match avg with Some a -> Printf.sprintf "%.0f км/ч" a | None -> "—" in
       let mc_s  = match min_c with Some c -> Printf.sprintf "%.0f%%" c | None -> "—" in
       Printf.printf "  [%2ds] %-6s ср.скорость %-8s мин.заряд %-5s"
         (wend/1000) depot avg_s mc_s;
       if low > 0 then
         Printf.printf "  ⚠ %d автобус(ов) с низким зарядом" low;
       print_newline ())
  ;
  if !dropped > 0 then
    Printf.printf "  (%d битых показаний отброшено сенсорной валидацией)\n" !dropped;

  (* 2. Накопительная статистика по депо. Сервис ведёт её с exactly-once
     и durable checkpoint-ами — состояние переживёт перезапуск процесса
     (checkpoint восстанавливается при старте; здесь показываем сам учёт). *)
  Printf.printf "\nСуммарная статистика по депо (exactly-once, durable checkpoints):\n";
  let events = valid_telemetry () in
  let (store, totals) = run_accumulation events
      ~persist:(fun _cp -> ()) in   (* в проде: запись checkpoint на диск *)
  Hashtbl.fold (fun d s acc -> (d,s)::acc) totals []
  |> List.sort compare
  |> List.iter (fun (d, s) ->
       Printf.printf "  %-6s суммарная скорость %.0f (checkpoint-ов: %d)\n"
         d s (CP.checkpoint_count store));

  (* 3. Публикация сводки наружу с retry *)
  Printf.printf "\nПубликация сводки во внешнюю систему:\n";
  let summary = Printf.sprintf "depots=%d events=%d"
      (Hashtbl.length totals) (List.length events) in
  Retry.with_retry Retry.default
    ~sleep:(fun _ -> ())
    ~on_give_up:(fun _ n -> Printf.printf "  не удалось опубликовать после %d попыток\n" n)
    publish_summary summary
  |> ignore;
  Printf.printf "  опубликовано (попыток: %d)\n" !publish_attempts;

  (* 4. Health для мониторинга самого сервиса *)
  Printf.printf "\nHealth: %s\n"
    (Health.to_json (Health.check ~state_size:(fun () -> List.length events) ()))
