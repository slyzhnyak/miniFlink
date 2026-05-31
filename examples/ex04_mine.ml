(* ============================================================
   Пример 4 — комплексная топология (мониторинг шахты).

   Показывает весь функционал в одном реалистичном сценарии:

     • ТРИ источника: позиционирование людей, газовые датчики,
       поток конфигурации датчиков
     • union — слияние людей и газа в общий поток телеметрии по
       event-time
     • update_table — поток конфигурации наполняет таблицу порогов
     • enrich — телеметрия обогащается зоной/порогом из таблиц
     • tumbling window — средний уровень газа за окно
     • session window — периоды активности человека под землёй
     • global window + trigger — батчи показаний газа (count-триггер)
     • aggregate, flat_map (правила), dedup — алерты без спама

   Топология:

     config ──update_table──> [таблица порогов]
                                     │ enrich
     gas    ──┐                      ▼
              union──> telemetry ──enrich──> ┌─ tumbling → avg → rules → dedup → АЛЕРТЫ
     person ──┘                              ├─ global+trigger → критический газ (рано)
                                             └─ session → активность людей

   Запуск: dune exec examples/ex04_mine.exe
   ============================================================ *)

open Time

(* ════════════════════════════════════════════════════════════
   Типы событий
   ════════════════════════════════════════════════════════════ *)

(* Единый тип телеметрии — и люди, и газ (чтобы объединять через union).
   Вид различается полем kind. *)
type telemetry = {
  kind     : [ `Person | `Gas ];
  id       : string;          (* табельный номер или ID датчика *)
  horizon  : int;             (* горизонт, м (−80, −160, ...) *)
  ch4      : float;           (* для газа: метан %, для людей 0 *)
  ts       : int;
  zone     : string option;   (* заполняется enrich *)
  limit    : float option;    (* порог газа, заполняется enrich *)
}

module Tel = Keyed.Make (struct
  type t = telemetry
  let key t = t.id
end)

(* Конфигурация датчика: порог газа. Приходит отдельным потоком. *)
type sensor_config = { cfg_id : string; cfg_limit : float; cfg_ts : int }

(* Алерт. *)
type alert = { a_id : string; a_kind : string; a_ts : int; a_detail : string }

module Alert = Keyed.Make (struct
  type t = alert
  let key a = a.a_id
end)

(* ════════════════════════════════════════════════════════════
   Источники данных (мок)
   ════════════════════════════════════════════════════════════ *)

let mk_person id horizon ts =
  { kind = `Person; id; horizon; ch4 = 0.; ts; zone = None; limit = None }
let mk_gas id horizon ch4 ts =
  { kind = `Gas; id; horizon; ch4; ts; zone = None; limit = None }

(* Поток позиционирования людей: активность, пауза, снова активность *)
let person_events = [
  mk_person "p884460" (-80) (seconds 1);
  mk_person "p884460" (-80) (seconds 3);
  mk_person "p884461" (-80) (seconds 4);
  mk_person "p884460" (-80) (seconds 6);
  (* пауза человека p884460 ~40с *)
  mk_person "p884460" (-80) (seconds 48);
  mk_person "p884460" (-80) (seconds 50);
]

(* Поток газовых датчиков: растущий метан на одном датчике *)
let gas_events = [
  mk_gas "gas-A" (-80) 0.4 (seconds 2);
  mk_gas "gas-A" (-80) 0.6 (seconds 5);
  mk_gas "gas-A" (-80) 0.9 (seconds 8);    (* приближается к порогу *)
  mk_gas "gas-A" (-80) 1.4 (seconds 11);   (* превышение! *)
  mk_gas "gas-B" (-160) 0.3 (seconds 7);
  mk_gas "gas-A" (-80) 1.6 (seconds 14);   (* всё ещё превышение *)
]

(* Поток конфигурации: пороги датчиков (могут меняться) *)
let config_events = [
  { cfg_id = "gas-A"; cfg_limit = 1.0; cfg_ts = seconds 0 };
  { cfg_id = "gas-B"; cfg_limit = 1.2; cfg_ts = seconds 0 };
]

(* Справочник зон по горизонту (статический) *)
let zones = Table.of_list [ ("-80", "западный штрек"); ("-160", "южный штрек") ]

(* ════════════════════════════════════════════════════════════
   Топология
   ════════════════════════════════════════════════════════════ *)

let () =
  Printf.printf "=== Пример 4: комплексный мониторинг шахты ===\n\n";

  (* ── 1. Таблица порогов, наполняемая из потока конфигурации ── *)
  let limits : (string, sensor_config) Hashtbl.t = Hashtbl.create 8 in
  Stream.of_list (List.map (fun c -> Mf_event.data c c.cfg_ts) config_events)
  |> Pipe.update_table limits ~key:(fun c -> c.cfg_id)
  |> Stream.to_list |> ignore;          (* прогнали — таблица наполнена *)
  Printf.printf "Конфигурация загружена: %d порогов\n\n" (Hashtbl.length limits);

  (* ── 2. union людей и газа в общий поток по event-time ────── *)
  let person_stream =
    Stream.of_list (List.map (fun p -> Mf_event.data p p.ts) person_events) in
  let gas_stream =
    Stream.of_list (List.map (fun g -> Mf_event.data g g.ts) gas_events) in
  let telemetry = Mf_event.union person_stream gas_stream in

  (* ── 3. обогащение зоной и порогом ────────────────────────── *)
  (* для примера материализуем обогащённый поток в список и переиспользуем *)
  let enriched_list =
    telemetry
    |> Mf_event.with_watermarks ~latency:(seconds 2)
    |> Pipe.enrich (module Tel)
         ~from:(fun id ->
           (* зону берём по горизонту, но enrich даёт по ключу id —
              поэтому зону подставим в merge напрямую из таблицы зон *)
           ignore id; None)
         ~merge:(fun t _ ->
           let zone = zones (string_of_int t.horizon) in
           let limit = match Hashtbl.find_opt limits t.id with
             | Some c -> Some c.cfg_limit | None -> None in
           { t with zone; limit })
    |> Stream.to_list in
  let enriched () = Stream.of_list enriched_list in

  (* ── 4а. ГАЗ: tumbling окно → средний метан → правило → dedup ── *)
  Printf.printf "── Средний метан по датчику (окно 10с) и алерты превышения ──\n";
  enriched ()
  |> Pipe.filter (fun t -> t.kind = `Gas)
  |> Pipe.window (module Tel) (Pipe.tumbling (seconds 10))
  |> Pipe.aggregate (fun id readings ->
       let n = List.length readings in
       let avg = List.fold_left (fun a r -> a +. r.ch4) 0. readings /. float_of_int n in
       let lim = List.fold_left (fun acc r -> match r.limit with Some l -> Some l | None -> acc) None readings in
       (id, avg, lim))
  |> Pipe.flat_map (fun (id, avg, lim) ->
       match lim with
       | Some l when avg > l ->
         [{ a_id = id; a_kind = "gas_avg_over"; a_ts = 0;
            a_detail = Printf.sprintf "средний CH4 %.2f%% > порог %.1f%%" avg l }]
       | _ -> [])
  |> Pipe.dedup (module Alert) ~rule:(fun a -> a.a_kind) ~cooldown:(seconds 30)
  |> Pipe.sink (fun a -> Printf.printf "  🔴 АЛЕРТ %s: %s\n" a.a_id a.a_detail);

  (* ── 4б. ГАЗ: мгновенная реакция на критический метан ─────── *)
  (* Здесь нужна реакция на КАЖДОЕ критическое показание сразу, без
     накопления — это чистый filter+map, а не окно. (global_window с
     накопительным триггером тут дал бы дубли на конце потока — окна
     хороши для агрегации, а не для пер-событийной реакции.) *)
  Printf.printf "\n── Мгновенная реакция на критический метан (>1.3%%) ──\n";
  enriched ()
  |> Pipe.filter (fun t -> t.kind = `Gas && t.ch4 > 1.3)
  |> Pipe.map (fun t -> { a_id = t.id; a_kind = "critical_gas"; a_ts = t.ts;
                          a_detail = Printf.sprintf "CH4 %.1f%% (%s)" t.ch4
                            (match t.zone with Some z -> z | None -> "?") })
  (* подавляем повтор по датчику в пределах 5с, чтобы не спамить *)
  |> Pipe.dedup (module Alert) ~rule:(fun a -> a.a_kind) ~cooldown:(seconds 5)
  |> Pipe.sink (fun a ->
       Printf.printf "  ⚠️  КРИТИЧНО %s: %s\n" a.a_id a.a_detail);

  (* ── 4в. ЛЮДИ: session окно → периоды активности под землёй ── *)
  Printf.printf "\n── Сессии активности людей (пауза > 20с разрывает) ──\n";
  enriched ()
  |> Pipe.filter (fun t -> t.kind = `Person)
  |> Pipe.session_window (module Tel) ~gap:(seconds 20)
  |> Pipe.aggregate (fun id events ->
       let first = (List.hd events).ts in
       let last = (List.fold_left (fun _ e -> e) (List.hd events) events).ts in
       (id, List.length events, first, last))
  |> Pipe.sink (fun (id, n, first, last) ->
       Printf.printf "  👤 %s: сессия %d событий, %ds..%ds (%dс под землёй активен)\n"
         id n (first/1000) (last/1000) ((last - first)/1000));

  (* ── 4г. ГАЗ: global window + count-триггер ───────────────── *)
  (* Прогрессивная сводка: каждые 2 показания датчика выдаём батч
     (FireAndPurge — батчи не пересекаются). Показывает триггеры и то,
     что на конце потока неполный остаток выходит без дублей. *)
  Printf.printf "\n── Батчи показаний газа (global window, каждые 2 замера) ──\n";
  enriched ()
  |> Pipe.filter (fun t -> t.kind = `Gas)
  |> Pipe.global_window (module Tel) ~trigger:(Pipe.trigger_count 2)
  |> Pipe.aggregate (fun id readings ->
       (id, List.map (fun r -> r.ch4) readings))
  |> Pipe.sink (fun (id, vals) ->
       Printf.printf "  📦 %s: батч [%s]\n" id
         (String.concat ", " (List.map (Printf.sprintf "%.1f") vals)));

  Printf.printf "\n=== готово ===\n"
