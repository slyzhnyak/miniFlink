(** {1 Пример 8. Множество параллельных пайплайнов на реальном Kafka}

    Несколько независимых пайплайнов minePASS работают одновременно в
    одном процессе, читая из реальных Kafka-топиков и записывая обратно с
    гарантией exactly-once — {b каждый в своём домене OCaml 5}.

    {2 Что демонстрирует пример}

    - {b Декларативность.} Логика каждого пайплайна — несколько
      комбинаторов (см. {!Pipelines}), без ручного управления состоянием.
    - {b Изоляция.} N пайплайнов = N независимых {!Checkpoint_parallel.
      run_exactly_once}. Ядро single-node не знает о соседях. Изоляция
      держится на трёх идентификаторах на пайплайн: checkpoint-каталог,
      [transactional.id], [group.id].
    - {b Общий вход и общий выход.} Пайплайны [voltage] и [loss] читают из
      {b одного} топика [telemetry.raw] и пишут в {b один} [alerts.out] —
      работает благодаря разным [group.id] (каждый видит полный поток) и
      разным [transactional.id] (транзакции не фенсят друг друга).
    - {b Exactly-once поверх Kafka.} Транзакционный producer + ручной
      commit offset'ов через чекпоинт: при рестарте пайплайн переигрывает
      с последнего чекпоинта, не теряя и не дублируя.

    {2 Архитектура: домен на пайплайн}

    [run_exactly_once] {b блокирует} вызывающий поток (внутри
    [Thread.join] воркеров) и на Kafka-источнике не возвращается — источник
    бесконечен. Поэтому два пайплайна нельзя запустить последовательно в
    одном потоке: первый заблокирует второй.

    Решение — {b каждый пайплайн в отдельном} [Domain.spawn]. Даёт сразу:
    - пайплайны идут {b одновременно} (домены не блокируют друг друга);
    - {b настоящий параллелизм по ядрам} между пайплайнами (в OCaml 5
      разные домены — разные ядра; несколько [Thread] внутри одного домена
      делят рантайм-lock и параллелизма по CPU не дают).

    {2 Запуск}

    Нужен брокер на [localhost:9092] и топики [telemetry.raw], [gps.raw],
    [alerts.out], [positions.out]. Собирается в профиле rdkafka:
    {[
      dune exec --profile rdkafka \
        examples/ex08_multi_pipeline/ex08_multi_pipeline.exe
    ]} *)

open Miniflink
open Pipelines
open Kafka_connector

(** Мини-парсер payload ["lamp,beacon,rssi,voltage,ts"]. Возвращает
    [result]: на битой строке — [Error], которую источник направит в DLQ
    через [on_error] (не теряя сообщение). *)
let decode_reading (payload : string) : (reading, string) result =
  match String.split_on_char ',' payload with
  | [ lamp; beacon; rssi; voltage; ts ] ->
    (try
       Ok { lamp; beacon;
            rssi = float_of_string rssi;
            voltage = float_of_string voltage;
            ts_ms = int_of_string ts }
     with _ -> Error ("нечисловое поле: " ^ payload))
  | _ -> Error ("bad payload: " ^ payload)

(** Пайплайн 1 (per-event): низкое напряжение, cooldown 60с через durable
    backend — переживает рестарт вместе с чекпоинтом. *)
let voltage_process (backend : State_backend_memory.t)
    (ev : reading Mf_event.t) : string list =
  match Mf_event.value ev with
  | Some r when r.voltage < 3.2 ->
    let key = "last_alert:" ^ r.lamp in
    let now = r.ts_ms in
    let recent =
      match State_backend_memory.get backend key with
      | Some prev -> now - int_of_string (Bytes.to_string prev) < 60_000
      | None -> false in
    if recent then []
    else begin
      State_backend_memory.set backend key (Bytes.of_string (string_of_int now));
      [ alert_to_string (Low_voltage (r.lamp, r.voltage)) ]
    end
  | _ -> []

(** Пайплайн 2 (per-event): потеря связи. Храним последний event-time по
    лампе; на watermark проверяем, кто «замолчал» дольше 30с. *)
let loss_process (backend : State_backend_memory.t)
    (ev : reading Mf_event.t) : string list =
  match ev with
  | Mf_event.Data (r, _) ->
    State_backend_memory.set backend r.lamp (Bytes.of_string (string_of_int r.ts_ms));
    []
  | Mf_event.Watermark wm ->
    let silent = ref [] in
    List.iter (fun lamp ->
      match State_backend_memory.get backend lamp with
      | Some last_ts when wm - int_of_string (Bytes.to_string last_ts) > 30_000 ->
        silent := alert_to_string (No_contact lamp) :: !silent
      | _ -> ())
      (State_backend_memory.keys backend);
    !silent
  | _ -> []

(** Пайплайн 3 (per-event): позиционирование, мгновенная оценка по каждому
    показанию. Другой вход/выход. *)
let positioning_process (_backend : State_backend_memory.t)
    (ev : reading Mf_event.t) : string list =
  match Mf_event.value ev with
  | Some r -> [ position_to_string { p_lamp = r.lamp;
                                     p_est = 10. ** ((-. r.rssi -. 40.) /. 20.) } ]
  | None -> []

(** Запуск одного пайплайна в exactly-once на реальном Kafka. Три
    идентификатора ([group_id], [txn_id], [cp_dir]) — уникальные на
    пайплайн; именно они обеспечивают изоляцию и корректность общего
    входа/выхода. *)
let run_pipeline
    ~(brokers : string)
    ~(in_topic : string) ~(group_id : string)
    ~(out_topic : string) ~(txn_id : string)
    ~(cp_dir : string)
    ~(process : State_backend_memory.t -> reading Mf_event.t -> string list)
    () : unit =
  let module K = Kafka_rdkafka in
  let module Src = Kafka_source.Make (K.Consumer) in
  let module Snk = Kafka_sink.Make (K.Producer) in
  let consumer = K.Consumer.create ~brokers ~group_id ~topics:[ in_topic ] in
  let source =
    Src.create consumer ~decode:decode_reading ~ts_of:(fun r -> r.ts_ms)
      ~on_error:(fun _ p ->
        Printf.eprintf "[%s] битый payload в DLQ: %s\n%!" group_id p) ()
    |> Src.to_seekable in
  let producer = K.Producer.create ~brokers ~transactional_id:txn_id () in
  let sink =
    Snk.create ~producer ~topic:out_topic ~encode:(fun s -> s) ()
    |> Snk.to_transactional_sink in
  let store = Checkpoint_parallel.durable_store ~dir:cp_dir in
  Printf.printf "[%s] старт: %s → %s (txn=%s, cp=%s)\n%!"
    group_id in_topic out_topic txn_id cp_dir;
  Checkpoint_parallel.run_exactly_once
    ~workers:4 ~capacity:256 ~checkpoint_every:500
    ~key_of:(fun r -> r.lamp)
    ~make_state:State_backend_memory.create
    ~process ~source ~sink ~store ()

(** Оркестрация: каждый пайплайн — свой [Domain.spawn] (вызов блокирующий
    и бесконечный; домены дают параллелизм по ядрам между пайплайнами). *)
let () =
  let brokers =
    try Sys.getenv "KAFKA_BROKERS" with Not_found -> "localhost:9092" in
  Printf.printf "═══ minePASS: 3 пайплайна, домен на каждый ═══\n%!";
  let domains = [|
    Domain.spawn (fun () ->
      run_pipeline ~brokers
        ~in_topic:"telemetry.raw" ~group_id:"minepass.voltage"
        ~out_topic:"alerts.out"   ~txn_id:"minepass.voltage.txn"
        ~cp_dir:"/var/lib/minepass/cp/voltage"
        ~process:voltage_process ());
    (* ТОТ ЖЕ вход telemetry.raw, ТОТ ЖЕ выход alerts.out — но другие
       group_id и txn_id, поэтому корректно. *)
    Domain.spawn (fun () ->
      run_pipeline ~brokers
        ~in_topic:"telemetry.raw" ~group_id:"minepass.loss"
        ~out_topic:"alerts.out"   ~txn_id:"minepass.loss.txn"
        ~cp_dir:"/var/lib/minepass/cp/loss"
        ~process:loss_process ());
    Domain.spawn (fun () ->
      run_pipeline ~brokers
        ~in_topic:"gps.raw"        ~group_id:"minepass.position"
        ~out_topic:"positions.out" ~txn_id:"minepass.position.txn"
        ~cp_dir:"/var/lib/minepass/cp/position"
        ~process:positioning_process ());
  |] in
  Array.iter Domain.join domains;
  Printf.printf "все пайплайны завершены\n%!"
