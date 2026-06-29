(* Интеграционный тест против ЖИВОГО Kafka-брокера через librdkafka.

   Запускается ТОЛЬКО в CI с поднятым брокером (см. job kafka-eos в
   .github/workflows/ci.yml) и переменной KAFKA_BROKERS. Без неё тест
   тихо пропускается (skip), чтобы не падать в обычной сборке без
   librdkafka/брокера.

   Что проверяет (то, что нельзя проверить на фейке):
   - реальные C-стабы транзакций (init/begin/commit/abort) не падают и
     возвращают success-коды против настоящего librdkafka;
   - сообщения, записанные в транзакции, становятся видимы потребителю
     с isolation.level=read_committed ТОЛЬКО после commit;
   - abort'нутая транзакция не доходит до read_committed-потребителя
     (exactly-once на выходе сквозь реальный брокер). *)

open Kafka_connector
open Kafka_client

module Producer = Kafka_rdkafka.Producer
module Consumer = Kafka_rdkafka.Consumer
module Sink = Kafka_sink.Make (Producer)

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let unique_topic prefix =
  Printf.sprintf "%s_%d_%d" prefix (Unix.getpid ())
    (int_of_float (Unix.gettimeofday () *. 1000.) mod 1000000)

(* собрать payload-ы из read_committed-потребителя за отведённое время *)
let drain_committed ~brokers ~topic ~timeout_total_ms =
  let c = Consumer.create ~brokers ~group_id:(unique_topic "grp")
      ~topics:[topic] in
  let deadline = Unix.gettimeofday () +. float_of_int timeout_total_ms /. 1000. in
  let acc = ref [] in
  let rec loop () =
    if Unix.gettimeofday () > deadline then ()
    else match Consumer.poll c ~timeout_ms:500 with
      | Some m -> acc := m.m_payload :: !acc; loop ()
      | None -> loop ()
  in
  loop ();
  Consumer.close c;
  List.rev !acc

let run brokers =
  Printf.printf "Integration: live Kafka broker at %s\n%!" brokers;

  (* ── 1. committed транзакция видна read_committed-потребителю ── *)
  Printf.printf "\n-- 1. committed transaction is visible\n";
  let topic = unique_topic "eos_commit" in
  let producer = Producer.create ~brokers
      ~transactional_id:(unique_topic "txn") () in
  let sink = Sink.create ~producer ~topic ~encode:(fun s -> s) () in
  let ts = Sink.to_transactional_sink sink in
  ts.ts_write 1 "alpha";
  ts.ts_write 1 "beta";
  ts.ts_commit 1;
  Sink.close sink;
  let got = drain_committed ~brokers ~topic ~timeout_total_ms:8000 in
  check "read_committed видит alpha,beta после commit"
    (List.sort compare got = ["alpha"; "beta"]);

  (* ── 2. abort'нутая транзакция НЕ видна ──────────────────── *)
  Printf.printf "\n-- 2. aborted transaction is invisible\n";
  let topic = unique_topic "eos_abort" in
  let producer = Producer.create ~brokers
      ~transactional_id:(unique_topic "txn") () in
  let sink = Sink.create ~producer ~topic ~encode:(fun s -> s) () in
  let ts = Sink.to_transactional_sink sink in
  (* первая попытка — аборт (имитация сбоя до commit) *)
  ts.ts_write 1 "ghost";
  ts.ts_abort 1;
  (* переобработка — пишем заново и коммитим *)
  ts.ts_write 1 "real";
  ts.ts_commit 1;
  Sink.close sink;
  let got = drain_committed ~brokers ~topic ~timeout_total_ms:8000 in
  check "read_committed видит только real (ghost абортнут)"
    (got = ["real"]);

  Printf.printf "\nLive Kafka EOS integration passed.\n"

let () =
  match Sys.getenv_opt "KAFKA_BROKERS" with
  | None | Some "" ->
    Printf.printf "SKIP: KAFKA_BROKERS не задана — интеграционный тест \
                   пропущен (нужен живой брокер).\n%!"
  | Some brokers -> run brokers
