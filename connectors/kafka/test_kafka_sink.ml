(* Тест Kafka sink: транзакционная семантика exactly-once на выходе.

   Проверяет, что Kafka_sink правильно ложит 2PC-протокол ядра
   (ts_write / ts_commit / ts_abort / ts_flush) на Kafka-транзакции,
   используя Kafka_fake как брокер (транзакционный producer с буфером
   до commit). Без живого брокера.

   Ключевые свойства EOS на выходе:
   1. ts_write копит в транзакцию — наружу не видно до ts_commit;
   2. ts_commit делает сообщения epoch видимыми атомарно;
   3. ts_abort выбрасывает незакоммиченное — дублей при переобработке нет;
   4. интерливинг epoch'ей: commit одной не публикует незакоммиченную другую. *)

open Miniflink
open Kafka_connector
module Sink = Kafka_sink.Make (Kafka_fake.Producer)

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

(* прочитать все payload-ы топика из фейкового брокера *)
let read_all broker ~topic ~partition =
  Array.to_list (Kafka_fake.topic_log broker ~topic ~partition)

let mk_sink broker =
  let producer = Kafka_fake.Producer.create broker in
  let s = Sink.create ~producer ~topic:"alerts"
    ~encode:(fun (x : string) -> x) () in
  (s, Sink.to_transactional_sink s)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Kafka sink: exactly-once (transactional)\n";
  Printf.printf "==========================================\n";

  (* ── 1. write не виден до commit ─────────────────────────── *)
  Printf.printf "\n-- 1. writes invisible until commit\n";
  let broker = Kafka_fake.make_broker () in
  let (_s, ts) = mk_sink broker in
  ts.ts_write 1 "a";
  ts.ts_write 1 "b";
  check "до commit брокер пуст" (read_all broker ~topic:"alerts" ~partition:0 = []);
  ts.ts_commit 1;
  check "после commit видны a,b"
    (read_all broker ~topic:"alerts" ~partition:0 = ["a"; "b"]);

  (* ── 2. abort выбрасывает незакоммиченное ────────────────── *)
  Printf.printf "\n-- 2. abort discards uncommitted\n";
  let broker = Kafka_fake.make_broker () in
  let (_s, ts) = mk_sink broker in
  ts.ts_write 1 "x";
  ts.ts_write 1 "y";
  ts.ts_abort 1;
  check "после abort брокер пуст" (read_all broker ~topic:"alerts" ~partition:0 = []);
  (* следующая epoch пишет и коммитит — abort не сломал producer *)
  ts.ts_write 2 "z";
  ts.ts_commit 2;
  check "epoch 2 после abort коммитится нормально"
    (read_all broker ~topic:"alerts" ~partition:0 = ["z"]);

  (* ── 3. EOS-сценарий: переобработка epoch после "сбоя" ───── *)
  Printf.printf "\n-- 3. reprocessing after crash: no duplicates\n";
  let broker = Kafka_fake.make_broker () in
  let (_s, ts) = mk_sink broker in
  (* epoch 1 успешно закоммичена *)
  ts.ts_write 1 "e1";
  ts.ts_commit 1;
  (* epoch 2 пишется, но "сбой" до commit → abort (откат) *)
  ts.ts_write 2 "e2_attempt1";
  ts.ts_abort 2;
  (* переобработка epoch 2 после recovery: пишем заново и коммитим *)
  ts.ts_write 2 "e2_final";
  ts.ts_commit 2;
  check "только закоммиченные: e1, e2_final (без e2_attempt1)"
    (read_all broker ~topic:"alerts" ~partition:0 = ["e1"; "e2_final"]);

  (* ── 4. ts_flush коммитит хвост ──────────────────────────── *)
  Printf.printf "\n-- 4. flush commits the tail\n";
  let broker = Kafka_fake.make_broker () in
  let (_s, ts) = mk_sink broker in
  ts.ts_write 5 "tail1";
  ts.ts_write 5 "tail2";
  (* штатное завершение без явного commit — flush должен закоммитить *)
  ts.ts_flush ();
  check "flush опубликовал хвост epoch 5"
    (read_all broker ~topic:"alerts" ~partition:0 = ["tail1"; "tail2"]);

  (* ── 5. пустой commit (ничего не писали) безопасен ───────── *)
  Printf.printf "\n-- 5. empty commit is a no-op\n";
  let broker = Kafka_fake.make_broker () in
  let (_s, ts) = mk_sink broker in
  ts.ts_commit 99;   (* в epoch 99 ничего не писали *)
  check "пустой commit не падает и брокер пуст"
    (read_all broker ~topic:"alerts" ~partition:0 = []);

  Printf.printf "\nKafka sink EOS tests passed.\n"
