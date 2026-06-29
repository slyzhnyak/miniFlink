(* Kafka sink adapter — реализует Checkpoint_parallel.transactional_sink
   через Kafka-транзакции, замыкая exactly-once на ВЫХОДНОЙ стороне.

   Протокол 2PC ядра ложится на Kafka-транзакции один-к-одному:
   - ts_write epoch v  — produce сообщения в открытую транзакцию epoch'и
                         (буферизуется на стороне брокера/клиента, наружу
                         НЕ видно до commit);
   - ts_commit epoch   — commit_transaction: сообщения epoch'и становятся
                         видимы потребителям с isolation.level=read_committed
                         атомарно;
   - ts_abort epoch    — abort_transaction: незакоммиченные сообщения
                         epoch'и отбрасываются (при переобработке после
                         сбоя дубли не попадут к читателю);
   - ts_flush          — финализировать последнюю открытую транзакцию при
                         штатном завершении.

   Транзакция открывается лениво на первом ts_write новой epoch'и.
   Между epoch'ами producer вне транзакции. Это соответствует
   barrier-протоколу ядра: одна транзакция Kafka = одна epoch
   чекпойнта.

   Параметризован по Kafka_client.PRODUCER, поэтому работает и с
   Kafka_fake (тесты без брокера), и с Kafka_rdkafka (прод). Сериализация
   значения в payload — функция приложения (encode). *)

open Miniflink
open Kafka_client

module Make (P : PRODUCER) = struct
  type 'b t = {
    producer  : P.t;
    topic     : string;
    encode    : 'b -> string;            (* значение → payload *)
    key_of    : 'b -> string option;     (* опц. ключ партиционирования *)
    partition : 'b -> int;               (* -1 = авто *)
    mutable open_epoch : int option;     (* epoch текущей открытой txn *)
  }

  let create ~producer ~topic ~encode
      ?(key_of = fun _ -> None) ?(partition = fun _ -> -1) () =
    { producer; topic; encode; key_of; partition; open_epoch = None }

  (* открыть транзакцию для epoch, если ещё не открыта для неё *)
  let ensure_txn t epoch =
    match t.open_epoch with
    | Some e when e = epoch -> ()        (* уже открыта для этой epoch *)
    | Some _ ->
      (* пришёл write для НОВОЙ epoch, а старая не закоммичена — этого не
         должно происходить (ядро коммитит/абортит epoch перед следующей),
         но на всякий случай закрываем старую коммитом, чтобы не потерять. *)
      P.commit_txn t.producer;
      P.begin_txn t.producer;
      t.open_epoch <- Some epoch
    | None ->
      P.begin_txn t.producer;
      t.open_epoch <- Some epoch

  let to_transactional_sink (t : 'b t)
      : 'b Checkpoint_parallel.transactional_sink =
    {
      ts_write = (fun epoch v ->
        ensure_txn t epoch;
        P.produce t.producer
          ~topic:t.topic
          ~partition:(t.partition v)
          ~key:(t.key_of v)
          ~payload:(t.encode v));

      ts_commit = (fun epoch ->
        match t.open_epoch with
        | Some e when e = epoch ->
          P.commit_txn t.producer;
          t.open_epoch <- None
        | _ ->
          (* commit epoch'и, в которую ничего не писали — нет открытой
             транзакции, делать нечего (пустой чекпойнт). *)
          ());

      ts_abort = (fun epoch ->
        match t.open_epoch with
        | Some e when e = epoch ->
          P.abort_txn t.producer;
          t.open_epoch <- None
        | _ -> ());

      ts_flush = (fun () ->
        (* штатное завершение: закоммитить последнюю открытую транзакцию *)
        match t.open_epoch with
        | Some _ -> P.commit_txn t.producer; t.open_epoch <- None
        | None -> ());
    }

  let close t = P.close t.producer
end
