(* Реальный librdkafka-биндинг — прод-реализация Kafka_client.CONSUMER и
   PRODUCER через C FFI (kafka_stubs.c → librdkafka).

   ВНИМАНИЕ: требует системную librdkafka и собирается ОТДЕЛЬНЫМ профилем
   (см. dune.rdkafka). В основную сборку miniFlink НЕ входит — ядро и
   адаптер тестируются на Kafka_fake без брокера. Подключай этот модуль в
   приложении, где librdkafka доступна.

   Логика адаптера (offset-маппинг, seek, EO) — в Kafka_source, общая для
   фейка и реального клиента; здесь только транспорт. *)

(* Kafka_client приходит из библиотеки kafka_connector (этот биндинг —
   отдельная library из-за C-стабов), поэтому ссылаемся через неё. *)
open Kafka_connector
open Kafka_client

(* Непрозрачный хэндл на rd_kafka_t. На C-стороне это custom block
   (см. kafka_stubs.c, alloc_rk + rk_finalize), который OCaml видит
   как value. Раньше тип был Obj.t — это работало, но Obj.t стирает
   различие между consumer- и producer-хэндлами, так что компилятор
   не поймал бы случайную передачу producer'а в consumer_poll (→ UB
   в C). Abstract type не даёт большей строгости между Consumer и
   Producer (оба используют один handle), но убирает Obj и делает
   намерение явным: это непрозрачный указатель, а не произвольный
   OCaml-объект. *)
type handle

module Raw = struct
  external producer_new   : (string * string) list -> handle = "caml_rdk_producer_new"
  external produce        : handle -> string -> int -> string option -> string -> int = "caml_rdk_produce"
  external flush          : handle -> int -> int = "caml_rdk_flush"
  external init_txns      : handle -> int -> int = "caml_rdk_init_transactions"
  external begin_txn      : handle -> int = "caml_rdk_begin_transaction"
  external commit_txn     : handle -> int -> int = "caml_rdk_commit_transaction"
  external abort_txn      : handle -> int -> int = "caml_rdk_abort_transaction"
  external consumer_new   : (string * string) list -> handle = "caml_rdk_consumer_new"
  external subscribe      : handle -> string list -> int = "caml_rdk_subscribe"
  external consumer_poll  : handle -> int -> (string * int * int64 * string option * string) option = "caml_rdk_consumer_poll"
  external commit_offset  : handle -> string -> int -> int64 -> int = "caml_rdk_commit_offset"
  external seek           : handle -> string -> int -> int64 -> int = "caml_rdk_seek"
  external consumer_close : handle -> unit = "caml_rdk_consumer_close"
end

module Consumer = struct
  type t = { rk : handle }

  let create ~brokers ~group_id ~topics =
    let rk = Raw.consumer_new [
      "bootstrap.servers", brokers;
      "group.id", group_id;
      "enable.auto.commit", "false";   (* ручной commit для EO *)
      "auto.offset.reset", "earliest";
    ] in
    ignore (Raw.subscribe rk topics);
    { rk }

  let poll t ~timeout_ms =
    match Raw.consumer_poll t.rk timeout_ms with
    | None -> None
    | Some (topic, partition, offset, m_key, m_payload) ->
      Some { m_pos = { topic; partition; offset }; m_key; m_payload }

  let seek t (po : partition_offset) =
    ignore (Raw.seek t.rk po.topic po.partition po.offset)

  let commit t (po : partition_offset) =
    ignore (Raw.commit_offset t.rk po.topic po.partition po.offset)

  let close t = Raw.consumer_close t.rk
end

module Producer = struct
  type t = { rk : handle; transactional : bool }

  let create ~brokers ?transactional_id () =
    let conf = ("bootstrap.servers", brokers) ::
      (match transactional_id with Some tid -> ["transactional.id", tid] | None -> []) in
    let rk = Raw.producer_new conf in
    let transactional = transactional_id <> None in
    (* init_transactions один раз при старте транзакционного producer'а;
       регистрирует producer в координаторе транзакций брокера. Без неё
       begin/commit_transaction вернут ошибку. *)
    if transactional then begin
      let rc = Raw.init_txns rk 30000 in
      if rc <> 0 then
        failwith (Printf.sprintf "Kafka init_transactions failed (err=%d)" rc)
    end;
    { rk; transactional }

  let produce t ~topic ~partition ~key ~payload =
    ignore (Raw.produce t.rk topic partition key payload)

  let flush t ~timeout_ms = ignore (Raw.flush t.rk timeout_ms)

  (* Реальные транзакции для exactly-once sink. Для нетранзакционного
     producer'а (transactional_id не задан) — no-op + flush как граница,
     чтобы тот же адаптер работал и без EOS. *)
  let begin_txn t =
    if t.transactional then begin
      let rc = Raw.begin_txn t.rk in
      if rc <> 0 then
        failwith (Printf.sprintf "Kafka begin_transaction failed (err=%d)" rc)
    end

  let commit_txn t =
    if t.transactional then begin
      (* commit_transaction сам флашит и дожидается доставки всех
         сообщений транзакции перед коммитом. *)
      let rc = Raw.commit_txn t.rk 30000 in
      if rc <> 0 then
        failwith (Printf.sprintf "Kafka commit_transaction failed (err=%d)" rc)
    end else
      flush t ~timeout_ms:10000

  let abort_txn t =
    if t.transactional then begin
      let rc = Raw.abort_txn t.rk 30000 in
      if rc <> 0 then
        failwith (Printf.sprintf "Kafka abort_transaction failed (err=%d)" rc)
    end

  let close t = flush t ~timeout_ms:10000
end
