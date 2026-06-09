(* Реальный librdkafka-биндинг — прод-реализация Kafka_client.CONSUMER и
   PRODUCER через C FFI (kafka_stubs.c → librdkafka).

   ВНИМАНИЕ: требует системную librdkafka и собирается ОТДЕЛЬНЫМ профилем
   (см. dune.rdkafka). В основную сборку miniFlink НЕ входит — ядро и
   адаптер тестируются на Kafka_fake без брокера. Подключай этот модуль в
   приложении, где librdkafka доступна.

   Логика адаптера (offset-маппинг, seek, EO) — в Kafka_source, общая для
   фейка и реального клиента; здесь только транспорт. *)

open Kafka_client

module Raw = struct
  external producer_new   : (string * string) list -> Obj.t = "caml_rdk_producer_new"
  external produce        : Obj.t -> string -> int -> string option -> string -> int = "caml_rdk_produce"
  external flush          : Obj.t -> int -> int = "caml_rdk_flush"
  external consumer_new   : (string * string) list -> Obj.t = "caml_rdk_consumer_new"
  external subscribe      : Obj.t -> string list -> int = "caml_rdk_subscribe"
  external consumer_poll  : Obj.t -> int -> (string * int * int64 * string option * string) option = "caml_rdk_consumer_poll"
  external commit_offset  : Obj.t -> string -> int -> int64 -> int = "caml_rdk_commit_offset"
  external seek           : Obj.t -> string -> int -> int64 -> int = "caml_rdk_seek"
  external consumer_close : Obj.t -> unit = "caml_rdk_consumer_close"
end

module Consumer = struct
  type t = { rk : Obj.t }

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
  type t = { rk : Obj.t }

  let create ~brokers ?transactional_id () =
    let conf = ("bootstrap.servers", brokers) ::
      (match transactional_id with Some tid -> ["transactional.id", tid] | None -> []) in
    { rk = Raw.producer_new conf }

  let produce t ~topic ~partition ~key ~payload =
    ignore (Raw.produce t.rk topic partition key payload)

  let flush t ~timeout_ms = ignore (Raw.flush t.rk timeout_ms)

  (* транзакции: C-стабы init_transactions/begin/commit нужно добавить в
     kafka_stubs.c для полноценного EOS; пока флаш как граница *)
  let begin_txn _ = ()
  let commit_txn t = flush t ~timeout_ms:10000
  let abort_txn _ = ()
  let close t = flush t ~timeout_ms:10000
end
