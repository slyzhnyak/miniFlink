(* Абстрактный Kafka-клиент.

   Адаптер (kafka_source/kafka_sink) написан против этой сигнатуры, а не
   против librdkafka напрямую. Это даёт две реализации:
   - Kafka_rdkafka — реальный librdkafka через C FFI (прод; требует
     librdkafka, не собирается в окружении без неё)
   - Kafka_fake — in-memory брокер для тестов (логика адаптера —
     offset-маппинг, seek, EO-стыковка — проверяется без живого брокера)

   Позиция партиции — (topic, partition, offset). Multi-partition. *)

type partition_offset = {
  topic     : string;
  partition : int;
  offset    : int64;
}

(* сообщение из брокера: позиция + payload (+ опц. ключ) *)
type message = {
  m_pos     : partition_offset;
  m_key     : string option;
  m_payload : string;
}

module type CONSUMER = sig
  type t
  (* poll один шаг; None если сейчас нет данных *)
  val poll        : t -> timeout_ms:int -> message option
  (* перемотать партицию на offset (для recovery) *)
  val seek        : t -> partition_offset -> unit
  (* зафиксировать offset в брокере (после подтверждённого checkpoint) *)
  val commit      : t -> partition_offset -> unit
  val close       : t -> unit
end

module type PRODUCER = sig
  type t
  (* отправить; partition = -1 → авто *)
  val produce     : t -> topic:string -> partition:int ->
                    key:string option -> payload:string -> unit
  (* дождаться отправки буфера (для commit/flush) *)
  val flush       : t -> timeout_ms:int -> unit
  (* транзакции (для exactly-once sink); no-op если не транзакционный *)
  val begin_txn   : t -> unit
  val commit_txn  : t -> unit
  val abort_txn   : t -> unit
  val close       : t -> unit
end
