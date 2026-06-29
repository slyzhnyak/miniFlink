/* ============================================================
   kafka_stubs.c — C FFI для librdkafka

   Consumer и Producer через высокоуровневый rdkafka API.

   Ключевые отличия от MQTT:
   - Consumer poll возвращает одно сообщение за раз
   - Offset хранится в rdkafka и коммитится явно
   - Producer async: message delivery report через poll
   - Partition + offset = точная позиция для checkpoint

   Компиляция:
     cc -c kafka_stubs.c -I$(ocamlopt -where) -o kafka_stubs.o
     (линковка: -lrdkafka)
   ============================================================ */

#include <caml/mlvalues.h>
#include <caml/memory.h>
#include <caml/alloc.h>
#include <caml/custom.h>
#include <caml/fail.h>
#include <librdkafka/rdkafka.h>
#include <string.h>
#include <stdlib.h>

/* ── Error buffer ────────────────────────────────────────── */

#define ERR_BUF_LEN 512

/* ── Custom block: rd_kafka_t ────────────────────────────── */

static void rk_finalize(value v) {
    rd_kafka_t *rk = *((rd_kafka_t **)Data_custom_val(v));
    if (rk) {
        rd_kafka_flush(rk, 5000);
        rd_kafka_destroy(rk);
    }
}

static struct custom_operations rk_ops = {
    "miniflink.rdkafka",
    rk_finalize,
    custom_compare_default, custom_hash_default,
    custom_serialize_default, custom_deserialize_default,
    custom_compare_ext_default, custom_fixed_length_default
};

static value alloc_rk(rd_kafka_t *rk) {
    value v = caml_alloc_custom(&rk_ops, sizeof(rd_kafka_t *), 0, 1);
    *((rd_kafka_t **)Data_custom_val(v)) = rk;
    return v;
}
#define Rk_val(v) (*((rd_kafka_t **)Data_custom_val(v)))

/* ── Custom block: rd_kafka_topic_partition_list_t ───────── */

static void tpl_finalize(value v) {
    rd_kafka_topic_partition_list_t *tpl =
        *((rd_kafka_topic_partition_list_t **)Data_custom_val(v));
    if (tpl) rd_kafka_topic_partition_list_destroy(tpl);
}

static struct custom_operations tpl_ops = {
    "miniflink.rdkafka.tpl",
    tpl_finalize,
    custom_compare_default, custom_hash_default,
    custom_serialize_default, custom_deserialize_default,
    custom_compare_ext_default, custom_fixed_length_default
};
#define Tpl_val(v) (*((rd_kafka_topic_partition_list_t **)Data_custom_val(v)))

/* ── conf helpers ────────────────────────────────────────── */

/* Apply OCaml string list [(key,value);...] to conf */
static int apply_conf(rd_kafka_conf_t *conf, value v_pairs, char *errbuf) {
    while (v_pairs != Val_emptylist) {
        value pair  = Field(v_pairs, 0);
        const char *key = String_val(Field(pair, 0));
        const char *val = String_val(Field(pair, 1));
        if (rd_kafka_conf_set(conf, key, val, errbuf, ERR_BUF_LEN)
            != RD_KAFKA_CONF_OK)
            return -1;
        v_pairs = Field(v_pairs, 1);
    }
    return 0;
}

/* ── Producer ────────────────────────────────────────────── */

/* create_producer(conf_pairs) -> rk */
CAMLprim value caml_rdk_producer_new(value v_pairs) {
    CAMLparam1(v_pairs);
    char errbuf[ERR_BUF_LEN];
    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    if (apply_conf(conf, v_pairs, errbuf) < 0) {
        rd_kafka_conf_destroy(conf);
        caml_failwith(errbuf);
    }
    rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errbuf, ERR_BUF_LEN);
    if (!rk) caml_failwith(errbuf);
    CAMLreturn(alloc_rk(rk));
}

/* produce(rk, topic, partition, key_opt, payload) -> int (0=ok) */
CAMLprim value caml_rdk_produce(value v_rk, value v_topic,
                                 value v_part, value v_key, value v_payload) {
    CAMLparam5(v_rk, v_topic, v_part, v_key, v_payload);
    rd_kafka_t *rk = Rk_val(v_rk);
    const char *topic    = String_val(v_topic);
    int32_t     part     = (Int_val(v_part) < 0)
                            ? RD_KAFKA_PARTITION_UA : (int32_t)Int_val(v_part);
    void       *payload  = (void *)String_val(v_payload);
    size_t      plen     = caml_string_length(v_payload);
    void       *key      = NULL;
    size_t      klen     = 0;

    if (Is_block(v_key)) {           /* Some key */
        key  = (void *)String_val(Field(v_key, 0));
        klen = caml_string_length(Field(v_key, 0));
    }

    /* RD_KAFKA_MSG_F_COPY: rdkafka copies payload — safe with GC */
    int rc = rd_kafka_producev(rk,
        RD_KAFKA_V_TOPIC(topic),
        RD_KAFKA_V_PARTITION(part),
        RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
        RD_KAFKA_V_VALUE(payload, plen),
        RD_KAFKA_V_KEY(key, klen),
        RD_KAFKA_V_END);

    /* Serve delivery reports non-blocking */
    rd_kafka_poll(rk, 0);
    CAMLreturn(Val_int(rc));
}

/* flush(rk, timeout_ms) -> int */
CAMLprim value caml_rdk_flush(value v_rk, value v_timeout) {
    CAMLparam2(v_rk, v_timeout);
    rd_kafka_resp_err_t err = rd_kafka_flush(Rk_val(v_rk), Int_val(v_timeout));
    CAMLreturn(Val_int((int)err));
}

/* outq_len(rk) -> int — число сообщений ещё не доставленных */
CAMLprim value caml_rdk_outq_len(value v_rk) {
    CAMLparam1(v_rk);
    CAMLreturn(Val_int(rd_kafka_outq_len(Rk_val(v_rk))));
}

/* ── Transactions (EOS) ──────────────────────────────────────
   librdkafka transaction API возвращает rd_kafka_error_t* (отличается от
   rd_kafka_resp_err_t): NULL = успех, иначе содержит код + флаги
   retriable/abortable. Возвращаем наружу int-код ошибки (0 = успех),
   обязательно освобождая error-объект (rd_kafka_error_destroy), иначе
   утечка. transactional.id задаётся в конфиге producer'а (producer_new).
   Порядок жизненного цикла: init_transactions (один раз) →
   { begin_transaction → produce* → commit_transaction | abort_transaction }*. */

/* init_transactions(rk, timeout_ms) -> int (0 = ok) */
CAMLprim value caml_rdk_init_transactions(value v_rk, value v_timeout) {
    CAMLparam2(v_rk, v_timeout);
    rd_kafka_error_t *err =
        rd_kafka_init_transactions(Rk_val(v_rk), Int_val(v_timeout));
    int code = 0;
    if (err) { code = (int)rd_kafka_error_code(err); rd_kafka_error_destroy(err); }
    CAMLreturn(Val_int(code));
}

/* begin_transaction(rk) -> int (0 = ok) */
CAMLprim value caml_rdk_begin_transaction(value v_rk) {
    CAMLparam1(v_rk);
    rd_kafka_error_t *err = rd_kafka_begin_transaction(Rk_val(v_rk));
    int code = 0;
    if (err) { code = (int)rd_kafka_error_code(err); rd_kafka_error_destroy(err); }
    CAMLreturn(Val_int(code));
}

/* commit_transaction(rk, timeout_ms) -> int (0 = ok) */
CAMLprim value caml_rdk_commit_transaction(value v_rk, value v_timeout) {
    CAMLparam2(v_rk, v_timeout);
    rd_kafka_error_t *err =
        rd_kafka_commit_transaction(Rk_val(v_rk), Int_val(v_timeout));
    int code = 0;
    if (err) { code = (int)rd_kafka_error_code(err); rd_kafka_error_destroy(err); }
    CAMLreturn(Val_int(code));
}

/* abort_transaction(rk, timeout_ms) -> int (0 = ok) */
CAMLprim value caml_rdk_abort_transaction(value v_rk, value v_timeout) {
    CAMLparam2(v_rk, v_timeout);
    rd_kafka_error_t *err =
        rd_kafka_abort_transaction(Rk_val(v_rk), Int_val(v_timeout));
    int code = 0;
    if (err) { code = (int)rd_kafka_error_code(err); rd_kafka_error_destroy(err); }
    CAMLreturn(Val_int(code));
}

/* send_offsets_to_transaction(producer_rk, consumer_rk, topic, partition,
                               offset, timeout_ms) -> int (0 = ok)

   Строжайший EOS read-process-write: коммитит consumer-offset ВНУТРИ
   открытой producer-транзакции, так что «прочитано → записано → offset
   сдвинут» атомарны. При abort'е транзакции offset тоже откатывается, и
   после сбоя данные перечитываются с той же позиции — ни потерь, ни
   дублей сквозь брокер.

   offset здесь — это offset СЛЕДУЮЩЕГО сообщения для чтения (Kafka-
   семантика commit: «уже обработано всё до offset»), т.е. last_processed
   + 1. Group metadata берётся от consumer-handle (он должен быть в той
   же группе). */
CAMLprim value caml_rdk_send_offsets(value *argv, int argn) {
    CAMLparam0();
    (void)argn;
    value v_prk     = argv[0];
    value v_crk     = argv[1];
    value v_topic   = argv[2];
    value v_part    = argv[3];
    value v_offset  = argv[4];
    value v_timeout = argv[5];

    rd_kafka_t *prk = Rk_val(v_prk);   /* транзакционный producer */
    rd_kafka_t *crk = Rk_val(v_crk);   /* consumer (для group metadata) */

    rd_kafka_topic_partition_list_t *offsets =
        rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_t *tp =
        rd_kafka_topic_partition_list_add(offsets,
            String_val(v_topic), Int_val(v_part));
    tp->offset = Int64_val(v_offset);

    rd_kafka_consumer_group_metadata_t *cgmd =
        rd_kafka_consumer_group_metadata(crk);

    rd_kafka_error_t *err = rd_kafka_send_offsets_to_transaction(
        prk, offsets, cgmd, Int_val(v_timeout));

    int code = 0;
    if (err) { code = (int)rd_kafka_error_code(err); rd_kafka_error_destroy(err); }

    if (cgmd) rd_kafka_consumer_group_metadata_destroy(cgmd);
    rd_kafka_topic_partition_list_destroy(offsets);
    CAMLreturn(Val_int(code));
}

/* bytecode-обёртка для >5 аргументов (OCaml требует пару native/byte) */
CAMLprim value caml_rdk_send_offsets_byte(value *argv, int argn) {
    return caml_rdk_send_offsets(argv, argn);
}

/* ── Consumer ────────────────────────────────────────────── */

/* create_consumer(conf_pairs) -> rk */
CAMLprim value caml_rdk_consumer_new(value v_pairs) {
    CAMLparam1(v_pairs);
    char errbuf[ERR_BUF_LEN];
    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    if (apply_conf(conf, v_pairs, errbuf) < 0) {
        rd_kafka_conf_destroy(conf);
        caml_failwith(errbuf);
    }
    rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errbuf, ERR_BUF_LEN);
    if (!rk) caml_failwith(errbuf);
    /* Forward rebalance callbacks to main queue */
    rd_kafka_poll_set_consumer(rk);
    CAMLreturn(alloc_rk(rk));
}

/* subscribe(rk, topics_list) -> int */
CAMLprim value caml_rdk_subscribe(value v_rk, value v_topics) {
    CAMLparam2(v_rk, v_topics);
    rd_kafka_topic_partition_list_t *tpl =
        rd_kafka_topic_partition_list_new(4);
    value topics = v_topics;
    while (topics != Val_emptylist) {
        rd_kafka_topic_partition_list_add(tpl, String_val(Field(topics, 0)),
                                          RD_KAFKA_PARTITION_UA);
        topics = Field(topics, 1);
    }
    rd_kafka_resp_err_t err = rd_kafka_subscribe(Rk_val(v_rk), tpl);
    rd_kafka_topic_partition_list_destroy(tpl);
    CAMLreturn(Val_int((int)err));
}

/* poll(rk, timeout_ms) ->
     None              if timeout / no message
     Some (topic, partition, offset, key_opt, payload) */
CAMLprim value caml_rdk_consumer_poll(value v_rk, value v_timeout) {
    CAMLparam2(v_rk, v_timeout);
    CAMLlocal5(result, tuple, key_opt, k, payload);

    rd_kafka_message_t *msg =
        rd_kafka_consumer_poll(Rk_val(v_rk), Int_val(v_timeout));

    if (!msg) CAMLreturn(Val_int(0));   /* None */

    if (msg->err) {
        if (msg->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
            rd_kafka_message_destroy(msg);
            CAMLreturn(Val_int(0));     /* None — end of partition */
        }
        const char *errstr = rd_kafka_message_errstr(msg);
        rd_kafka_message_destroy(msg);
        caml_failwith(errstr);
    }

    /* Build Some(topic, partition, offset, key_opt, payload) */
    tuple = caml_alloc_tuple(5);
    Store_field(tuple, 0,
        caml_copy_string(rd_kafka_topic_name(msg->rkt)));
    Store_field(tuple, 1, Val_int((int)msg->partition));
    Store_field(tuple, 2, caml_copy_int64(msg->offset));

    if (msg->key && msg->key_len > 0) {
        k = caml_alloc_string(msg->key_len);
        memcpy(Bytes_val(k), msg->key, msg->key_len);
        key_opt = caml_alloc_tuple(1);
        Store_field(key_opt, 0, k);
    } else {
        key_opt = Val_int(0);           /* None */
    }
    Store_field(tuple, 3, key_opt);

    payload = caml_alloc_string(msg->len);
    memcpy(Bytes_val(payload), msg->payload, msg->len);
    Store_field(tuple, 4, payload);

    rd_kafka_message_destroy(msg);

    result = caml_alloc_tuple(1);
    Store_field(result, 0, tuple);
    CAMLreturn(result);                 /* Some(...) */
}

/* commit_offsets(rk, topic, partition, offset) -> int
   Коммит конкретного offset (для exactly-once) */
CAMLprim value caml_rdk_commit_offset(value v_rk, value v_topic,
                                       value v_part, value v_offset) {
    CAMLparam4(v_rk, v_topic, v_part, v_offset);
    rd_kafka_topic_partition_list_t *offsets =
        rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_t *tp =
        rd_kafka_topic_partition_list_add(offsets,
            String_val(v_topic), (int32_t)Int_val(v_part));
    /* +1: commit следующего offset для чтения *)  */
    tp->offset = Int64_val(v_offset) + 1;
    rd_kafka_resp_err_t err =
        rd_kafka_commit(Rk_val(v_rk), offsets, 0 /* sync */);
    rd_kafka_topic_partition_list_destroy(offsets);
    CAMLreturn(Val_int((int)err));
}

/* seek(rk, topic, partition, offset) -> int
   Перемотать consumer на конкретный offset (для checkpoint recovery) */
CAMLprim value caml_rdk_seek(value v_rk, value v_topic,
                              value v_part, value v_offset) {
    CAMLparam4(v_rk, v_topic, v_part, v_offset);
    rd_kafka_topic_t *rkt =
        rd_kafka_topic_new(Rk_val(v_rk), String_val(v_topic), NULL);
    rd_kafka_resp_err_t err = rd_kafka_seek(rkt,
        (int32_t)Int_val(v_part),
        Int64_val(v_offset),
        5000 /* timeout ms */);
    rd_kafka_topic_destroy(rkt);
    CAMLreturn(Val_int((int)err));
}

/* consumer_close(rk) -> unit */
CAMLprim value caml_rdk_consumer_close(value v_rk) {
    CAMLparam1(v_rk);
    rd_kafka_consumer_close(Rk_val(v_rk));
    CAMLreturn(Val_unit);
}

/* err2str(err_int) -> string */
CAMLprim value caml_rdk_err2str(value v_err) {
    CAMLparam1(v_err);
    CAMLreturn(caml_copy_string(
        rd_kafka_err2str((rd_kafka_resp_err_t)Int_val(v_err))));
}

/* metadata: list of (topic, num_partitions) for subscribed topics */
CAMLprim value caml_rdk_topic_partition_count(value v_rk, value v_topic) {
    CAMLparam2(v_rk, v_topic);
    const rd_kafka_metadata_t *meta;
    rd_kafka_resp_err_t err = rd_kafka_metadata(
        Rk_val(v_rk), 0 /* only_for_rk=false */,
        NULL, &meta, 5000);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
        CAMLreturn(Val_int(1));  /* default to 1 on error */
    const char *topic = String_val(v_topic);
    int count = 1;
    for (int i = 0; i < meta->topic_cnt; i++) {
        if (strcmp(meta->topics[i].topic, topic) == 0) {
            count = meta->topics[i].partition_cnt;
            break;
        }
    }
    rd_kafka_metadata_destroy(meta);
    CAMLreturn(Val_int(count));
}
