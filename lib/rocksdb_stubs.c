/* ============================================================
   rocksdb_stubs.c — C FFI к RocksDB C API

   Минимальный binding: open/close/put/get/delete.
   Все байты копируются между OCaml и C немедленно —
   не держим OCaml-указатели на стороне C (GC safety).
   ============================================================ */

#include <string.h>
#include <stdlib.h>
#include <rocksdb/c.h>

#include <caml/mlvalues.h>
#include <caml/alloc.h>
#include <caml/memory.h>
#include <caml/fail.h>
#include <caml/custom.h>
#include <caml/threads.h>   /* caml_enter/leave_blocking_section */

/* Обёртка для rocksdb* в OCaml custom block */
#define Rocksdb_val(v) (*((rocksdb_t **) Data_custom_val(v)))

static void finalize_rocksdb(value v) {
  rocksdb_t *db = Rocksdb_val(v);
  if (db != NULL) {
    rocksdb_close(db);
    Rocksdb_val(v) = NULL;
  }
}

static struct custom_operations rocksdb_ops = {
  "miniflink.rocksdb",
  finalize_rocksdb,
  custom_compare_default,
  custom_hash_default,
  custom_serialize_default,
  custom_deserialize_default,
  custom_compare_ext_default,
  custom_fixed_length_default
};

/* open : string -> t */
CAMLprim value mf_rocksdb_open(value path) {
  CAMLparam1(path);
  CAMLlocal1(result);

  /* копируем путь до blocking section */
  size_t plen = caml_string_length(path);
  char *pbuf = malloc(plen + 1);
  if (!pbuf) caml_failwith("rocksdb_open: oom");
  memcpy(pbuf, String_val(path), plen);
  pbuf[plen] = '\0';

  char *err = NULL;
  rocksdb_options_t *opts = rocksdb_options_create();
  rocksdb_options_set_create_if_missing(opts, 1);

  /* открытие БД — блокирующий дисковый I/O, отпускаем runtime */
  caml_enter_blocking_section();
  rocksdb_t *db = rocksdb_open(opts, pbuf, &err);
  rocksdb_options_destroy(opts);
  caml_leave_blocking_section();

  free(pbuf);

  if (err != NULL) {
    char msg[512];
    snprintf(msg, sizeof(msg), "rocksdb_open: %s", err);
    free(err);
    caml_failwith(msg);
  }

  result = caml_alloc_custom(&rocksdb_ops, sizeof(rocksdb_t *), 0, 1);
  Rocksdb_val(result) = db;
  CAMLreturn(result);
}

/* close : t -> unit */
CAMLprim value mf_rocksdb_close(value v) {
  CAMLparam1(v);
  rocksdb_t *db = Rocksdb_val(v);
  if (db != NULL) {
    rocksdb_close(db);
    Rocksdb_val(v) = NULL;
  }
  CAMLreturn(Val_unit);
}

/* put : t -> string -> bytes -> unit */
CAMLprim value mf_rocksdb_put(value v, value key, value data) {
  CAMLparam3(v, key, data);
  rocksdb_t *db = Rocksdb_val(v);
  if (db == NULL) caml_failwith("rocksdb_put: db closed");

  /* Скопировать key/data в C-память ДО blocking section: внутри секции
     другие домены работают и GC может двигать OCaml-кучу — трогать
     String_val/Bytes_val там нельзя. */
  size_t klen = caml_string_length(key);
  size_t dlen = caml_string_length(data);
  char *kbuf = malloc(klen);
  char *dbuf = malloc(dlen);
  if ((klen && !kbuf) || (dlen && !dbuf)) {
    free(kbuf); free(dbuf); caml_failwith("rocksdb_put: oom");
  }
  memcpy(kbuf, String_val(key), klen);
  memcpy(dbuf, Bytes_val(data), dlen);

  char *err = NULL;
  rocksdb_writeoptions_t *wopts = rocksdb_writeoptions_create();
  /* Отпустить runtime на время блокирующего дискового I/O — другие
     домены продолжают работать (иначе на OCaml 5 встал бы весь процесс). */
  caml_enter_blocking_section();
  rocksdb_put(db, wopts, kbuf, klen, dbuf, dlen, &err);
  rocksdb_writeoptions_destroy(wopts);
  caml_leave_blocking_section();

  free(kbuf); free(dbuf);

  if (err != NULL) {
    char msg[512];
    snprintf(msg, sizeof(msg), "rocksdb_put: %s", err);
    free(err);
    caml_failwith(msg);
  }
  CAMLreturn(Val_unit);
}

/* get : t -> string -> bytes option  (None если ключа нет) */
CAMLprim value mf_rocksdb_get(value v, value key) {
  CAMLparam2(v, key);
  CAMLlocal2(result, data);

  rocksdb_t *db = Rocksdb_val(v);
  if (db == NULL) caml_failwith("rocksdb_get: db closed");

  /* копируем ключ до blocking section (см. mf_rocksdb_put) */
  size_t klen = caml_string_length(key);
  char *kbuf = malloc(klen ? klen : 1);
  if (!kbuf) caml_failwith("rocksdb_get: oom");
  memcpy(kbuf, String_val(key), klen);

  char *err = NULL;
  size_t vlen = 0;
  rocksdb_readoptions_t *ropts = rocksdb_readoptions_create();
  caml_enter_blocking_section();
  char *val = rocksdb_get(db, ropts, kbuf, klen, &vlen, &err);
  rocksdb_readoptions_destroy(ropts);
  caml_leave_blocking_section();

  free(kbuf);

  if (err != NULL) {
    char msg[512];
    snprintf(msg, sizeof(msg), "rocksdb_get: %s", err);
    free(err);
    caml_failwith(msg);
  }

  if (val == NULL) {
    /* None */
    CAMLreturn(Val_int(0));
  }

  /* Some bytes — аллоцируем OCaml ПОСЛЕ blocking section, копируем,
     затем освобождаем C-буфер */
  data = caml_alloc_string(vlen);
  memcpy(Bytes_val(data), val, vlen);
  free(val);

  result = caml_alloc(1, 0);   /* Some _ */
  Store_field(result, 0, data);
  CAMLreturn(result);
}

/* delete : t -> string -> unit */
CAMLprim value mf_rocksdb_delete(value v, value key) {
  CAMLparam2(v, key);
  rocksdb_t *db = Rocksdb_val(v);
  if (db == NULL) caml_failwith("rocksdb_delete: db closed");

  size_t klen = caml_string_length(key);
  char *kbuf = malloc(klen ? klen : 1);
  if (!kbuf) caml_failwith("rocksdb_delete: oom");
  memcpy(kbuf, String_val(key), klen);

  char *err = NULL;
  rocksdb_writeoptions_t *wopts = rocksdb_writeoptions_create();
  caml_enter_blocking_section();
  rocksdb_delete(db, wopts, kbuf, klen, &err);
  rocksdb_writeoptions_destroy(wopts);
  caml_leave_blocking_section();

  free(kbuf);

  if (err != NULL) {
    char msg[512];
    snprintf(msg, sizeof(msg), "rocksdb_delete: %s", err);
    free(err);
    caml_failwith(msg);
  }
  CAMLreturn(Val_unit);
}
