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

  char *err = NULL;
  rocksdb_options_t *opts = rocksdb_options_create();
  rocksdb_options_set_create_if_missing(opts, 1);

  rocksdb_t *db = rocksdb_open(opts, String_val(path), &err);
  rocksdb_options_destroy(opts);

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

  char *err = NULL;
  rocksdb_writeoptions_t *wopts = rocksdb_writeoptions_create();
  rocksdb_put(db, wopts,
              String_val(key), caml_string_length(key),
              Bytes_val(data), caml_string_length(data),
              &err);
  rocksdb_writeoptions_destroy(wopts);

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

  char *err = NULL;
  size_t vlen = 0;
  rocksdb_readoptions_t *ropts = rocksdb_readoptions_create();
  char *val = rocksdb_get(db, ropts,
                          String_val(key), caml_string_length(key),
                          &vlen, &err);
  rocksdb_readoptions_destroy(ropts);

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

  /* Some bytes — копируем немедленно, затем free C-буфер */
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

  char *err = NULL;
  rocksdb_writeoptions_t *wopts = rocksdb_writeoptions_create();
  rocksdb_delete(db, wopts,
                 String_val(key), caml_string_length(key), &err);
  rocksdb_writeoptions_destroy(wopts);

  if (err != NULL) {
    char msg[512];
    snprintf(msg, sizeof(msg), "rocksdb_delete: %s", err);
    free(err);
    caml_failwith(msg);
  }
  CAMLreturn(Val_unit);
}
