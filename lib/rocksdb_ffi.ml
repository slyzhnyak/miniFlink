(* ============================================================
   Rocksdb_ffi.ml — низкоуровневые привязки к RocksDB C API

   Тонкая обёртка над rocksdb_stubs.c. Все функции могут бросить
   Failure с текстом ошибки от RocksDB.
   ============================================================ *)

type db

external open_db : string -> db        = "mf_rocksdb_open"
external close_db : db -> unit          = "mf_rocksdb_close"
external put : db -> string -> bytes -> unit = "mf_rocksdb_put"
external get : db -> string -> bytes option  = "mf_rocksdb_get"
external delete : db -> string -> unit  = "mf_rocksdb_delete"
