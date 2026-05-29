(* ============================================================
   State_backend_rocksdb.ml — персистентный backend на RocksDB

   Реальный RocksDB через C FFI (rocksdb_ffi → rocksdb_stubs.c).
   Переживает рестарт процесса.

   keys/snapshot/restore: ведём in-memory set ключей рядом с db
   (ключей операторного стейта обычно немного). При open читать
   все ключи из RocksDB не нужно для нашего use-case — стейт
   восстанавливается через restore из checkpoint.
   ============================================================ *)

type t = {
  db   : Rocksdb_ffi.db;
  path : string;
  mutable known : (string, unit) Hashtbl.t;  (* индекс ключей *)
}

let default_dir = "/tmp/miniflink_rocksdb"

let create_at path =
  (try Unix.mkdir (Filename.dirname path) 0o755
   with Unix.Unix_error (Unix.EEXIST,_,_) -> () | _ -> ());
  { db = Rocksdb_ffi.open_db path; path; known = Hashtbl.create 64 }

let create () = create_at default_dir

let get t k = Rocksdb_ffi.get t.db k

let set t k v =
  Rocksdb_ffi.put t.db k v;
  Hashtbl.replace t.known k ()

let delete t k =
  Rocksdb_ffi.delete t.db k;
  Hashtbl.remove t.known k

let keys t = Hashtbl.fold (fun k () a -> k :: a) t.known []

let size t = Hashtbl.length t.known

let close t = Rocksdb_ffi.close_db t.db

(* Снапшот: сериализуем все (ключ,значение) пары.
   Для персистентного backend снапшот — это логический дамп,
   сам RocksDB уже на диске. *)
let snapshot t =
  let pairs = Hashtbl.fold (fun k () a ->
    match get t k with Some v -> (k,v)::a | None -> a) t.known [] in
  Marshal.to_bytes pairs []

let restore t b =
  let pairs : (string * bytes) list = Marshal.from_bytes b 0 in
  List.iter (fun (k,v) -> set t k v) pairs
