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
   with
   | Unix.Unix_error (Unix.EEXIST, _, _) -> ()   (* уже есть — норма *)
   | Unix.Unix_error (err, _, _) ->
     (* EACCES/ENOSPC/EROFS/... — не глотаем молча: дальше open_db
        упадёт с потерянным контекстом, лучше залогировать причину *)
     Log.warn ~fields:[("dir", Filename.dirname path);
                       ("error", Unix.error_message err)]
       "rocksdb: mkdir failed (continuing to open_db)");
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
  Snapshot_frame.marshal_wrap pairs

let restore t b =
  (* Snapshot_frame проверяет магию/длину/crc ДО Marshal.from_bytes (N-9).
     Декодируем ДО очистки: если вход битый, состояние не теряется. *)
  let pairs : (string * bytes) list = Snapshot_frame.marshal_unwrap b in
  (* очищаем текущее состояние перед загрузкой снапшота — restore
     ЗАМЕНЯЕТ состояние, не объединяет (иначе ключи записанные после
     снапшота остались бы, расходясь с memory-backend и ломая recovery) *)
  Hashtbl.iter (fun k () -> Rocksdb_ffi.delete t.db k) t.known;
  Hashtbl.clear t.known;
  List.iter (fun (k,v) -> set t k v) pairs
