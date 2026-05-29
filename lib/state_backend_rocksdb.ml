(* ============================================================
   State_backend_rocksdb.ml — заглушка RocksDB

   Интерфейс идентичен Memory.
   Реальная реализация через C FFI подключается здесь.

   Чтобы использовать настоящий RocksDB:
   1. opam install rocksdb (или через apt: librocksdb-dev)
   2. Раскомментировать external привязки
   3. Заменить Hashtbl на вызовы RocksDB C API

   Сейчас: пишет в /tmp/miniflink_state/ через файлы.
   Это не RocksDB но даёт персистентность между рестартами.
   ============================================================ *)
type t = {
  path  : string;
  cache : (string, bytes) Hashtbl.t;
}

let state_dir = "/tmp/miniflink_state"

let key_to_file t k =
  let safe = String.map (function '/' | ':' -> '_' | c -> c) k in
  Filename.concat t.path safe

let create () =
  let path = state_dir in
  (try Unix.mkdir path 0o755 with Unix.Unix_error (Unix.EEXIST,_,_) -> ());
  let t = { path; cache = Hashtbl.create 64 } in
  (* Загружаем существующий стейт при старте *)
  (try
    let d = Unix.opendir path in
    (try while true do
      let f = Unix.readdir d in
      if f <> "." && f <> ".." then begin
        let full = Filename.concat path f in
        let ic = open_in_bin full in
        let n  = in_channel_length ic in
        let b  = Bytes.create n in
        really_input ic b 0 n; close_in ic;
        Hashtbl.replace t.cache f b
      end
    done with End_of_file -> ());
    Unix.closedir d
  with _ -> ());
  t

let get t k = Hashtbl.find_opt t.cache k

let set t k v =
  Hashtbl.replace t.cache k v;
  let f  = key_to_file t k in
  let oc = open_out_bin f in
  output_bytes oc v; close_out oc

let delete t k =
  Hashtbl.remove t.cache k;
  (try Sys.remove (key_to_file t k) with _ -> ())

let keys t     = Hashtbl.fold (fun k _ a -> k :: a) t.cache []
let size t     = Hashtbl.length t.cache

let snapshot t =
  Marshal.to_bytes (Hashtbl.fold (fun k v a -> (k,v)::a) t.cache []) []

let restore t b =
  Hashtbl.clear t.cache;
  let pairs : (string * bytes) list = Marshal.from_bytes b 0 in
  List.iter (fun (k,v) -> set t k v) pairs
