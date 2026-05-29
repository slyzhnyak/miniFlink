(* Тест roadmap 2.2: настоящий RocksDB backend через C FFI *)

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let uniq_path () =
  Printf.sprintf "/tmp/miniflink_rocksdb_test_%d_%d"
    (Unix.getpid ()) (Random.int 1000000)

(* ── 1. put/get/delete базовые операции ──────────────────── *)
let test_basic () =
  Printf.printf "\n-- Basic put/get/delete\n";
  let path = uniq_path () in
  let t = State_backend_rocksdb.create_at path in
  State_backend_rocksdb.set t "k1" (Bytes.of_string "hello");
  State_backend_rocksdb.set t "k2" (Bytes.of_string "world");
  check "get k1" (State_backend_rocksdb.get t "k1" = Some (Bytes.of_string "hello"));
  check "get k2" (State_backend_rocksdb.get t "k2" = Some (Bytes.of_string "world"));
  check "get missing = None" (State_backend_rocksdb.get t "nope" = None);
  State_backend_rocksdb.delete t "k1";
  check "deleted k1 = None" (State_backend_rocksdb.get t "k1" = None);
  check "k2 still present" (State_backend_rocksdb.get t "k2" <> None);
  State_backend_rocksdb.close t

(* ── 2. ПЕРСИСТЕНТНОСТЬ: данные переживают close/reopen ───── *)
let test_persistence () =
  Printf.printf "\n-- Persistence across close + reopen (THE point of RocksDB)\n";
  let path = uniq_path () in
  (* Сессия 1: пишем, закрываем *)
  let t1 = State_backend_rocksdb.create_at path in
  State_backend_rocksdb.set t1 "device_A" (Bytes.of_string "count=42");
  State_backend_rocksdb.set t1 "device_B" (Bytes.of_string "count=17");
  State_backend_rocksdb.close t1;

  (* Сессия 2: переоткрываем тот же путь — данные должны быть на месте *)
  let t2 = State_backend_rocksdb.create_at path in
  check "device_A survived restart"
    (State_backend_rocksdb.get t2 "device_A" = Some (Bytes.of_string "count=42"));
  check "device_B survived restart"
    (State_backend_rocksdb.get t2 "device_B" = Some (Bytes.of_string "count=17"));
  State_backend_rocksdb.close t2

(* ── 3. snapshot/restore ─────────────────────────────────── *)
let test_snapshot () =
  Printf.printf "\n-- snapshot / restore\n";
  let path1 = uniq_path () in
  let path2 = uniq_path () in
  let t1 = State_backend_rocksdb.create_at path1 in
  State_backend_rocksdb.set t1 "x" (Bytes.of_string "1");
  State_backend_rocksdb.set t1 "y" (Bytes.of_string "2");
  let snap = State_backend_rocksdb.snapshot t1 in
  State_backend_rocksdb.close t1;

  (* Восстанавливаем в ДРУГОЙ инстанс *)
  let t2 = State_backend_rocksdb.create_at path2 in
  State_backend_rocksdb.restore t2 snap;
  check "x restored" (State_backend_rocksdb.get t2 "x" = Some (Bytes.of_string "1"));
  check "y restored" (State_backend_rocksdb.get t2 "y" = Some (Bytes.of_string "2"));
  State_backend_rocksdb.close t2

(* ── 4. бинарные значения (не только текст) ──────────────── *)
let test_binary () =
  Printf.printf "\n-- Binary values (Marshal round-trip)\n";
  let path = uniq_path () in
  let t = State_backend_rocksdb.create_at path in
  let data = Marshal.to_bytes [1;2;3;42;1000] [] in
  State_backend_rocksdb.set t "list" data;
  (match State_backend_rocksdb.get t "list" with
   | Some b ->
     let restored : int list = Marshal.from_bytes b 0 in
     check "binary round-trip" (restored = [1;2;3;42;1000])
   | None -> fail "binary value lost");
  State_backend_rocksdb.close t

let () =
  Random.self_init ();
  Printf.printf "==========================================\n";
  Printf.printf "  RocksDB backend via C FFI (roadmap 2.2)\n";
  Printf.printf "==========================================\n";
  test_basic ();
  test_persistence ();
  test_snapshot ();
  test_binary ();
  Printf.printf "\nAll RocksDB tests passed.\n"
