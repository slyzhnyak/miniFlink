open Miniflink
(* Differential testing бэкендов состояния: memory и rocksdb должны
   давать ИДЕНТИЧНЫЙ результат на одной последовательности операций.
   Это ловит расхождения в семантике бэкендов (то что юнит-тесты по
   отдельности не поймают — только сравнение бок о бок).

   noop НЕ участвует в differential по значению: он намеренно без
   состояния (всегда None) — проверяется отдельно что он консистентно
   пустой. *)

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

(* Последовательность операций над backend; возвращает «наблюдаемую
   историю» — список результатов всех get, плюс финальные keys/size. *)
type op = Set of string * int | Get of string | Delete of string

let gen_ops n =
  Random.self_init ();
  List.init n (fun _ ->
    match Random.int 5 with
    | 0 -> Delete (Printf.sprintf "k%d" (Random.int 8))
    | 1 -> Get (Printf.sprintf "k%d" (Random.int 8))
    | _ -> Set (Printf.sprintf "k%d" (Random.int 8), Random.int 1000))

(* Прогнать ops через backend с заданными get/set/delete/keys/size,
   собрать наблюдаемую историю. *)
let observe ~get ~set ~delete ~keys ~size ops =
  let history = List.map (fun op ->
    match op with
    | Set (k, v) -> set k (Bytes.of_string (string_of_int v)); `Set
    | Delete k -> delete k; `Del
    | Get k ->
      (match get k with
       | Some b -> `Got (Bytes.to_string b)
       | None -> `None)) ops in
  (history, List.sort compare (keys ()), size ())

(* ── differential: memory vs rocksdb ──────────────────────── *)
let test_memory_vs_rocksdb () =
  Printf.printf "\n-- memory and rocksdb backends agree on identical ops\n";
  let ops = gen_ops 500 in

  let mem = State_backend_memory.create () in
  let mem_result = observe
    ~get:(State_backend_memory.get mem)
    ~set:(State_backend_memory.set mem)
    ~delete:(State_backend_memory.delete mem)
    ~keys:(fun () -> State_backend_memory.keys mem)
    ~size:(fun () -> State_backend_memory.size mem)
    ops in

  let dir = Printf.sprintf "/tmp/miniflink_diff_%d" (Random.int 1000000) in
  let rocks = State_backend_rocksdb.create_at dir in
  let rocks_result = observe
    ~get:(State_backend_rocksdb.get rocks)
    ~set:(State_backend_rocksdb.set rocks)
    ~delete:(State_backend_rocksdb.delete rocks)
    ~keys:(fun () -> State_backend_rocksdb.keys rocks)
    ~size:(fun () -> State_backend_rocksdb.size rocks)
    ops in

  let (mh, mk, ms) = mem_result and (rh, rk, rs) = rocks_result in
  check "get histories identical" (mh = rh);
  check "final key sets identical" (mk = rk);
  check "final sizes identical" (ms = rs)

(* ── differential: snapshot/restore консистентны между бэкендами ── *)
let test_snapshot_restore_diff () =
  Printf.printf "\n-- snapshot/restore round-trips agree\n";
  let ops = gen_ops 200 in
  let mem = State_backend_memory.create () in
  List.iter (function
    | Set (k, v) -> State_backend_memory.set mem k (Bytes.of_string (string_of_int v))
    | Delete k -> State_backend_memory.delete mem k
    | Get _ -> ()) ops;
  (* snapshot → restore в новый backend → состояние совпадает *)
  let snap = State_backend_memory.snapshot mem in
  let mem2 = State_backend_memory.create () in
  State_backend_memory.restore mem2 snap;
  let keys1 = List.sort compare (State_backend_memory.keys mem) in
  let keys2 = List.sort compare (State_backend_memory.keys mem2) in
  check "memory snapshot/restore preserves keys" (keys1 = keys2);
  check "memory snapshot/restore preserves values"
    (List.for_all (fun k ->
       State_backend_memory.get mem k = State_backend_memory.get mem2 k) keys1)

(* ── noop консистентно пустой (отдельная семантика) ────────── *)
let test_noop_consistent () =
  Printf.printf "\n-- noop backend is consistently stateless\n";
  let n = State_backend_noop.create () in
  State_backend_noop.set n "k" (Bytes.of_string "v");
  check "noop get always None" (State_backend_noop.get n "k" = None);
  check "noop size always 0" (State_backend_noop.size n = 0);
  check "noop keys always empty" (State_backend_noop.keys n = [])

(* N2: restore должен ЗАМЕНЯТЬ состояние снапшотом, не объединять.
   Ключи записанные после снапшота должны исчезнуть после restore.
   memory и rocksdb обязаны вести себя ОДИНАКОВО. *)
let test_restore_replaces_state () =
  Printf.printf "\n-- restore replaces state (drops post-snapshot keys), both backends\n";
  let check_backend name ~create ~set ~delete:_ ~keys ~snapshot ~restore =
    let b = create () in
    set b "a" (Bytes.of_string "1");
    set b "b" (Bytes.of_string "2");
    let snap = snapshot b in           (* снапшот: {a, b} *)
    set b "c" (Bytes.of_string "3");   (* добавили c ПОСЛЕ снапшота *)
    restore b snap;                    (* должно вернуть ровно {a, b} *)
    let ks = List.sort compare (keys b) in
    check (Printf.sprintf "%s: restore drops post-snapshot key c" name)
      (ks = ["a"; "b"]) in
  check_backend "memory"
    ~create:State_backend_memory.create
    ~set:State_backend_memory.set
    ~delete:State_backend_memory.delete
    ~keys:State_backend_memory.keys
    ~snapshot:State_backend_memory.snapshot
    ~restore:State_backend_memory.restore;
  let dir = Printf.sprintf "/tmp/miniflink_restore_%d" (Random.int 1000000) in
  let rocks = State_backend_rocksdb.create_at dir in
  let snap_r =
    State_backend_rocksdb.set rocks "a" (Bytes.of_string "1");
    State_backend_rocksdb.set rocks "b" (Bytes.of_string "2");
    let s = State_backend_rocksdb.snapshot rocks in
    State_backend_rocksdb.set rocks "c" (Bytes.of_string "3");
    State_backend_rocksdb.restore rocks s;
    List.sort compare (State_backend_rocksdb.keys rocks) in
  check "rocksdb: restore drops post-snapshot key c" (snap_r = ["a"; "b"])

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Differential backend testing\n";
  Printf.printf "==========================================\n";
  test_memory_vs_rocksdb ();
  test_snapshot_restore_diff ();
  test_restore_replaces_state ();
  test_noop_consistent ();
  Printf.printf "\nDifferential backend tests passed.\n"
