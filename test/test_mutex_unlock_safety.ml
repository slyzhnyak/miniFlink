(** Тест: исключение в пользовательском callback'е (persist/publish)
    НЕ оставляет mutex заблокированным (без deadlock).

    Регрессия для fix/mutex-unlock-safety: раньше commit/ts_commit
    держали mutex при вызове user-кода без try/finally, так что
    исключение в колбэке вешало весь checkpoint-механизм. *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let () =
  Printf.printf "Test: mutex unlock safety on callback exception\n%!";

  (* ── 1. commit с бросающим persist: mutex освобождается ── *)
  Printf.printf "\n-- 1. commit: throwing persist callback doesn't deadlock\n";
  let throw_count = ref 0 in
  let store = Checkpoint_parallel.make_store
    ~persist:(fun _cp ->
      incr throw_count;
      failwith "simulated persist failure") () in
  let cp : Checkpoint_parallel.checkpoint = {
    cp_epoch = 1;
    cp_offset = 0;
    cp_snapshots = [||];
  } in
  (* Первый commit бросит из-за persist *)
  (try Checkpoint_parallel.commit store cp; fail "expected exception"
   with Failure _ -> pass "first commit raised (as expected)");

  (* КЛЮЧЕВАЯ ПРОВЕРКА: mutex освобождён, второй вызов не висит.
     Если бы mutex остался заблокированным — этот вызов завис бы
     навечно (тест бы повис по таймауту). *)
  (try Checkpoint_parallel.commit store cp; fail "expected exception"
   with Failure _ -> pass "second commit also raised (mutex was released)");

  check "persist called twice (no deadlock)" (!throw_count = 2);

  (* Операции чтения тоже не висят после исключения *)
  let n = Checkpoint_parallel.checkpoint_count store in
  Printf.printf "  checkpoint_count = %d\n" n;
  check "checkpoint_count accessible after exception" (n >= 0);

  (* ── 2. latest_checkpoint доступен после исключения ── *)
  Printf.printf "\n-- 2. reads work after persist exception\n";
  let latest = Checkpoint_parallel.latest_checkpoint store in
  check "latest_checkpoint accessible"
    (match latest with Some cp -> cp.cp_epoch = 1 | None -> false);

  Printf.printf "\nMutex unlock safety verified.\n"
