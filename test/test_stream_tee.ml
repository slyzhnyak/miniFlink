(** Тесты Stream.tee и Stream.split.

    Проверяем что:
    - tee/split дают независимые копии исходного потока
    - оба видят все события в том же порядке
    - можно читать неравномерно (один обогнал — отстающий догонит)
    - корректно завершаются на исчерпании источника
    - split N даёт N независимых копий *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Stream.tee / split\n";
  Printf.printf "==========================================\n";

  (* ── 1. tee: обе копии видят те же события ────────────────── *)
  Printf.printf "\n-- 1. tee — both copies see same elements\n";

  let src = Stream.of_list [1; 2; 3; 4; 5] in
  let a, b = Stream.tee src in
  let xs_a = Stream.to_list a in
  let xs_b = Stream.to_list b in
  check (Printf.sprintf "copy A: %s" (String.concat ";" (List.map string_of_int xs_a)))
    (xs_a = [1; 2; 3; 4; 5]);
  check (Printf.sprintf "copy B: %s" (String.concat ";" (List.map string_of_int xs_b)))
    (xs_b = [1; 2; 3; 4; 5]);

  (* ── 2. tee: неравномерное чтение ──────────────────────────── *)
  Printf.printf "\n-- 2. tee — uneven reading\n";

  let src = Stream.of_list [10; 20; 30; 40; 50] in
  let a, b = Stream.tee src in
  (* A читает 3 элемента вперёд *)
  let a1 = a () in let a2 = a () in let a3 = a () in
  (* B пока стоит, потом читает все 5 *)
  let xs_b = Stream.to_list b in
  (* A дочитывает оставшиеся 2 *)
  let a4 = a () in let a5 = a () in let a6 = a () in
  check "A first 3: 10,20,30"
    (a1 = Some 10 && a2 = Some 20 && a3 = Some 30);
  check (Printf.sprintf "B all 5: %s" (String.concat ";" (List.map string_of_int xs_b)))
    (xs_b = [10; 20; 30; 40; 50]);
  check "A remaining 2 + None"
    (a4 = Some 40 && a5 = Some 50 && a6 = None);

  (* ── 3. tee: пустой источник ───────────────────────────────── *)
  Printf.printf "\n-- 3. tee on empty source\n";

  let a, b = Stream.tee Stream.empty in
  check "A empty" (a () = None);
  check "B empty" (b () = None);

  (* ── 4. split N — N копий ──────────────────────────────────── *)
  Printf.printf "\n-- 4. split N\n";

  let src = Stream.of_list [1; 2; 3] in
  let copies = Stream.split 4 src in
  check "split 4 returns 4 streams" (List.length copies = 4);
  List.iteri (fun i c ->
    let xs = Stream.to_list c in
    check (Printf.sprintf "copy %d: %s" i (String.concat ";" (List.map string_of_int xs)))
      (xs = [1; 2; 3])
  ) copies;

  (* ── 5. split 1 — pass-through ─────────────────────────────── *)
  Printf.printf "\n-- 5. split 1\n";

  let src = Stream.of_list [42] in
  (match Stream.split 1 src with
   | [s] ->
     let xs = Stream.to_list s in
     check "split 1 = [s]" (xs = [42])
   | _ -> fail "split 1 should return single-element list");

  (* ── 6. split 0 — invalid ──────────────────────────────────── *)
  Printf.printf "\n-- 6. split 0 raises\n";

  (try
    let _ = Stream.split 0 Stream.empty in
    fail "split 0 should raise"
   with Invalid_argument _ -> pass "Invalid_argument on split 0");

  (* ── 7. split: неравномерное чтение ────────────────────────── *)
  Printf.printf "\n-- 7. split — uneven reading\n";

  let src = Stream.of_list [100; 200; 300] in
  (match Stream.split 3 src with
   | [s1; s2; s3] ->
     (* s1 читает всё, s2 и s3 пока ждут *)
     let xs1 = Stream.to_list s1 in
     let xs2 = Stream.to_list s2 in
     let xs3 = Stream.to_list s3 in
     check "s1" (xs1 = [100; 200; 300]);
     check "s2 catches up" (xs2 = [100; 200; 300]);
     check "s3 catches up" (xs3 = [100; 200; 300])
   | _ -> fail "expected 3 streams");

  Printf.printf "\nAll Stream.tee/split tests passed.\n"
