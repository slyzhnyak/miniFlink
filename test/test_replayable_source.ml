(** Тесты Replayable_source. *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let drain stream =
  let n = ref 0 in
  let rec loop () = match stream () with
    | None -> ()
    | Some _ -> incr n; loop ()
  in loop ();
  !n

let collect stream =
  let acc = ref [] in
  let rec loop () = match stream () with
    | None -> ()
    | Some v -> acc := v :: !acc; loop ()
  in loop ();
  List.rev !acc

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Replayable_source\n";
  Printf.printf "==========================================\n";

  (* ── 1. Базовое чтение с offset=0 ──────────────────────────── *)
  Printf.printf "\n-- 1. Read from offset=0 (default)\n";

  let src = Replayable_source.of_list [1; 2; 3; 4; 5] in
  let stream, get_offset = Replayable_source.read_from src in
  let xs = collect stream in
  check (Printf.sprintf "read all 5: %s"
           (String.concat ";" (List.map string_of_int xs)))
    (xs = [1; 2; 3; 4; 5]);
  check "offset=5 after reading all" (get_offset () = 5);

  (* ── 2. Skip первых N — read_from offset=3 ─────────────────── *)
  Printf.printf "\n-- 2. Read from offset=3\n";

  let src = Replayable_source.of_list [1; 2; 3; 4; 5] in
  let stream, get_offset = Replayable_source.read_from ~offset:3 src in
  let xs = collect stream in
  check (Printf.sprintf "skip 3 leaves: %s"
           (String.concat ";" (List.map string_of_int xs)))
    (xs = [4; 5]);
  check "offset=5 after reading remainder" (get_offset () = 5);

  (* ── 3. read_from offset=length — пустой stream ─────────────── *)
  Printf.printf "\n-- 3. Read from end (offset=length)\n";

  let src = Replayable_source.of_list [10; 20; 30] in
  let stream, get_offset = Replayable_source.read_from ~offset:3 src in
  check "empty stream from end" (drain stream = 0);
  check "offset still 3" (get_offset () = 3);

  (* ── 4. Невалидный offset ──────────────────────────────────── *)
  Printf.printf "\n-- 4. Invalid offsets raise\n";

  let src = Replayable_source.of_list [1; 2] in
  (try
     let _ = Replayable_source.read_from ~offset:(-1) src in
     fail "negative offset should raise"
   with Invalid_argument _ -> pass "negative offset → Invalid_argument");
  (try
     let _ = Replayable_source.read_from ~offset:99 src in
     fail "out-of-bounds offset should raise"
   with Invalid_argument _ -> pass "out-of-bounds → Invalid_argument");

  (* ── 5. Two independent reads — каждое со своим offset'ом ──── *)
  Printf.printf "\n-- 5. Multiple independent reads\n";

  let src = Replayable_source.of_list [100; 200; 300] in
  let s1, off1 = Replayable_source.read_from src in
  let s2, off2 = Replayable_source.read_from ~offset:1 src in
  let _ = s1 () in let _ = s1 () in    (* s1 продвинули на 2 *)
  let _ = s2 () in                      (* s2 продвинули на 1 *)
  check (Printf.sprintf "s1 offset=2 (got %d)" (off1 ())) (off1 () = 2);
  check (Printf.sprintf "s2 offset=2 (got %d)" (off2 ())) (off2 () = 2);
  (* s1 даёт 300, s2 тоже 300 *)
  let v1 = s1 () in let v2 = s2 () in
  check "s1 next = Some 300" (v1 = Some 300);
  check "s2 next = Some 300" (v2 = Some 300);

  (* ── 6. Empty source ──────────────────────────────────────── *)
  Printf.printf "\n-- 6. Empty source\n";

  let src = Replayable_source.of_list ([] : int list) in
  check "length=0" (Replayable_source.length src = 0);
  let stream, _ = Replayable_source.read_from src in
  check "empty stream" (drain stream = 0);

  (* ── 7. Replay scenario: process N, save offset, "crash",
        restore offset, continue from there ─────────────────── *)
  Printf.printf "\n-- 7. Crash+restore scenario\n";

  let events = ["a"; "b"; "c"; "d"; "e"; "f"; "g"; "h"; "i"; "j"] in
  let src = Replayable_source.of_list events in

  (* Phase 1: читаем 4 elements, имитируем crash *)
  let s1, off1 = Replayable_source.read_from src in
  let xs1 = List.init 4 (fun _ -> s1 ()) in
  let committed_offset = off1 () in
  check "phase 1: read 4, offset committed = 4" (committed_offset = 4);
  check "phase 1: got [a;b;c;d]"
    (xs1 = [Some "a"; Some "b"; Some "c"; Some "d"]);

  (* Phase 2: новый stream с того же source с offset=4 *)
  let s2, off2 = Replayable_source.read_from ~offset:committed_offset src in
  let xs2 = collect s2 in
  check (Printf.sprintf "phase 2: got %d more elements" (List.length xs2))
    (xs2 = ["e"; "f"; "g"; "h"; "i"; "j"]);
  check "phase 2: final offset=10" (off2 () = 10);

  (* Сумма обоих phase'ов даёт оригинальный log *)
  let total = List.length xs1 + List.length xs2 in
  check (Printf.sprintf "no duplicates, no skips: total=%d" total)
    (total = 10);

  Printf.printf "\nAll Replayable_source tests passed.\n"
