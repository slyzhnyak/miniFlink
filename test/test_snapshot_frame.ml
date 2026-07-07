(** N-9 (аудит 2026-07): Snapshot_frame защищает restore от сырого
    Marshal на битом/чужом вводе.

    Подтверждённая находка: [State_backend_memory.restore
    (Marshal.to_bytes [(1,2);(3,4)])] раньше проходил без ошибки, а
    первое обращение к «ключу» давало SIGSEGV (type confusion). Рамка
    (магия+длина+md5) отсекает такой вход ДО Marshal.from_bytes.

    Проверяем:
    - валидный round-trip snapshot→restore сохраняет состояние;
    - битые/чужие входы дают Failure, НЕ segfault (иначе тест бы упал
      сигналом);
    - clear состояния происходит ПОСЛЕ успешного декода (битый вход не
      теряет существующее состояние). *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  N-9: Snapshot_frame защита restore\n";
  Printf.printf "==========================================\n";

  (* ── round-trip: валидный снапшот восстанавливается ── *)
  Printf.printf "\n-- валидный snapshot -> restore\n";
  let b1 = State_backend_memory.create () in
  State_backend_memory.set b1 "k1" (Bytes.of_string "v1");
  State_backend_memory.set b1 "k2" (Bytes.of_string "v2");
  let snap = State_backend_memory.snapshot b1 in
  let b2 = State_backend_memory.create () in
  State_backend_memory.restore b2 snap;
  check "round-trip: k1 восстановлен"
    (State_backend_memory.get b2 "k1" = Some (Bytes.of_string "v1"));
  check "round-trip: k2 восстановлен"
    (State_backend_memory.get b2 "k2" = Some (Bytes.of_string "v2"));
  check "round-trip: ровно 2 ключа"
    (State_backend_memory.size b2 = 2);

  (* ── битые входы: Failure, не segfault ── *)
  Printf.printf "\n-- битый вход -> Failure (не segfault)\n";
  let expect_failure name b =
    let bk = State_backend_memory.create () in
    match State_backend_memory.restore bk b with
    | () -> fail (name ^ ": прошёл без ошибки (ожидался Failure)")
    | exception Failure _ -> pass (name ^ ": Failure (отсечён рамкой)")
    | exception e -> fail (name ^ ": неожиданное " ^ Printexc.to_string e)
  in
  expect_failure "пустой вход" (Bytes.create 0);
  expect_failure "случайный мусор" (Bytes.of_string "not a snapshot at all");
  (* САМЫЙ ОПАСНЫЙ: сырой Marshal чужого типа — раньше segfault *)
  expect_failure "raw Marshal (int*int) list — бывший segfault"
    (Marshal.to_bytes [(1,2);(3,4)] []);
  expect_failure "raw Marshal int" (Marshal.to_bytes 42 []);
  (* framed, но с подделанной длиной *)
  let good = State_backend_memory.snapshot b1 in
  let tampered = Bytes.copy good in
  Bytes.set tampered 5 '\255';   (* портим payload_len *)
  expect_failure "подделанная длина в заголовке" tampered;
  (* framed, но payload искажён (crc не сойдётся) *)
  let corrupt = Bytes.copy good in
  Bytes.set corrupt (Bytes.length corrupt - 1)
    (Char.chr ((Char.code (Bytes.get corrupt (Bytes.length corrupt - 1)) + 1) land 0xFF));
  expect_failure "искажённый payload (crc mismatch)" corrupt;

  (* ── битый вход не теряет существующее состояние ── *)
  Printf.printf "\n-- битый restore не стирает существующее состояние\n";
  let b3 = State_backend_memory.create () in
  State_backend_memory.set b3 "keep" (Bytes.of_string "safe");
  (try State_backend_memory.restore b3 (Bytes.of_string "garbage")
   with Failure _ -> ());
  check "существующий ключ уцелел после битого restore"
    (State_backend_memory.get b3 "keep" = Some (Bytes.of_string "safe"));

  Printf.printf "\nSnapshot_frame tests passed.\n"
