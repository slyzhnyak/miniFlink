(** Настоящие Crowbar/afl fuzz-таргеты (аудит 2026-07).

    Собирается ТОЛЬКО когда доступен crowbar (см. dune: этот модуль в
    отдельном executable с (libraries ... crowbar), опционально).

    Запуск на машине с toolchain:
      opam install crowbar
      dune build test/fuzz_crowbar.exe
      # afl-режим:
      mkdir -p fuzz_in && printf '\\x00\\x01' > fuzz_in/seed
      afl-fuzz -i fuzz_in -o fuzz_out -- \\
        _build/default/test/fuzz_crowbar.exe @@
      # или быстрый quickcheck-режим Crowbar (без afl):
      dune exec test/fuzz_crowbar.exe

    Три таргета, по возрастанию опасности:
      1. schema.decode  — ДОЛЖЕН вернуть Ok/Error, не бросить (контракт);
      2. codec round-trip — encode>>decode тождественно;
      3. state_backend restore — ⚠ Marshal.from_bytes на СЫРЫХ байтах:
         в OCaml это небезопасно (segfault/heap corruption на битом
         вводе). Таргет 3 — не «проверка, что не бросает», а
         ИНСТРУМЕНТ ПОИСКА входов, которые валят процесс. Если afl найдёт
         крэш — это подтверждение, что restore нуждается в safe-декодере
         (length-prefixed + версия + проверка границ) вместо голого
         Marshal. См. FUZZING.md, раздел «known-unsafe: Marshal restore». *)

open Miniflink

let string_schema =
  Schema_default.make ~version:1
    ~encode:(fun (s : string) -> Bytes.of_string s)
    ~decode:(fun b -> Ok (Bytes.to_string b))
    ()

let () =
  (* Таргет 1: schema.decode никогда не бросает *)
  Crowbar.add_test ~name:"schema.decode never raises" [ Crowbar.bytes ]
    (fun raw ->
      match Schema_default.decode string_schema (Bytes.of_string raw) with
      | Ok _ | Error _ -> ()
      | exception e ->
        Crowbar.failf "schema.decode raised %s" (Printexc.to_string e));

  (* Таргет 2: round-trip *)
  Crowbar.add_test ~name:"schema round-trip" [ Crowbar.bytes ]
    (fun s ->
      match
        Schema_default.decode string_schema
          (Schema_default.encode string_schema s)
      with
      | Ok s' -> Crowbar.check_eq ~pp:Crowbar.pp_string s s'
      | Error e -> Crowbar.failf "round-trip lost value: %s" e
      | exception e ->
        Crowbar.failf "round-trip raised %s" (Printexc.to_string e));

  (* Таргет 3: state_backend restore. ИСПРАВЛЕНО (N-9): restore теперь
     оборачивает Marshal в Snapshot_frame (магия+длина+md5), поэтому
     битый/чужой вход даёт Failure, а не segfault. Раньше вход
     [Marshal.to_bytes [(1,2);(3,4)]] проходил и валил процесс при
     первом обращении к «ключу» (type confusion). Таргет проверяет, что
     рамка держит: ЛЮБОЙ вход → Failure или Ok, но НЕ segfault.
     Если afl найдёт вход, дающий segfault — рамку пробили (например
     намеренно пересчитанный md5 + type-confused payload), это отдельная
     находка про намеренную атаку (см. FUZZING.md). *)
  Crowbar.add_test ~name:"state_backend restore is crash-safe"
    [ Crowbar.bytes ]
    (fun raw ->
      let b = State_backend_memory.create () in
      (try State_backend_memory.restore b (Bytes.of_string raw)
       with
       | Failure _ -> ()          (* рамка отсекла — ожидаемо *)
       | End_of_file -> ()
       | e ->
         Crowbar.failf "restore raised unexpected %s"
           (Printexc.to_string e)))
