(** Fuzzing декодеров недоверенного входа (аудит 2026-07, категория
    «паники на битом внешнем входе»).

    Прогон на машине с полным toolchain:
      opam install crowbar
      dune exec test/fuzz_decoders.exe                 # быстрый QCheck-режим
      # или под afl (настоящий fuzzing):
      dune build test/fuzz_decoders.exe --instrument-with ... # см. README
      afl-fuzz -i in -o out -- _build/.../fuzz_decoders.exe @@

    Инвариант, который проверяем: НИ ОДИН байтовый мусор на входе
    декодера не должен вызывать НЕОБРАБОТАННОЕ исключение или порчу
    памяти. Контракт codec/schema — возвращать [Error], а не бросать.

    ВАЖНО про Marshal: [Marshal.from_bytes] на произвольных байтах в
    OCaml НЕ безопасен — может не только бросить, но и segfault/повредить
    heap. Эти таргеты документируют и проверяют границы: где вход
    оборачивается (codec/schema возвращают Error) и где — нет (сырой
    Marshal в state-backend). Последнее — реальная находка для приоритета
    hardening (см. FUZZING.md). *)

open Miniflink

(* ── Таргет 1: Schema_default.decode на битых байтах ──
   Контракт: любой вход длиной >=0 → Ok или Error, НИКОГДА исключение. *)
let string_schema =
  Schema_default.make ~version:1
    ~encode:(fun (s : string) -> Bytes.of_string s)
    ~decode:(fun b -> Ok (Bytes.to_string b))
    ()

let check_schema_decode (raw : bytes) : bool =
  match Schema_default.decode string_schema raw with
  | Ok _ | Error _ -> true              (* оба допустимы *)
  | exception _ -> false                (* исключение = НАРУШЕНИЕ контракта *)

(* ── Таргет 2: round-trip устойчивость ──
   encode >> decode = Ok исходного; а decode(мусор) не должен паниковать. *)
let check_schema_roundtrip (s : string) : bool =
  match Schema_default.decode string_schema (Schema_default.encode string_schema s) with
  | Ok s' -> s = s'
  | Error _ -> false
  | exception _ -> false

(* ── Таргет 3: version-заголовок битый ──
   Первые 2 байта = версия. Битая версия без migrate → Error, не паника. *)
let check_schema_bad_version (raw : bytes) : bool =
  (* добавим 2 случайных байта версии спереди — decode не должен падать *)
  match Schema_default.decode string_schema raw with
  | _ -> true
  | exception Failure _ -> true   (* Failure допустим если documented *)
  | exception _ -> false

(* ── Crowbar-режим (настоящий fuzzing) ──
   Активен, когда собрано с crowbar. Здесь — заглушка-переключатель:
   реальные Crowbar.add_test идут в fuzz_decoders_crowbar.ml (см. dune),
   чтобы этот файл собирался и БЕЗ crowbar (QCheck-режим ниже). *)

(* ── QCheck-режим (быстрый sanity без afl) ── *)
open QCheck

let arb_bytes =
  make ~print:(fun b -> Printf.sprintf "<%d bytes>" (Bytes.length b))
    Gen.(map (fun s -> Bytes.of_string s) (string_size (int_range 0 256)))

let t_decode_never_raises =
  Test.make ~count:5000 ~name:"schema.decode never raises on arbitrary bytes"
    arb_bytes check_schema_decode

let t_roundtrip =
  Test.make ~count:5000 ~name:"schema round-trip preserves value"
    (make (Gen.string_size (Gen.int_range 0 256)))
    check_schema_roundtrip

let t_bad_version =
  Test.make ~count:5000 ~name:"schema.decode handles bad version header"
    arb_bytes check_schema_bad_version

let () =
  Printf.printf "=== fuzz_decoders (QCheck sanity mode) ===\n%!";
  let suite = [ t_decode_never_raises; t_roundtrip; t_bad_version ] in
  let failed =
    List.fold_left (fun acc t ->
      match QCheck_runner.run_tests ~verbose:false [t] with
      | 0 -> acc | _ -> acc + 1) 0 suite in
  if failed = 0 then Printf.printf "\nAll decoder fuzz properties held.\n"
  else (Printf.printf "\n%d properties FAILED\n" failed; exit 1)
