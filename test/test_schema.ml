open Miniflink
(* Тест эволюции схем. Версионированный codec: версия в заголовке,
   миграция старых версий. Проверяем round-trip, миграцию, и что
   неизвестная версия без миграции даёт явную ошибку (не тихий пропуск). *)

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

(* Простой codec строки для тестов *)
let str_encode (s : string) = Bytes.of_string s
let str_decode b = Ok (Bytes.to_string b)

(* ── round-trip в одной версии ─────────────────────────────── *)
let test_same_version () =
  Printf.printf "\n-- same version: encode then decode round-trips\n";
  let c = Schema_default.make ~version:2 ~encode:str_encode ~decode:str_decode () in
  let enc = Schema_default.encode c "hello" in
  check "decode(encode x) = Ok x" (Schema_default.decode c enc = Ok "hello");
  check "current_version = 2" (Schema_default.current_version c = 2)

(* ── версия зашита в заголовок ─────────────────────────────── *)
let test_version_in_header () =
  Printf.printf "\n-- version is stored in the payload header\n";
  let c1 = Schema_default.make ~version:1 ~encode:str_encode ~decode:str_decode () in
  let c2 = Schema_default.make ~version:2 ~encode:str_encode ~decode:str_decode () in
  let e1 = Schema_default.encode c1 "x" in
  let e2 = Schema_default.encode c2 "x" in
  (* первые 2 байта различаются (версия), payload одинаков *)
  check "different version → different header"
    (Bytes.sub e1 0 2 <> Bytes.sub e2 0 2)

(* ── миграция старой версии ────────────────────────────────── *)
let test_migration () =
  Printf.printf "\n-- migrate transforms old-version payload\n";
  (* v2 codec умеет мигрировать v1: v1 хранил "old:DATA", v2 хочет "DATA" *)
  let migrate old_ver payload =
    if old_ver = 1 then
      let s = Bytes.to_string payload in
      let stripped = if String.length s >= 4 && String.sub s 0 4 = "old:"
        then String.sub s 4 (String.length s - 4) else s in
      Bytes.of_string stripped
    else payload
  in
  let c2 = Schema_default.make ~version:2 ~encode:str_encode ~decode:str_decode ~migrate () in
  (* сэмулируем v1-сообщение: версия 1 в заголовке + payload "old:hi" *)
  let c1 = Schema_default.make ~version:1
    ~encode:(fun s -> Bytes.of_string ("old:" ^ s)) ~decode:str_decode () in
  let v1_msg = Schema_default.encode c1 "hi" in
  (* декодируем v1-сообщение codec-ом v2 — должна сработать миграция *)
  check "v2 decodes migrated v1 message" (Schema_default.decode c2 v1_msg = Ok "hi")

(* ── граничные случаи ──────────────────────────────────────── *)
let test_edge_cases () =
  Printf.printf "\n-- edge cases: too-short payload\n";
  let c = Schema_default.make ~version:1 ~encode:str_encode ~decode:str_decode () in
  check "payload < 2 bytes → Error"
    (match Schema_default.decode c (Bytes.of_string "x") with Error _ -> true | _ -> false);
  check "empty payload → Error"
    (match Schema_default.decode c (Bytes.create 0) with Error _ -> true | _ -> false)

(* ── неизвестная версия без миграции = явная ошибка ────────── *)
let test_unknown_version_errors () =
  Printf.printf "\n-- unknown version without migrate → explicit Error (no silent passthrough)\n";
  (* сообщение версии 5, codec версии 2 без migrate *)
  let c5 = Schema_default.make ~version:5 ~encode:str_encode ~decode:str_decode () in
  let msg_v5 = Schema_default.encode c5 "data" in
  let c2 = Schema_default.make ~version:2 ~encode:str_encode ~decode:str_decode () in
  check "decode v5 msg with v2 (no migrate) → Error"
    (match Schema_default.decode c2 msg_v5 with Error _ -> true | _ -> false);
  (* а с миграцией — успех *)
  let c2m = Schema_default.make ~version:2 ~encode:str_encode ~decode:str_decode
    ~migrate:(fun _ p -> p) () in
  check "decode v5 msg with v2 + migrate → Ok"
    (match Schema_default.decode c2m msg_v5 with Ok _ -> true | _ -> false)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Schema evolution (versioned codec)\n";
  Printf.printf "==========================================\n";
  test_same_version ();
  test_version_in_header ();
  test_migration ();
  test_edge_cases ();
  test_unknown_version_errors ();
  Printf.printf "\nAll schema tests passed.\n"
