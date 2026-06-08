open Miniflink
(* Тесты Codec — сериализация была дырой в покрытии (0 тестов).
   Главное свойство: decode(encode(x)) = x. Плюс граничные данные
   и пути ошибок (битый ввод → Error, не исключение). *)

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

(* ── JSON codec для простой записи ─────────────────────────── *)
type rec_t = { id : string; n : int; flag : bool }

let rec_codec : rec_t Codec.t =
  Codec.json
    ~encode:(fun r -> `Assoc [
      ("id", `String r.id); ("n", `Int r.n); ("flag", `Bool r.flag)])
    ~decode:(fun j ->
      match j with
      | `Assoc l ->
        (match List.assoc_opt "id" l, List.assoc_opt "n" l, List.assoc_opt "flag" l with
         | Some (`String id), Some (`Int n), Some (`Bool flag) -> Ok { id; n; flag }
         | _ -> Error "bad fields")
      | _ -> Error "not an object")

let roundtrip c x =
  match c.Codec.decode (c.Codec.encode x) with Ok y -> y = x | Error _ -> false

(* ── 1. Round-trip на конкретных значениях ─────────────────── *)
let test_roundtrip_basic () =
  Printf.printf "\n-- JSON round-trip: decode(encode(x)) = x\n";
  check "simple" (roundtrip rec_codec { id = "a"; n = 1; flag = true });
  check "negative int" (roundtrip rec_codec { id = "x"; n = -42; flag = false });
  check "zero" (roundtrip rec_codec { id = "z"; n = 0; flag = false });
  check "large int" (roundtrip rec_codec { id = "big"; n = 1_000_000_000; flag = true });
  check "min_int" (roundtrip rec_codec { id = "m"; n = min_int; flag = false });
  check "max_int" (roundtrip rec_codec { id = "M"; n = max_int; flag = true })

(* ── 2. Граничные строки (Unicode, спецсимволы, пустые) ────── *)
let test_roundtrip_strings () =
  Printf.printf "\n-- Round-trip with tricky strings\n";
  check "empty string" (roundtrip rec_codec { id = ""; n = 1; flag = true });
  check "unicode" (roundtrip rec_codec { id = "Асхат Каймулдин"; n = 1; flag = true });
  check "quotes" (roundtrip rec_codec { id = "a\"b\"c"; n = 1; flag = true });
  check "backslash" (roundtrip rec_codec { id = "a\\b"; n = 1; flag = true });
  check "newline" (roundtrip rec_codec { id = "line1\nline2"; n = 1; flag = true });
  check "tab+control" (roundtrip rec_codec { id = "a\tb"; n = 1; flag = true });
  check "json-injection" (roundtrip rec_codec { id = "{\"fake\":1}"; n = 1; flag = true })

(* ── 3. Пути ошибок — битый ввод даёт Error, не исключение ── *)
let test_decode_errors () =
  Printf.printf "\n-- Decode errors return Error, never raise\n";
  let dec b = rec_codec.Codec.decode (Bytes.of_string b) in
  check "invalid json → Error" (match dec "{not json" with Error _ -> true | _ -> false);
  check "empty bytes → Error" (match dec "" with Error _ -> true | _ -> false);
  check "wrong shape (array) → Error" (match dec "[1,2,3]" with Error _ -> true | _ -> false);
  check "missing fields → Error" (match dec "{\"id\":\"a\"}" with Error _ -> true | _ -> false);
  check "wrong types → Error" (match dec "{\"id\":1,\"n\":\"x\",\"flag\":1}" with Error _ -> true | _ -> false);
  check "garbage bytes → Error"
    (match dec "\x00\x01\xff\xfe" with Error _ -> true | _ -> false)

(* ── 4. QCheck property: round-trip для произвольных записей ── *)
let test_roundtrip_property () =
  Printf.printf "\n-- QCheck: round-trip holds for arbitrary records\n";
  let gen = QCheck.make
    QCheck.Gen.(map3 (fun id n flag -> { id; n; flag })
                  string small_signed_int bool) in
  let t = QCheck.Test.make ~count:500 ~name:"json roundtrip" gen
    (fun r -> roundtrip rec_codec r) in
  let res = QCheck_base_runner.run_tests ~verbose:false [t] in
  check "property: decode(encode(x))=x over 500 random" (res = 0)

(* ── 5. protobuf-обёртка тоже ловит исключения в Error ─────── *)
let test_protobuf_wrapper () =
  Printf.printf "\n-- protobuf codec wraps exceptions as Error\n";
  let c = Codec.protobuf
    ~encode:(fun (x:int) -> Bytes.of_string (string_of_int x))
    ~decode:(fun b -> int_of_string (Bytes.to_string b)) in  (* кидает на мусоре *)
  check "protobuf ok roundtrip"
    (match c.Codec.decode (c.Codec.encode 123) with Ok 123 -> true | _ -> false);
  check "protobuf bad input → Error (not raise)"
    (match c.Codec.decode (Bytes.of_string "not-a-number") with Error _ -> true | _ -> false)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Codec tests (round-trip + error paths)\n";
  Printf.printf "==========================================\n";
  test_roundtrip_basic ();
  test_roundtrip_strings ();
  test_decode_errors ();
  test_roundtrip_property ();
  test_protobuf_wrapper ();
  Printf.printf "\nAll codec tests passed.\n"
