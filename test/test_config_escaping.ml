(** Регрессия: Config.to_json должен экранировать state_dir, иначе
    путь с кавычкой ломает JSON (флаг ревью). *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Config.to_json escaping\n";
  Printf.printf "==========================================\n";

  (* путь с двойной кавычкой и бэкслешем *)
  let cfg = { Config.default with
              state_dir = "/var/lib/\"mine\"\\state" } in
  let json = Config.to_json cfg in
  Printf.printf "\n  json: %s\n%!" json;

  (* результат должен парситься Yojson без ошибки *)
  (match Yojson.Safe.from_string json with
   | exception e -> fail (Printf.sprintf "invalid JSON: %s" (Printexc.to_string e))
   | parsed ->
     pass "output is valid JSON";
     (* и state_dir внутри должен совпасть с исходным после парсинга *)
     (match parsed with
      | `Assoc fields ->
        (match List.assoc_opt "state_dir" fields with
         | Some (`String s) ->
           check "state_dir round-trips intact" (s = "/var/lib/\"mine\"\\state")
         | _ -> fail "state_dir missing or not a string")
      | _ -> fail "not a JSON object"));

  (* обычный путь без спецсимволов тоже валиден *)
  let cfg2 = { Config.default with state_dir = "/tmp/state" } in
  (match Yojson.Safe.from_string (Config.to_json cfg2) with
   | exception _ -> fail "plain path produced invalid JSON"
   | _ -> pass "plain path still valid");

  Printf.printf "\nConfig escaping regression passed.\n"
