(* Smoke-тест ex11 (учёт присутствия) — защищает от регресса
   семантики Data/Update/Retract в примере, который иллюстрирует, когда
   нужен именно Retract. Запускает example как процесс и проверяет, что
   все три типа события появляются и live-карта верна. *)

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let contains hay needle =
  let nl = String.length needle and hl = String.length hay in
  let rec go i = i + nl <= hl && (String.sub hay i nl = needle || go (i+1)) in
  go 0

let run_example path =
  let ic = Unix.open_process_in path in
  let buf = Buffer.create 4096 in
  (try while true do Buffer.add_channel buf ic 4096 done
   with End_of_file -> ());
  let _ = Unix.close_process_in ic in
  Buffer.contents buf

(* рабочая директория при запуске теста неизвестна — пробуем варианты *)
let find_and_run () =
  let candidates = [
    "./examples/ex11_presence.exe";
    "../examples/ex11_presence.exe";
    "../../examples/ex11_presence.exe";
    "_build/default/examples/ex11_presence.exe";
  ] in
  let rec try_paths = function
    | [] -> fail "ex11_presence.exe не найден ни по одному пути"
    | p :: rest ->
      let out = run_example p in
      if String.length out > 0 then out else try_paths rest
  in try_paths candidates

let () =
  Printf.printf "Smoke-тест ex11 (presence / Retract)\n%!";
  let out = find_and_run () in

  (* все три типа корректирующих событий должны появиться *)
  check "[Data] присутствует (появление)"   (contains out "[Data]");
  check "[Update] присутствует (смена зоны)" (contains out "[Update]");
  check "[Retract] присутствует (выход)"     (contains out "[Retract]");

  (* конкретные переходы *)
  check "A перешёл shaft_1 → tunnel_3 (Update)"
    (contains out "перешёл shaft_1 → tunnel_3");
  check "B покинул шахту (Retract)"
    (contains out "miner_B покинул");
  check "A покинул шахту (Retract)"
    (contains out "miner_A покинул");

  (* итоговая live-карта: остался только C, A и B вышли *)
  check "live-карта содержит miner_C → shaft_2"
    (contains out "miner_C → shaft_2");
  check "miner_A НЕ на финальной карте (вышел)"
    (not (contains out "miner_A → "));
  check "miner_B НЕ на финальной карте (вышел)"
    (not (contains out "miner_B → "));

  Printf.printf "\nex11 smoke test passed.\n"
