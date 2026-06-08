open Miniflink
(* Тест бага #1: sliding window assign *)
open Time

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

(* Проверяем что событие попадает ровно в нужные окна *)
let windows_for ts size step =
  Pipe.assign (Pipe.sliding size step) ts

let sorted ws = List.sort compare ws

let test_sliding_membership () =
  Printf.printf "\n-- Sliding window assignment\n";

  (* size=60s, step=30s. ts=45s → окна [0,60) и [30,90) *)
  let w = sorted (windows_for (seconds 45) (seconds 60) (seconds 30)) in
  check "ts=45s in [0,60) and [30,90)"
    (w = [(0, seconds 60); (seconds 30, seconds 90)]);

  (* ts=5s → только [0,60) (окно [-30,30) невалидно, s>=0) *)
  let w = sorted (windows_for (seconds 5) (seconds 60) (seconds 30)) in
  check "ts=5s in [0,60) only"
    (w = [(0, seconds 60)]);

  (* ts=65s → [30,90) и [60,120) *)
  let w = sorted (windows_for (seconds 65) (seconds 60) (seconds 30)) in
  check "ts=65s in [30,90) and [60,120)"
    (w = [(seconds 30, seconds 90); (seconds 60, seconds 120)]);

  (* Каждое окно действительно содержит ts: s <= ts < s+size *)
  let all_contain ts size step =
    List.for_all (fun (s, e) -> s <= ts && ts < e)
      (windows_for ts size step) in
  check "all windows contain ts (45s)" (all_contain (seconds 45) (seconds 60) (seconds 30));
  check "all windows contain ts (123s)" (all_contain 123000 (seconds 60) (seconds 30));
  check "all windows contain ts (7s)" (all_contain 7000 (seconds 60) (seconds 30));

  (* Число окон = size/step для ts глубоко внутри потока *)
  let n = List.length (windows_for 300000 (seconds 60) (seconds 30)) in
  check "ts=300s: exactly size/step=2 windows" (n = 2);

  (* size=100s step=25s → 4 окна для большого ts *)
  let n = List.length (windows_for 1000000 (seconds 100) (seconds 25)) in
  check "size=100 step=25: 4 windows" (n = 4);

  (* Малый шаг не вызывает stack overflow и даёт верное число *)
  let n = List.length (windows_for 10000000 (seconds 60) (seconds 1)) in
  check "small step (size=60 step=1): 60 windows, no overflow" (n = 60)

(* Tumbling не сломан *)
let test_tumbling () =
  Printf.printf "\n-- Tumbling unchanged\n";
  let w = Pipe.assign (Pipe.tumbling (seconds 30)) (seconds 45) in
  check "ts=45s → single window [30,60)" (w = [(seconds 30, seconds 60)])

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Bug #1: sliding window assign\n";
  Printf.printf "==========================================\n";
  test_tumbling ();
  test_sliding_membership ();
  Printf.printf "\nAll sliding window tests passed.\n"
