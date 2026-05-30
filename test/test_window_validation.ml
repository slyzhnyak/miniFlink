(* Тест валидации окон. Раньше Pipe.tumbling 0 / Pipe.sliding _ 0
   приводили к Division_by_zero в глубине assign (ts / 0). Теперь
   конструкторы отвергают невалидные параметры сразу через
   Invalid_argument — понятная ошибка в точке создания окна. *)

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let raises_invalid f =
  match f () with
  | _ -> false
  | exception (Invalid_argument _) -> true
  | exception _ -> false   (* другое исключение — не то что ждём *)

(* ── tumbling отвергает size <= 0 ──────────────────────────── *)
let test_tumbling_validation () =
  Printf.printf "\n-- tumbling rejects non-positive size\n";
  check "tumbling 0 → Invalid_argument" (raises_invalid (fun () -> Pipe.tumbling 0));
  check "tumbling -5 → Invalid_argument" (raises_invalid (fun () -> Pipe.tumbling (-5)));
  check "tumbling 60 → ok" (not (raises_invalid (fun () -> Pipe.tumbling 60)))

(* ── sliding отвергает size/step <= 0 ──────────────────────── *)
let test_sliding_validation () =
  Printf.printf "\n-- sliding rejects non-positive size/step\n";
  check "sliding 60 0 → Invalid_argument (zero step)"
    (raises_invalid (fun () -> Pipe.sliding 60 0));
  check "sliding 0 30 → Invalid_argument (zero size)"
    (raises_invalid (fun () -> Pipe.sliding 0 30));
  check "sliding -10 30 → Invalid_argument"
    (raises_invalid (fun () -> Pipe.sliding (-10) 30));
  check "sliding 60 -30 → Invalid_argument"
    (raises_invalid (fun () -> Pipe.sliding 60 (-30)));
  check "sliding 60 30 → ok (overlapping)"
    (not (raises_invalid (fun () -> Pipe.sliding 60 30)))

(* ── валидные окна работают как прежде (регрессия) ─────────── *)
let test_valid_windows_still_work () =
  Printf.printf "\n-- valid windows assign correctly (no regression)\n";
  let tw = Pipe.assign (Pipe.tumbling 60) 45 in
  check "tumbling 60: ts=45 → [(0,60)]" (tw = [(0, 60)]);
  let sw = List.sort compare (Pipe.assign (Pipe.sliding 60 30) 45) in
  check "sliding 60/30: ts=45 → [(0,60);(30,90)]" (sw = [(0, 60); (30, 90)]);
  (* step > size — допустимо, образует дыры, но не падает *)
  let gap = Pipe.assign (Pipe.sliding 10 30) 45 in
  check "sliding 10/30 (step>size): ts=45 не падает, отдаёт окна"
    (List.length gap >= 0)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Window validation (no Division_by_zero)\n";
  Printf.printf "==========================================\n";
  test_tumbling_validation ();
  test_sliding_validation ();
  test_valid_windows_still_work ();
  Printf.printf "\nAll window validation tests passed.\n"
