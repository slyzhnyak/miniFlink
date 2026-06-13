(** Тесты Item.silence_age. *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

(* Запустить silence_age на списке событий, собрать все эмиссии *)
let run ?(tick = 100) events =
  events
  |> Stream.of_list
  |> Item.silence_age ~by:(fun s -> s) ~tick
  |> (fun s ->
       let acc = ref [] in
       let rec loop () = match s () with
         | None -> () | Some e -> acc := e :: !acc; loop ()
       in loop ();
       List.rev !acc)

let only_data = List.filter (function
  | Mf_event.Data _ -> true | _ -> false)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Item.silence_age\n";
  Printf.printf "==========================================\n";

  (* ── 1. На каждое Data эмитится (key, 0) ───────────────────── *)
  Printf.printf "\n-- 1. Data event → (key, 0)\n";

  let evs = run ~tick:100 [
    Mf_event.data "A" 50;
  ] in
  (match only_data evs with
   | [Mf_event.Data (("A", 0), 50)] ->
     pass "single Data: emits (A, 0)"
   | _ -> fail "expected single (A, 0)");

  (* ── 2. Без watermark — таймеры не срабатывают ─────────────── *)
  Printf.printf "\n-- 2. No watermark → no silence emissions\n";

  let evs = run ~tick:100 [
    Mf_event.data "A" 50;
  ] in
  let n = List.length (only_data evs) in
  check (Printf.sprintf "without WM: only 1 emission (got %d)" n) (n = 1);

  (* ── 3. Watermark проходит fire_at → silence-эмиссия ───────── *)
  Printf.printf "\n-- 3. Watermark fires silence timer\n";

  let evs = run ~tick:100 [
    Mf_event.data "A" 50;     (* last_seen=50, timer fires at 150 *)
    Mf_event.wm 200;           (* wm>=150 → эмит (A, 200-50)=150 *)
  ] in
  (match only_data evs with
   | [Mf_event.Data (("A", 0), 50);
      Mf_event.Data (("A", 150), 200)] ->
     pass "silence emission: (A, 0) then (A, 150)"
   | other ->
     Printf.printf "  unexpected (%d):\n" (List.length other);
     List.iter (function
       | Mf_event.Data ((k, age), ts) ->
         Printf.printf "    Data (%s, %d) @ %d\n" k age ts
       | _ -> ()) other;
     fail "expected 2 Data events");

  (* ── 4. Несколько тиков подряд ─────────────────────────────── *)
  Printf.printf "\n-- 4. Multiple ticks\n";

  let evs = run ~tick:100 [
    Mf_event.data "A" 0;       (* last=0, timer at 100 *)
    Mf_event.wm 250;            (* проходит 100 → эмит (A, 250); перерегистр на 350 *)
    Mf_event.wm 400;            (* проходит 350 → эмит (A, 400) *)
  ] in
  let ages =
    only_data evs |> List.filter_map (function
      | Mf_event.Data (("A", age), _) -> Some age
      | _ -> None) in
  check (Printf.sprintf "ages = [0; 250; 400] (got %s)"
           (String.concat ";" (List.map string_of_int ages)))
    (ages = [0; 250; 400]);

  (* ── 5. Event сбрасывает silence ───────────────────────────── *)
  Printf.printf "\n-- 5. New event resets silence\n";

  let evs = run ~tick:100 [
    Mf_event.data "A" 0;
    Mf_event.wm 200;            (* эмит (A, 200) *)
    Mf_event.data "A" 250;      (* эмит (A, 0), сбрасывает таймер *)
    Mf_event.wm 500;            (* теперь last=250, age=500-250=250 *)
  ] in
  let ages =
    only_data evs |> List.filter_map (function
      | Mf_event.Data (("A", age), _) -> Some age
      | _ -> None) in
  check (Printf.sprintf "ages = [0; 200; 0; 250] (got %s)"
           (String.concat ";" (List.map string_of_int ages)))
    (ages = [0; 200; 0; 250]);

  (* ── 6. Per-key изоляция ───────────────────────────────────── *)
  Printf.printf "\n-- 6. Per-key independence\n";

  let evs = run ~tick:100 [
    Mf_event.data "A" 0;
    Mf_event.data "B" 50;
    Mf_event.wm 200;            (* A: age=200, B: age=150 *)
  ] in
  let counts =
    only_data evs |> List.fold_left (fun (a, b) ev ->
      match ev with
      | Mf_event.Data (("A", _), _) -> (a + 1, b)
      | Mf_event.Data (("B", _), _) -> (a, b + 1)
      | _ -> (a, b)) (0, 0) in
  check (Printf.sprintf "A: 2 emissions, B: 2 emissions (got A=%d B=%d)"
           (fst counts) (snd counts))
    (fst counts = 2 && snd counts = 2);

  Printf.printf "\nAll Item tests passed.\n"
