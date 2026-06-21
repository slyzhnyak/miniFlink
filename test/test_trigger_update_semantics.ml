(** Покрытие: как Trigger реагирует на Update на input.

    Update{old, new} — атомарная коррекция входного значения. Триггер
    реагирует на new_value (как на свежее значение). Проверяем что
    коррекция вниз через порог даёт recovery, а вверх — alert. *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

type alert = Above of string * float | Resolved of string

let trig () =
  Trigger.create
    ~name:"above5"
    ~condition:(Trigger.greater_than 5.0)
    ~severity:Trigger.Warning
    ~produce_alert:(fun ~key ~value ~ts:_ -> Above (key, value))
    ~produce_recovery:(fun ~key ~ts:_ -> Resolved key)
    ()

let run spec evs =
  evs |> Stream.of_list |> Trigger.of_stream spec
  |> Stream.to_list
  |> List.filter_map (function
     | Mf_event.Data (a, _) -> Some a | _ -> None)

let () =
  Printf.printf "Trigger + Update on input\n%!";

  (* 1. Data выше порога → alert. Update вниз через порог → recovery *)
  Printf.printf "\n-- 1. fire above, then Update down through threshold → recovery\n";
  let evs = [
    Mf_event.data ("A", 10.0) 0;                    (* 10 > 5 → alert *)
    Mf_event.wm 1000;
    Mf_event.update ("A", 10.0) ("A", 2.0) 1100;    (* коррекция 10→2, 2 < 5 *)
    Mf_event.wm 2000;
  ] in
  let alerts = run (trig ()) evs in
  Printf.printf "  events: %s\n" (String.concat ", " (List.map (function
    | Above (k,v) -> Printf.sprintf "Above(%s,%.0f)" k v
    | Resolved k -> Printf.sprintf "Resolved(%s)" k) alerts));
  check "Update below threshold triggers recovery"
    (List.exists (function Resolved "A" -> true | _ -> false) alerts);

  (* 2. Data ниже порога, Update вверх через порог → alert *)
  Printf.printf "\n-- 2. below, then Update up through threshold → alert\n";
  let evs = [
    Mf_event.data ("B", 2.0) 0;                     (* 2 < 5 → ничего *)
    Mf_event.wm 1000;
    Mf_event.update ("B", 2.0) ("B", 10.0) 1100;    (* коррекция 2→10, 10 > 5 *)
    Mf_event.wm 2000;
  ] in
  let alerts = run (trig ()) evs in
  Printf.printf "  events: %s\n" (String.concat ", " (List.map (function
    | Above (k,v) -> Printf.sprintf "Above(%s,%.0f)" k v
    | Resolved k -> Printf.sprintf "Resolved(%s)" k) alerts));
  check "Update above threshold triggers alert"
    (List.exists (function Above ("B", _) -> true | _ -> false) alerts);

  (* 3. Update в пределах одной зоны (оба выше) → без дубля alert *)
  Printf.printf "\n-- 3. Update within same zone (both above) → no duplicate alert\n";
  let evs = [
    Mf_event.data ("C", 10.0) 0;                    (* alert *)
    Mf_event.wm 1000;
    Mf_event.update ("C", 10.0) ("C", 20.0) 1100;   (* 20 тоже > 5, уже в alert *)
    Mf_event.wm 2000;
  ] in
  let alerts = run (trig ()) evs in
  let aboves = List.length (List.filter (function Above ("C",_) -> true | _ -> false) alerts) in
  Printf.printf "  Above(C) count: %d\n" aboves;
  check "no duplicate alert when staying in problem zone" (aboves <= 1);

  Printf.printf "\nDone.\n"
