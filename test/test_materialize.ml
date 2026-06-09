open Miniflink
(* materialize: свернуть поток с retract'ами в ФИНАЛЬНУЮ таблицу записей.
   Декларативная замена ручного Hashtbl-цикла «Data → положить, Retract →
   убрать». Идентичность записи задаётся функцией ~by. *)

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

(* окно эмитит (depot, count); retract отменяет прошлую эмиссию того же
   (depot, window_end). Идентичность: (depot, ts окна). *)
let test_materialize_applies_retracts () =
  Printf.printf "\n-- materialize applies retracts, keeps final value\n";
  let stream = Stream.of_list [
    Mf_event.data ("north", 3) 10;
    Mf_event.data ("south", 5) 10;
    Mf_event.retract ("north", 3) 10;   (* пере-счёт north@10 *)
    Mf_event.data ("north", 4) 10;      (* новый результат north@10 *)
    Mf_event.data ("north", 2) 20;      (* другое окно *)
  ] in
  (* идентичность записи = (depot, время окна). materialize даёт
     финальное значение каждой записи после применения retract *)
  let final = Pipe.materialize ~by:(fun (depot, _) ts -> (depot, ts)) stream in
  let sorted = List.sort compare final in
  check "north@10 = 4 (after retract+redo), south@10=5, north@20=2"
    (sorted = [ (("north",10), ("north",4));
                (("north",20), ("north",2));
                (("south",10), ("south",5)) ])

let test_materialize_no_retracts () =
  Printf.printf "\n-- materialize without retracts = just the data\n";
  let stream = Stream.of_list [
    Mf_event.data ("a", 1) 0;
    Mf_event.data ("b", 2) 0;
  ] in
  let final = Pipe.materialize ~by:(fun (k,_) ts -> (k, ts)) stream in
  check "two records" (List.length final = 2)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  materialize (declarative result collection)\n";
  Printf.printf "==========================================\n";
  test_materialize_applies_retracts ();
  test_materialize_no_retracts ();
  Printf.printf "\nmaterialize tests passed.\n"
