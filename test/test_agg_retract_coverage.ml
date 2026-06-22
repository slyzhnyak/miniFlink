(** Покрытие веток Agg, не задетых happy-path тестами:
    - проверки ошибок (top_k_by/bottom_k_by k<=0);
    - is_retractable для всех категорий;
    - remove-функции retractable-комбинаторов (both/map/contramap);
    - group_by retract: нет группы / inner не retractable;
    - to_list/count_if remove через with_parts2. *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Agg: error paths + retract combinators\n";
  Printf.printf "==========================================\n";

  (* ── 1. Проверки аргументов ────────────────────────────────── *)
  Printf.printf "\n-- 1. argument validation\n";
  check "top_k_by 0 → invalid_arg"
    (try ignore (Agg.top_k_by 0 ~by:(fun x -> float x)); false
     with Invalid_argument _ -> true);
  check "top_k_by -3 → invalid_arg"
    (try ignore (Agg.top_k_by (-3) ~by:(fun x -> float x)); false
     with Invalid_argument _ -> true);
  check "bottom_k_by 0 → invalid_arg"
    (try ignore (Agg.bottom_k_by 0 ~by:(fun x -> float x)); false
     with Invalid_argument _ -> true);

  (* ── 2. is_retractable по категориям ───────────────────────── *)
  Printf.printf "\n-- 2. is_retractable categories\n";
  check "count retractable" (Agg.is_retractable Agg.count);
  check "count_if retractable" (Agg.is_retractable (Agg.count_if (fun x -> x > 0)));
  check "sum retractable" (Agg.is_retractable (Agg.sum (fun x -> float x)));
  check "mean retractable" (Agg.is_retractable (Agg.mean (fun x -> float x)));
  check "to_list retractable" (Agg.is_retractable (Agg.to_list));
  check "top_k_by NOT retractable"
    (not (Agg.is_retractable (Agg.top_k_by 3 ~by:(fun x -> float x))));
  check "min_by NOT retractable"
    (not (Agg.is_retractable (Agg.min_by (fun x -> float x))));

  (* both: retractable только если ОБА retractable *)
  check "both(count,sum) retractable"
    (Agg.is_retractable (Agg.both Agg.count (Agg.sum (fun x -> float x))));
  check "both(count,min_by) NOT retractable"
    (not (Agg.is_retractable (Agg.both Agg.count (Agg.min_by (fun x -> float x)))));

  (* map сохраняет retractability базового *)
  check "map over count retractable"
    (Agg.is_retractable (Agg.map (fun n -> n * 2) Agg.count));
  (* contramap сохраняет retractability *)
  check "contramap into sum retractable"
    (Agg.is_retractable (Agg.contramap (fun s -> String.length s) (Agg.sum float_of_int)));

  (* ── 3. remove-функции через with_parts2 (реальный пересчёт) ─ *)
  Printf.printf "\n-- 3. retract recomputation via with_parts2\n";
  (* count: add [1;2;3], remove 2 → 2 *)
  let run_remove (type a r) (agg : (a,r) Agg.t) (xs : a list) (rm : a) : r option =
    Agg.with_parts2 agg { k2 = fun init add remove finish ->
      match remove with
      | None -> None
      | Some remf ->
        let acc = List.fold_left add (init ()) xs in
        Some (finish (remf acc rm)) }
  in
  check "count: add[1;2;3] remove 2 = 2"
    (run_remove Agg.count [1;2;3] 2 = Some 2);
  check "sum: add[10;20;30] remove 20 = 40"
    (run_remove (Agg.sum (fun x -> float x)) [10;20;30] 20 = Some 40.0);
  check "to_list: add[1;2;3] remove 2 = [1;3]"
    (run_remove Agg.to_list [1;2;3] 2 = Some [1;3]);
  check "count_if(>0): add[1;-1;2] remove 1 = 1"
    (run_remove (Agg.count_if (fun x -> x > 0)) [1;-1;2] 1 = Some 1);

  (* both retract: оба компонента откатываются *)
  check "both(count,sum): add[1;2;3] remove 2 = (2, 4.0)"
    (run_remove (Agg.both Agg.count (Agg.sum (fun x -> float x))) [1;2;3] 2
     = Some (2, 4.0));

  (* map retract: применяет f к откаченному результату *)
  check "map(*2) over count: add[1;2;3] remove 2 = 4"
    (run_remove (Agg.map (fun n -> n * 2) Agg.count) [1;2;3] 2 = Some 4);

  (* contramap retract *)
  check "contramap len into sum: add[\"ab\";\"c\"] remove \"ab\" = 1.0"
    (run_remove (Agg.contramap String.length (Agg.sum float_of_int)) ["ab";"c"] "ab"
     = Some 1.0);

  (* ── 4. group_by retract corner cases ──────────────────────── *)
  Printf.printf "\n-- 4. group_by retract corners\n";
  (* group_by с retractable inner → retractable *)
  let gb_count = Agg.group_by ~key:(fun (k,_) -> k) ~inner:Agg.count in
  check "group_by(count) retractable" (Agg.is_retractable gb_count);
  (* group_by с НЕ-retractable inner → НЕ retractable *)
  let gb_min = Agg.group_by ~key:(fun (k,_) -> k)
                 ~inner:(Agg.contramap snd (Agg.min_by (fun x -> float x))) in
  check "group_by(min_by) NOT retractable" (not (Agg.is_retractable gb_min));
  (* retract элемента несуществующей группы — игнорируется (no crash) *)
  (match run_remove gb_count [("a",1);("a",2)] ("zzz", 99) with
   | Some result ->
     let a_count = List.assoc_opt "a" result in
     check "group_by retract unknown group: 'a' still 2"
       (a_count = Some 2)
   | None -> fail "group_by(count) should be retractable");

  Printf.printf "\nAgg retract/error coverage passed.\n"
