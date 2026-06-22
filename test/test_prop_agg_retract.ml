(** Property-тесты: retract-консистентность агрегатов.

    Главный инвариант retractable-агрегата: серия add/remove должна
    давать тот же результат, что batch-агрегация ТОЛЬКО оставшихся
    (не отозванных) элементов. Это oracle — независимый «правильный»
    ответ, с которым сравниваем инкрементальный путь оператора.

    Именно этот класс багов был причиной потери late-данных в
    group_by (R5): инкрементальный remove расходился с пересчётом. *)

open Miniflink
open QCheck

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1

(* Прогнать агрегат инкрементально: последовательность операций
   Add x | Remove x, вернуть finish аккумулятора. Использует
   with_parts2 чтобы достать init/add/remove/finish. *)
type op = Add of float | Rem of float

let run_incremental (type r) (agg : (float, r) Agg.t) (ops : op list) : r option =
  Agg.with_parts2 agg { k2 = fun init add remove finish ->
    match remove with
    | None -> None
    | Some rem ->
      let acc = List.fold_left (fun acc -> function
        | Add x -> add acc x
        | Rem x -> rem acc x) (init ()) ops in
      Some (finish acc) }

(* Oracle: какие элементы «остались» после применения ops.
   Add добавляет вхождение, Rem убирает ОДНО вхождение (как multiset). *)
let remaining (ops : op list) : float list =
  let rec apply acc = function
    | [] -> acc
    | Add x :: tl -> apply (x :: acc) tl
    | Rem x :: tl ->
      (* убрать одно вхождение x *)
      let rec del = function
        | [] -> []
        | y :: ys when y = x -> ys
        | y :: ys -> y :: del ys
      in apply (del acc) tl
  in
  List.rev (apply [] ops)

(* Генератор: валидная последовательность ops, где каждый Rem
   отзывает ранее добавленный (и ещё не отозванный) элемент —
   моделирует корректный Retract (нельзя отозвать то, чего нет). *)
let gen_valid_ops : op list Gen.t =
  let open Gen in
  sized_size (int_range 0 40) (fun n ->
    let rec build i live acc =
      if i >= n then return (List.rev acc)
      else
        (* live = мультимножество добавленных и не отозванных *)
        let do_add () =
          float_range (-100.) 100. >>= fun x ->
          build (i+1) (x :: live) (Add x :: acc)
        in
        match live with
        | [] -> do_add ()
        | _ ->
          frequency [
            (3, do_add ());
            (1, (* отозвать случайный из live *)
             int_range 0 (List.length live - 1) >>= fun idx ->
             let x = List.nth live idx in
             let rec del i = function
               | [] -> []
               | y :: ys -> if i = 0 then ys else y :: del (i-1) ys in
             build (i+1) (del idx live) (Rem x :: acc))
          ]
    in
    build 0 [] [])

let arb_ops = make ~print:(fun ops ->
  String.concat " " (List.map (function
    | Add x -> Printf.sprintf "+%.1f" x
    | Rem x -> Printf.sprintf "-%.1f" x) ops)) gen_valid_ops

(* ── Инвариант для конкретного агрегата ──
   incremental(ops) = batch(remaining(ops)), с допуском для float. *)
let close a b = Float.abs (a -. b) < 1e-6

let prop_count =
  Test.make ~count:1000 ~name:"count: add/remove = |remaining|" arb_ops
    (fun ops ->
       match run_incremental Agg.count ops with
       | None -> false
       | Some n -> n = List.length (remaining ops))

let prop_sum =
  Test.make ~count:1000 ~name:"sum: add/remove = sum(remaining)" arb_ops
    (fun ops ->
       match run_incremental (Agg.sum (fun x -> x)) ops with
       | None -> false
       | Some s -> close s (List.fold_left (+.) 0. (remaining ops)))

let prop_mean =
  Test.make ~count:1000 ~name:"mean: add/remove = mean(remaining)" arb_ops
    (fun ops ->
       let rem = remaining ops in
       match run_incremental (Agg.mean (fun x -> x)) ops with
       | None -> false
       | Some result ->
         let oracle =
           if rem = [] then None
           else Some (List.fold_left (+.) 0. rem /. float_of_int (List.length rem)) in
         (match result, oracle with
          | None, None -> true
          | Some a, Some b -> close a b
          | _ -> false))

let prop_count_if =
  Test.make ~count:1000 ~name:"count_if(>0): add/remove = |positive remaining|" arb_ops
    (fun ops ->
       match run_incremental (Agg.count_if (fun x -> x > 0.)) ops with
       | None -> false
       | Some n ->
         n = List.length (List.filter (fun x -> x > 0.) (remaining ops)))

let prop_to_list =
  Test.make ~count:1000 ~name:"to_list: add/remove = remaining (as multiset)" arb_ops
    (fun ops ->
       match run_incremental Agg.to_list ops with
       | None -> false
       | Some lst ->
         List.sort compare lst = List.sort compare (remaining ops))

(* median на immutable list — retractable, тоже oracle-able *)
let prop_median =
  Test.make ~count:1000 ~name:"median: add/remove = median(remaining)" arb_ops
    (fun ops ->
       let rem = remaining ops in
       match run_incremental (Agg.median (fun x -> x)) ops with
       | None -> false
       | Some result ->
         let oracle =
           if rem = [] then None
           else
             let sorted = List.sort compare rem in
             let n = List.length sorted in
             let mid = n / 2 in
             if n mod 2 = 1 then Some (List.nth sorted mid)
             else Some ((List.nth sorted (mid-1) +. List.nth sorted mid) /. 2.)
         in
         (match result, oracle with
          | None, None -> true
          | Some a, Some b -> close a b
          | _ -> false))

(* both(count, sum): оба компонента консистентны *)
let prop_both =
  Test.make ~count:1000 ~name:"both(count,sum): both components consistent" arb_ops
    (fun ops ->
       match run_incremental (Agg.both Agg.count (Agg.sum (fun x -> x))) ops with
       | None -> false
       | Some (n, s) ->
         let rem = remaining ops in
         n = List.length rem && close s (List.fold_left (+.) 0. rem))

(* Дополнительный инвариант: add x; remove x = identity (для любого
   состояния). Проверяем что вставка-затем-отзыв не меняет finish. *)
let prop_add_remove_identity =
  Test.make ~count:1000 ~name:"add x then remove x = identity (sum)" arb_ops
    (fun ops ->
       (* baseline без лишней пары, и с добавленной парой (+5; -5) в конце *)
       let agg = Agg.sum (fun x -> x) in
       let base = run_incremental agg ops in
       let with_pair = run_incremental agg (ops @ [Add 5.; Rem 5.]) in
       match base, with_pair with
       | Some a, Some b -> close a b
       | _ -> false)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Property: Agg retract consistency\n";
  Printf.printf "==========================================\n";
  let suites = [
    prop_count; prop_sum; prop_mean; prop_count_if;
    prop_to_list; prop_median; prop_both; prop_add_remove_identity;
  ] in
  let ok = QCheck_runner.run_tests ~verbose:false suites in
  if ok = 0 then (Printf.printf "\nAll Agg retract properties passed.\n"; exit 0)
  else (Printf.printf "\nSOME PROPERTIES FAILED.\n"; exit 1)
