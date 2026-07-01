(** M-4: property-тест на event-time упорядочивание в {!Mf_event.union}.

    Ревью (M-4) предположило переупорядочивание в краевом случае: когда
    один вход завершается событием с высоким ts, а другой продолжает
    выдавать события с низким ts, union якобы может эмитить high-ts
    раньше low-ts, нарушив event-time порядок.

    Триаж показал, что в коде есть защита (union берёт вход с меньшим ts,
    watermarks — min). Тест проверяет заявленный баг эмпирически:
    генерируем случайные пары отсортированных по ts потоков (в т.ч.
    несбалансированных) и проверяем, что Data на выходе union монотонны
    по ts. Проходит на многих входах → подозрение снимается для потоков,
    каждый из которых сам упорядочен (предусловие union). *)

open Miniflink
open QCheck

let stream_of_sorted pairs =
  pairs
  |> List.sort (fun (t1, _) (t2, _) -> compare t1 t2)
  |> List.map (fun (ts, tag) -> Mf_event.data tag ts)
  |> Stream.of_list

let out_ts stream =
  let acc = ref [] in
  let rec loop () = match stream () with
    | None -> ()
    | Some (Mf_event.Data (_, ts)) -> acc := ts :: !acc; loop ()
    | Some _ -> loop ()
  in loop ();
  List.rev !acc

let is_monotone l =
  let rec go = function
    | a :: (b :: _ as rest) -> a <= b && go rest
    | _ -> true
  in go l

let gen_pairs : (int * string) list Gen.t =
  let open Gen in
  list_size (int_range 0 40)
    (pair (int_range 0 1000) (map string_of_int (int_range 0 99)))

let arb_pairs = make ~print:(Print.list (Print.pair Print.int Print.string)) gen_pairs

let prop_union_monotone =
  Test.make ~count:2000 ~name:"union output monotone in event-time"
    (pair arb_pairs arb_pairs)
    (fun (pa, pb) ->
       let a = stream_of_sorted pa in
       let b = stream_of_sorted pb in
       is_monotone (out_ts (Mf_event.union a b)))

(* прицельно краевой случай M-4: A короткий с ОДНИМ high-ts, B длинный
   со множеством low-ts *)
let gen_lows : int list Gen.t =
  Gen.(list_size (int_range 1 30) (int_range 0 499))
let arb_unbalanced =
  make ~print:(Print.pair Print.int (Print.list Print.int))
    (Gen.pair (Gen.int_range 500 1000) gen_lows)

let prop_union_unbalanced =
  Test.make ~count:2000 ~name:"union monotone: short-high vs long-low"
    arb_unbalanced
    (fun (a_high, b_lows) ->
       let a = stream_of_sorted [(a_high, "A")] in
       let b = stream_of_sorted
         (List.mapi (fun i t -> (t, "b" ^ string_of_int i)) b_lows) in
       is_monotone (out_ts (Mf_event.union a b)))

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  M-4: union event-time ordering (property)\n";
  Printf.printf "==========================================\n";
  let ok = QCheck_base_runner.run_tests ~verbose:true
    [ prop_union_monotone; prop_union_unbalanced ] in
  if ok = 0 then
    Printf.printf "\nM-4: union preserves event-time order on all cases.\n"
  else begin
    Printf.printf "\nM-4: COUNTEREXAMPLE found - union reorders.\n";
    exit 1
  end
