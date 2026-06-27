(** Property-инварианты для Temporal (as-of versioned store).

    temporal_join и его ядро [as_of] — один из самых сложных операторов
    (версионирование, выбор по event-time), но имели только
    example-тесты на подобранных точках. Здесь — свойства для ЛЮБЫХ
    историй версий и запросов.

    P1 (as_of = oracle): [as_of tbl k ts] всегда равно версии с
        максимальным [valid_from <= ts] (или None), независимо от
        порядка вставки. Сверяется с наивным перебором.

    P2 (prune сохраняет видимые ответы): после
        [prune_versions_before ~before] результат [as_of k ts] для всех
        [ts >= before] не меняется. Обрезка освобождает память, но НЕ
        должна менять то, что увидит любой будущий as_of. Это прямой
        контракт из доки prune_versions_before. *)

open Miniflink
open QCheck

(* история: список (key, valid_from, value), вставляется в случайном
   порядке. Ключи и времена из небольшого набора, чтобы коллизии и
   совпадения valid_from реально возникали. *)
let gen_history : (string * int * int) list Gen.t =
  let open Gen in
  let keys = [| "A"; "B"; "C" |] in
  list_size (int_range 0 30)
    (triple (map (fun i -> keys.(i)) (int_range 0 2))
            (int_range 0 50)
            (int_range 0 999))

let arb_history = make ~print:(fun h ->
  "[" ^ String.concat "; "
    (List.map (fun (k,vf,v) -> Printf.sprintf "%s@%d=%d" k vf v) h) ^ "]")
  gen_history

(* Наивный oracle: для (k, ts) выбрать значение версии с наибольшим
   valid_from <= ts. При равных valid_from — та, что вставлена позже
   (как и put_version: последняя запись побеждает). *)
let oracle_as_of (hist : (string*int*int) list) k ts : int option =
  (* проходим в порядке вставки, держим лучшую (max valid_from <= ts);
     при равенстве valid_from — на более позднюю вставку *)
  List.fold_left (fun best (hk, vf, v) ->
    if hk = k && vf <= ts then
      match best with
      | Some (bvf, _) when bvf > vf -> best
      | _ -> Some (vf, v)              (* vf >= bvf: берём (позже вставлено) *)
    else best) None hist
  |> Option.map snd

let build_table hist =
  let tbl = Temporal.create_versioned () in
  List.iter (fun (k, vf, v) ->
    Temporal.put_version tbl ~key:k ~valid_from:vf v) hist;
  tbl

let prop_as_of_oracle =
  Test.make ~count:2000 ~name:"as_of: версия с max valid_from <= ts (= oracle)"
    (pair arb_history (int_range (-5) 55))
    (fun (hist, ts) ->
       let tbl = build_table hist in
       List.for_all (fun k ->
         Temporal.as_of tbl k ts = oracle_as_of hist k ts)
         [ "A"; "B"; "C" ])

let prop_prune_preserves =
  Test.make ~count:2000
    ~name:"prune_versions_before: as_of(ts>=before) не меняется"
    (triple arb_history (int_range 0 50) (int_range 0 55))
    (fun (hist, before, ts_off) ->
       let ts = before + ts_off in          (* гарантированно ts >= before *)
       let tbl1 = build_table hist in
       let before_prune =
         List.map (fun k -> Temporal.as_of tbl1 k ts) [ "A"; "B"; "C" ] in
       let tbl2 = build_table hist in
       Temporal.prune_versions_before tbl2 ~before;
       let after_prune =
         List.map (fun k -> Temporal.as_of tbl2 k ts) [ "A"; "B"; "C" ] in
       before_prune = after_prune)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Property: Temporal as_of / prune\n";
  Printf.printf "==========================================\n";
  QCheck_runner.run_tests_main [ prop_as_of_oracle; prop_prune_preserves ]
