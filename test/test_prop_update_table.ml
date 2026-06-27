(** Property-инварианты для Pipe.update_table (наполнение справочника из
    проходящего потока).

    update_table мутирует внешний Hashtbl значениями из потока и
    пробрасывает события насквозь. Имел только example-тесты. Два
    инварианта для ЛЮБЫХ потоков:

    P1 (last-write-wins): после прохода всего потока table[k] равно
        ПОСЛЕДНЕМУ Data-значению с этим ключом (последняя запись
        побеждает). Сверяется с oracle.

    P2 (passthrough): выход равен входу по Data-значениям и их порядку —
        update_table не теряет, не дублирует и не меняет события, только
        наблюдает их в таблицу. *)

open Miniflink
open QCheck

let keys = [| "A"; "B"; "C" |]

(* поток событий (key, payload) *)
let gen_events : (string * int) list Gen.t =
  let open Gen in
  list_size (int_range 0 30)
    (pair (map (fun i -> keys.(i)) (int_range 0 2)) (int_range 0 100))

let arb = make
  ~print:(fun evs -> Printf.sprintf "n=%d" (List.length evs))
  gen_events

let run evs =
  let tbl : (string, string * int) Hashtbl.t = Hashtbl.create 8 in
  let stream =
    evs |> List.map (fun e -> Mf_event.data e 0) |> Stream.of_list
    |> Pipe.update_table tbl ~key:(fun (k, _) -> k) in
  let out = ref [] in
  let rec drain () = match stream () with
    | None -> () | Some (Mf_event.Data (v, _)) -> out := v :: !out; drain ()
    | Some _ -> drain ()
  in drain ();
  (tbl, List.rev !out)

(* oracle для P1: последнее значение на ключ в порядке потока *)
let oracle_last evs =
  let tbl = Hashtbl.create 8 in
  List.iter (fun (k, v) -> Hashtbl.replace tbl k (k, v)) evs;
  tbl

let prop_last_write_wins =
  Test.make ~count:2000 ~name:"update_table: table[k] = последнее значение k"
    arb
    (fun evs ->
       let (tbl, _) = run evs in
       let expect = oracle_last evs in
       List.for_all (fun k ->
         Hashtbl.find_opt tbl k = Hashtbl.find_opt expect k)
         [ "A"; "B"; "C" ])

let prop_passthrough =
  Test.make ~count:2000 ~name:"update_table: выход = вход (passthrough)"
    arb
    (fun evs ->
       let (_, out) = run evs in
       out = evs)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Property: update_table\n";
  Printf.printf "==========================================\n";
  QCheck_runner.run_tests_main [ prop_last_write_wins; prop_passthrough ]
