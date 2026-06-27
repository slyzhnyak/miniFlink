(** Property-инвариант для Pipe.enrich (left join со справочной
    таблицей).

    enrich — частая операция (обогащение события данными из справочника
    по ключу), но имела только example-тесты. Инвариант чёткий:

    Для каждого входного события enrich применяет merge с найденным в
    таблице значением (или None, если ключа нет), НЕ меняя порядок и
    число событий. Здесь значение события — (key, payload), таблица —
    отображение key -> bonus, а merge складывает bonus в payload. Тогда
    выход обязан совпасть с независимым oracle, который делает тот же
    lookup по той же assoc-карте.

    Свойства, которые это проверяет разом:
    - left join: нет значения → merge получает None (payload не
      меняется);
    - сохранение длины: одно входное Data → одно выходное;
    - порядок не нарушается. *)

open Miniflink
open QCheck

(* событие: (key, payload). Ключи из небольшого набора, чтобы и
   попадания, и промахи по таблице реально случались. *)
module ByKey : Keyed.S with type t = (string * int) = struct
  type t = string * int
  let key (k, _) = k
end

let keys = [| "A"; "B"; "C"; "D" |]   (* D не кладём в таблицу — промахи *)

let gen_events : (string * int) list Gen.t =
  let open Gen in
  list_size (int_range 0 30)
    (pair (map (fun i -> keys.(i)) (int_range 0 3)) (int_range 0 100))

(* таблица: подмножество ключей A..C с бонусами (D отсутствует) *)
let gen_table : (string * int) list Gen.t =
  let open Gen in
  let* a = int_range 0 9 in
  let* b = int_range 0 9 in
  let* c = int_range 0 9 in
  return [ ("A", a); ("B", b); ("C", c) ]

let arb = make
  ~print:(fun (evs, tbl) ->
    Printf.sprintf "events=%d, table=[%s]" (List.length evs)
      (String.concat ";" (List.map (fun (k,v) -> Printf.sprintf "%s=%d" k v) tbl)))
  (Gen.pair gen_events gen_table)

(* merge: прибавить bonus к payload; None → payload без изменений *)
let merge (k, p) bonus = match bonus with
  | Some b -> (k, p + b)
  | None -> (k, p)

let run_enrich evs tbl_assoc =
  let table = Table.of_list tbl_assoc in
  let stream =
    evs |> List.map (fun e -> Mf_event.data e 0) |> Stream.of_list
    |> Pipe.enrich (module ByKey) ~from:table ~merge in
  let out = ref [] in
  let rec drain () = match stream () with
    | None -> () | Some (Mf_event.Data (v,_)) -> out := v :: !out; drain ()
    | Some _ -> drain ()
  in drain (); List.rev !out

(* oracle: тот же lookup по assoc-карте, тот же merge *)
let oracle evs tbl_assoc =
  List.map (fun (k, p) -> merge (k, p) (List.assoc_opt k tbl_assoc)) evs

let prop_enrich_left_join =
  Test.make ~count:2000 ~name:"enrich: left join = oracle (длина и значения)"
    arb
    (fun (evs, tbl) -> run_enrich evs tbl = oracle evs tbl)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Property: enrich left join\n";
  Printf.printf "==========================================\n";
  QCheck_runner.run_tests_main [ prop_enrich_left_join ]
