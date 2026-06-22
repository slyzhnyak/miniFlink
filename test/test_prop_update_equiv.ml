(** Property-тесты: эквивалентность Update ≡ Retract(old)+Data(new).

    Фундаментальный инвариант атомарных Update-событий: обработка
    [Update old→new] оператором должна давать тот же downstream-
    эффект, что последовательность [Retract old; Data new] с тем же
    ts. Если это держится для всех операторов, вся Update-машинерия
    консистентна (Update — лишь атомарная оптимизация пары).

    Проверяем для stateless-комбинаторов (map/filter/flat_map), где
    эквивалентность должна быть точной по мультимножеству выходных
    событий. *)

open Miniflink
open QCheck

(* Нормализация выходного события в сравнимую форму. Update
   нормализуем В пару (Retract old, Data new), чтобы сравнивать
   мультимножества независимо от того, эмитил оператор атомарный
   Update или раздельную пару. *)
type norm = NData of int | NRetract of int | NWm of int

let normalize evs =
  List.concat_map (function
    | Mf_event.Data (v, _) -> [NData v]
    | Mf_event.Retract (v, _) -> [NRetract v]
    | Mf_event.Update { old; new_value; _ } -> [NRetract old; NData new_value]
    | Mf_event.Watermark w -> [NWm w]
  ) evs |> List.sort compare

let drain s =
  let acc = ref [] in
  let rec go () = match s () with None -> () | Some e -> acc := e :: !acc; go () in
  go (); List.rev !acc

(* Генератор потока с Update-событиями (и Data/Retract/Wm).
   Retract/Update old отзывают ранее «живое» значение. *)
type ev = D of int | R of int | U of int * int | W of int

let gen_stream : ev list Gen.t =
  let open Gen in
  sized_size (int_range 0 40) (fun n ->
    let rec build i live acc =
      if i >= n then return (List.rev acc)
      else
        let do_add () =
          int_range 0 30 >>= fun v -> build (i+1) (v :: live) (D v :: acc) in
        match live with
        | [] -> frequency [ (4, do_add ());
                            (1, int_range 0 1000 >>= fun w -> build (i+1) live (W w :: acc)) ]
        | _ ->
          let pick () = List.nth live (Random.int (List.length live)) in
          frequency [
            (4, do_add ());
            (2, (* Update: old из live → new произвольный *)
             int_range 0 (List.length live - 1) >>= fun idx ->
             int_range 0 30 >>= fun nv ->
             let old = List.nth live idx in
             let rec del i = function [] -> [] | x::xs -> if i=0 then xs else x :: del (i-1) xs in
             build (i+1) (nv :: del idx live) (U (old, nv) :: acc));
            (1, (* Retract из live *)
             int_range 0 (List.length live - 1) >>= fun idx ->
             let old = List.nth live idx in
             let rec del i = function [] -> [] | x::xs -> if i=0 then xs else x :: del (i-1) xs in
             build (i+1) (del idx live) (R old :: acc));
            (1, int_range 0 1000 >>= fun w -> ignore (pick ()); build (i+1) live (W w :: acc));
          ]
    in build 0 [] [])

let arb_stream = make ~print:(fun evs ->
  String.concat " " (List.map (function
    | D v -> Printf.sprintf "D%d" v | R v -> Printf.sprintf "R%d" v
    | U (o,n) -> Printf.sprintf "U%d→%d" o n | W w -> Printf.sprintf "W%d" w) evs))
  gen_stream

(* Построить два потока: один с атомарными Update, другой с Update,
   развёрнутым в Retract+Data пару. Оба должны дать одинаковый
   нормализованный выход. *)
let to_atomic ts_of evs =
  List.mapi (fun i e -> match e with
    | D v -> Mf_event.data v (ts_of i)
    | R v -> Mf_event.retract v (ts_of i)
    | U (o,n) -> Mf_event.update o n (ts_of i)
    | W w -> Mf_event.wm w) evs

let to_expanded ts_of evs =
  List.concat (List.mapi (fun i e -> match e with
    | D v -> [Mf_event.data v (ts_of i)]
    | R v -> [Mf_event.retract v (ts_of i)]
    | U (o,n) -> [Mf_event.retract o (ts_of i); Mf_event.data n (ts_of i)]
    | W w -> [Mf_event.wm w]) evs)

let ts_of i = i * 10

(* Инвариант для оператора op: normalize(op(atomic)) = normalize(op(expanded)).
   op — функция над Stream. *)
let equivalence_holds op evs =
  let lhs = to_atomic ts_of evs |> Stream.of_list |> op |> drain |> normalize in
  let rhs = to_expanded ts_of evs |> Stream.of_list |> op |> drain |> normalize in
  lhs = rhs

let prop_map =
  Test.make ~count:1000 ~name:"map: Update ≡ Retract+Data" arb_stream
    (fun evs -> equivalence_holds (Pipe.map (fun x -> x * 2 + 1)) evs)

let prop_filter =
  Test.make ~count:1000 ~name:"filter: Update ≡ Retract+Data" arb_stream
    (fun evs -> equivalence_holds (Pipe.filter (fun x -> x mod 2 = 0)) evs)

let prop_filter_high =
  Test.make ~count:1000 ~name:"filter(>15): Update ≡ Retract+Data" arb_stream
    (fun evs -> equivalence_holds (Pipe.filter (fun x -> x > 15)) evs)

let prop_flat_map =
  Test.make ~count:1000 ~name:"flat_map: Update ≡ Retract+Data" arb_stream
    (fun evs -> equivalence_holds (Pipe.flat_map (fun x -> [x; x + 100])) evs)

let prop_map_then_filter =
  Test.make ~count:1000 ~name:"map>>filter: Update ≡ Retract+Data" arb_stream
    (fun evs -> equivalence_holds
        (fun s -> s |> Pipe.map (fun x -> x + 5) |> Pipe.filter (fun x -> x < 25)) evs)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Property: Update ≡ Retract+Data\n";
  Printf.printf "==========================================\n";
  let suites = [ prop_map; prop_filter; prop_filter_high;
                 prop_flat_map; prop_map_then_filter ] in
  let ok = QCheck_runner.run_tests ~verbose:false suites in
  if ok = 0 then (Printf.printf "\nAll Update-equivalence properties passed.\n"; exit 0)
  else (Printf.printf "\nSOME PROPERTIES FAILED.\n"; exit 1)
