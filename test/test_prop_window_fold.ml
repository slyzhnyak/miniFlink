(** Property-инвариант для Pipe.window_fold (инкрементальная оконная
    свёртка).

    window_fold — основа оконной агрегации (window_agg построен поверх
    него), инкрементально сворачивает события окна через ~add. Имел
    только example-тесты. Золотой инвариант оконных операторов:

    инкрементальный результат каждого окна = прямой fold (add) по тем
    событиям, что попали в это окно.

    Проверяем на tumbling-окнах фиксированной ширины: событие с
    временем ts принадлежит окну [w*ts/w, ...), т.е. бакету ts / width.
    Oracle независимо группирует события по (ключ, бакет) и
    сворачивает add'ом из init. Сверяем множество выданных (key, acc).

    add = сумма значений; значение = полезная нагрузка события. Это
    тот же класс инварианта, что уже ловил ошибки в агрегатах с
    retract. *)

open Miniflink
open QCheck

module K1 : Keyed.S with type t = (string * int * int) = struct
  type t = string * int * int          (* (key, ts, value) *)
  let key (k, _, _) = k
end

let keys = [| "A"; "B" |]

(* поток: события с неубывающими ts, на 1-2 ключа *)
let gen_events : (string * int * int) list Gen.t =
  let open Gen in
  sized_size (int_range 0 30) (fun n ->
    let rec build i ts acc =
      if i >= n then return (List.rev acc)
      else
        int_range 0 40 >>= fun dt ->
        let ts' = ts + dt in
        int_range 0 1 >>= fun ki ->
        int_range 0 100 >>= fun v ->
        build (i+1) ts' ((keys.(ki), ts', v) :: acc)
    in build 0 0 [])

let gen_width : int Gen.t = Gen.int_range 1 50

let arb = make
  ~print:(fun (evs, w) ->
    Printf.sprintf "width=%d n=%d" w (List.length evs))
  (Gen.pair gen_events gen_width)

let width = ref 10   (* подменяется в проверке *)

let run_fold evs w =
  let events =
    List.map (fun (k,ts,v) -> Mf_event.data (k,ts,v) ts) evs in
  (* watermark в конце закрывает все окна *)
  let last = List.fold_left (fun m (_,ts,_) -> max m ts) 0 evs in
  let stream =
    Stream.of_list (events @ [ Mf_event.wm (last + w + 1) ])
    |> Pipe.window_fold (module K1) (Pipe.tumbling w)
         ~init:(fun () -> 0)
         ~add:(fun acc (_,_,v) -> acc + v) in
  let out = ref [] in
  let rec drain () = match stream () with
    | None -> ()
    | Some (Mf_event.Data ((k, acc), _)) -> out := (k, acc) :: !out; drain ()
    | Some _ -> drain ()
  in drain ();
  List.sort compare !out

(* oracle: группируем по (key, бакет = ts / w), суммируем value,
   возвращаем непустые окна как (key, sum) *)
let oracle evs w =
  let tbl = Hashtbl.create 16 in
  List.iter (fun (k, ts, v) ->
    let bucket = ts / w in
    let cur = try Hashtbl.find tbl (k, bucket) with Not_found -> 0 in
    Hashtbl.replace tbl (k, bucket) (cur + v)) evs;
  Hashtbl.fold (fun (k,_) sum acc -> (k, sum) :: acc) tbl []
  |> List.sort compare

let prop_window_fold_recompute =
  Test.make ~count:2000
    ~name:"window_fold: incremental = recompute (sum per tumbling window)"
    arb
    (fun (evs, w) ->
       (* мультимножество (key, acc) от инкрементального прогона должно
          совпасть с oracle-группировкой. Сравниваем как
          отсортированные списки сумм по ключам (бакеты разные окна, но
          один и тот же key может дать несколько окон). *)
       let got = run_fold evs w in
       let exp = oracle evs w in
       (* оба — мультимножества (key, sum); сравним отсортированные
          списки сумм, сгруппированные по ключу *)
       List.sort compare got = List.sort compare exp)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Property: window_fold incremental = recompute\n";
  Printf.printf "==========================================\n";
  Log.set_level Log.Error;
  ignore width;
  QCheck_runner.run_tests_main [ prop_window_fold_recompute ]
