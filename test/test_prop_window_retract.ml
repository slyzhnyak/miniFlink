(** Property-тесты: retract-консистентность window_agg.

    Главный инвариант: для ЛЮБОГО потока Data+Retract, финальное
    значение окна (после закрытия) = batch-агрегат тех элементов,
    что были добавлены и НЕ отозваны в этом окне.

    Это oracle-сравнение: инкрементальный путь оператора (add/remove
    на лету + atomic Update на late) vs независимый пересчёт с нуля.

    Именно расхождение этих двух путей было багом R5 (group_by терял
    late-данные: инкрементальная коррекция выглядела как noop). *)

open Miniflink
open QCheck

module K = struct type t = string * float let key (k,_) = k end

(* Поток событий: каждое — Data или Retract значения в фиксированном
   наборе ключей, с возрастающими (но не строго) ts. Retract всегда
   отзывает ранее добавленное значение в том же ключе. *)
type ev = D of string * float * int | R of string * float * int

(* Генератор валидного потока: ведём «живые» (добавленные, не
   отозванные) элементы per ключ, Retract берём только из живых. *)
let gen_stream : ev list Gen.t =
  let open Gen in
  let keys = [| "A"; "B"; "C" |] in
  sized_size (int_range 0 60) (fun n ->
    let rec build i ts live acc =
      if i >= n then return (List.rev acc)
      else
        int_range 0 200 >>= fun dt ->          (* шаг времени *)
        let ts' = ts + dt in
        let do_add () =
          int_range 0 (Array.length keys - 1) >>= fun ki ->
          float_range (-50.) 50. >>= fun v ->
          let k = keys.(ki) in
          build (i+1) ts' ((k, v, ts') :: live) (D (k, v, ts') :: acc)
        in
        match live with
        | [] -> do_add ()
        | _ ->
          frequency [
            (3, do_add ());
            (1, (* отозвать случайный живой — в его же окне (тот же ts
                   приблизительно: используем исходный ts элемента) *)
             int_range 0 (List.length live - 1) >>= fun idx ->
             let (k, v, orig_ts) = List.nth live idx in
             let rec del i = function
               | [] -> [] | x :: xs -> if i=0 then xs else x :: del (i-1) xs in
             (* retract с тем же ts что и исходный Data → попадёт в то же окно *)
             build (i+1) ts' (del idx live) (R (k, v, orig_ts) :: acc))
          ]
    in
    build 0 0 [] [])

let arb_stream = make ~print:(fun evs ->
  String.concat " " (List.map (function
    | D (k,v,t) -> Printf.sprintf "D(%s,%.1f,%d)" k v t
    | R (k,v,t) -> Printf.sprintf "R(%s,%.1f,%d)" k v t) evs)) gen_stream

(* window size для тестов *)
let win_size = 1000

(* Oracle: для каждого (ключ, окно) — мультимножество оставшихся
   значений (добавлено минус отозвано). Окно = ts / win_size. *)
let oracle_remaining (evs : ev list) : (string * int, float list) Hashtbl.t =
  let tbl : (string * int, float list) Hashtbl.t = Hashtbl.create 16 in
  let win ts = ts / win_size in
  List.iter (fun e ->
    let (k, v, ts, add) = match e with
      | D (k,v,t) -> (k,v,t,true) | R (k,v,t) -> (k,v,t,false) in
    let key = (k, win ts) in
    let cur = try Hashtbl.find tbl key with Not_found -> [] in
    if add then Hashtbl.replace tbl key (v :: cur)
    else
      let rec del = function
        | [] -> [] | x :: xs -> if x = v then xs else x :: del xs in
      Hashtbl.replace tbl key (del cur)
  ) evs;
  tbl

(* Прогнать window_agg, собрать ФИНАЛЬНОЕ значение каждого (ключ,окно).
   Окна эмитят Data на закрытии и Update на late-коррекции; берём
   последнее значение по (ключ, window-stop). *)
let run_window (type r) (agg : (float, r) Agg.t)
    (project : r -> float) (evs : ev list) : (string * int, float) Hashtbl.t =
  let to_event = function
    | D (k,v,ts) -> Mf_event.data (k, v) ts
    | R (k,v,ts) -> Mf_event.retract (k, v) ts in
  (* большой финальный watermark чтобы все окна закрылись *)
  let max_ts = List.fold_left (fun m e ->
    let ts = match e with D(_,_,t) -> t | R(_,_,t) -> t in max m ts) 0 evs in
  let stream_events =
    List.map to_event evs @ [Mf_event.wm (max_ts + win_size * 10)] in
  (* contramap: агрегат работает на float (значение), вход (k,v) *)
  let agg_kv = Agg.contramap snd agg in
  let out =
    stream_events |> Stream.of_list
    |> Pipe.window_agg (module K) ~allowed_lateness:(win_size * 100)
         (Pipe.tumbling win_size) agg_kv in
  let result : (string * int, float) Hashtbl.t = Hashtbl.create 16 in
  let rec drain () = match out () with
    | None -> ()
    | Some (Mf_event.Data ((k, r), stop)) ->
      Hashtbl.replace result (k, (stop-1) / win_size) (project r); drain ()
    | Some (Mf_event.Update { new_value = (k, r); ts = stop; _ }) ->
      Hashtbl.replace result (k, (stop-1) / win_size) (project r); drain ()
    | Some _ -> drain ()
  in drain ();
  result

let close a b = Float.abs (a -. b) < 1e-6

(* Сравнить оператор с oracle для данного агрегата. Только окна с
   непустым остатком (пустые окна оператор не эмитит). *)
let check_against_oracle agg project oracle_fn evs =
  let oracle = oracle_remaining evs in
  let actual = run_window agg project evs in
  Hashtbl.fold (fun key vals ok ->
    if not ok then false
    else if vals = [] then true  (* пустое окно — оператор может не эмитить *)
    else
      let expected = oracle_fn vals in
      match Hashtbl.find_opt actual key with
      | Some got -> close got expected
      | None -> false   (* непустое окно должно быть в выходе *)
  ) oracle true

let prop_window_sum =
  Test.make ~count:500 ~name:"window_agg sum: final = sum(remaining per window)"
    arb_stream
    (fun evs ->
       check_against_oracle (Agg.sum (fun x -> x)) (fun s -> s)
         (List.fold_left (+.) 0.) evs)

let prop_window_count =
  Test.make ~count:500 ~name:"window_agg count: final = |remaining per window|"
    arb_stream
    (fun evs ->
       check_against_oracle Agg.count float_of_int
         (fun vals -> float_of_int (List.length vals)) evs)

let prop_window_mean =
  Test.make ~count:500 ~name:"window_agg mean: final = mean(remaining per window)"
    arb_stream
    (fun evs ->
       (* mean возвращает option; project через Option.value, и oracle
          считает среднее непустого остатка *)
       check_against_oracle (Agg.mean (fun x -> x))
         (function Some m -> m | None -> nan)
         (fun vals -> List.fold_left (+.) 0. vals /. float_of_int (List.length vals))
         evs)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Property: window_agg retract consistency\n";
  Printf.printf "==========================================\n";
  let suites = [ prop_window_sum; prop_window_count; prop_window_mean ] in
  let ok = QCheck_runner.run_tests ~verbose:false suites in
  if ok = 0 then (Printf.printf "\nAll window_agg retract properties passed.\n"; exit 0)
  else (Printf.printf "\nSOME PROPERTIES FAILED.\n"; exit 1)
