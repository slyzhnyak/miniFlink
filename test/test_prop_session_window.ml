(** Property-инвариант для Pipe.session_window (динамические окна по
    паузам активности).

    session_window сложнее обычных окон: границы определяются разрывами
    во времени, а события-«мосты» сливают соседние сессии. Имел только
    example-тесты. Здесь — свойства для ЛЮБЫХ потоков (один ключ,
    события с неубывающими ts, закрытие в конце потока):

    P1 (сохранение): сумма размеров всех выданных сессий = числу
        входных Data-событий. Ничего не потеряно и не задвоено.

    P2 (gap-разделение внутри сессии): внутри каждой сессии
        последовательные по времени события разделены паузой <= gap.
        (Иначе они должны были бы оказаться в разных сессиях.)

    P3 (число сессий = oracle): количество сессий совпадает с наивным
        разбиением — новая сессия начинается, когда разрыв до
        предыдущего события > gap.

    Используем один ключ "K" и значение = ts, чтобы проверять и состав
    сессий по временам. Закрытие — на конце потока. *)

open Miniflink
open QCheck

module K1 : Keyed.S with type t = (string * int) = struct
  type t = string * int
  let key (k, _) = k
end

(* поток одного ключа: неубывающие ts (как и положено в event-time) *)
let gen_ts_list : int list Gen.t =
  let open Gen in
  sized_size (int_range 0 30) (fun n ->
    let rec build i ts acc =
      if i >= n then return (List.rev acc)
      else int_range 0 800 >>= fun dt ->
        let ts' = ts + dt in
        build (i+1) ts' (ts' :: acc)
    in build 0 0 [])

let gen_gap : int Gen.t = Gen.int_range 1 500

let arb = make
  ~print:(fun (tss, gap) ->
    Printf.sprintf "gap=%d ts=[%s]" gap
      (String.concat ";" (List.map string_of_int tss)))
  (Gen.pair gen_ts_list gen_gap)

(* запустить session_window, собрать сессии как списки ts *)
let run_sessions tss gap =
  let events =
    List.map (fun ts -> Mf_event.data ("K", ts) ts) tss in
  (* watermark в самом конце, заведомо закрывающий все сессии *)
  let last = List.fold_left max 0 tss in
  let events = events @ [ Mf_event.wm (last + gap + 1) ] in
  let stream = Stream.of_list events
    |> Pipe.session_window (module K1) ~gap in
  let out = ref [] in
  let rec drain () = match stream () with
    | None -> ()
    | Some (Mf_event.Data ((_, vs), _)) ->
      out := (List.map snd vs) :: !out; drain ()
    | Some _ -> drain ()
  in drain ();
  List.rev !out

(* oracle: разбить отсортированные ts на сессии по правилу
   «разрыв > gap начинает новую сессию» *)
let oracle_sessions tss gap =
  match List.sort compare tss with
  | [] -> []
  | first :: rest ->
    let (sessions, cur, _) =
      List.fold_left (fun (done_, cur, prev) ts ->
        if ts - prev > gap then (List.rev cur :: done_, [ts], ts)
        else (done_, ts :: cur, ts))
        ([], [first], first) rest in
    List.rev (List.rev cur :: sessions)

let prop_session_count =
  Test.make ~count:2000 ~name:"session_window: число сессий = oracle (gap-разбиение)"
    arb
    (fun (tss, gap) ->
       List.length (run_sessions tss gap) = List.length (oracle_sessions tss gap))

let prop_session_preserves =
  Test.make ~count:2000 ~name:"session_window: сумма размеров сессий = число Data"
    arb
    (fun (tss, gap) ->
       let sizes = List.fold_left (fun a s -> a + List.length s) 0
                     (run_sessions tss gap) in
       sizes = List.length tss)

let prop_session_gap =
  Test.make ~count:2000 ~name:"session_window: внутри сессии соседние ts <= gap"
    arb
    (fun (tss, gap) ->
       List.for_all (fun sess ->
         let sorted = List.sort compare sess in
         let rec chk = function
           | a :: (b :: _ as rest) -> (b - a <= gap) && chk rest
           | _ -> true
         in chk sorted)
         (run_sessions tss gap))

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Property: session_window gap separation\n";
  Printf.printf "==========================================\n";
  Log.set_level Log.Error;
  QCheck_runner.run_tests_main
    [ prop_session_count; prop_session_preserves; prop_session_gap ]
