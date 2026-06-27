(** P-02 (roadmap): монотонность watermark через merge_partitioned.

    Самый важный инвариант streaming-системы: watermark, выходящий из
    топологии, НИКОГДА не идёт назад. Если merge эмитит убывающий
    watermark, ломается семантика event-time для всех downstream-окон
    (окно может «закрыться» и тут же получить событие, которое должно
    было в него попасть).

    Существующие merge-тесты проверяли монотонность лишь на нескольких
    подобранных примерах. Здесь — property для ЛЮБЫХ входных партиций:
    N потоков, каждый со своими (неубывающими) watermark'ами и
    случайными перемежающимися Data, в т.ч. сценарий «партиция молчала,
    потом ожила с отстающим временем» (это был класс бага H-2). Выход
    проверяется на строгую неубываемость watermark'ов.

    idle:Never — без wall-clock, чтобы тест был детерминированным:
    общий watermark = минимум по ещё не завершённым партициям. *)

open Miniflink
open QCheck

(* Одна партиция: список событий с НЕубывающими ts. Watermark'и
   вставляются как «отметки прогресса времени». *)
let gen_partition : int Mf_event.t list Gen.t =
  let open Gen in
  sized_size (int_range 0 25) (fun n ->
    let rec build i ts acc =
      if i >= n then return (List.rev acc)
      else
        int_range 0 50 >>= fun dt ->
        let ts' = ts + dt in
        frequency [
          (* Data на текущем времени *)
          (3, int_range 0 100 >>= fun v ->
              build (i+1) ts' (Mf_event.data v ts' :: acc));
          (* Watermark — продвигает время партиции *)
          (2, build (i+1) ts' (Mf_event.wm ts' :: acc));
        ]
    in build 0 0 [])

(* 1..4 партиции *)
let gen_partitions : int Mf_event.t list list Gen.t =
  Gen.(int_range 1 4 >>= fun k -> list_repeat k gen_partition)

let arb_partitions = make ~print:(fun pss ->
  Printf.sprintf "%d партиций, размеры [%s]"
    (List.length pss)
    (String.concat ";" (List.map (fun p -> string_of_int (List.length p)) pss)))
  gen_partitions

(* Прогнать merge, собрать все эмитнутые watermark'и по порядку. *)
let emitted_watermarks pss =
  let streams = List.map Stream.of_list pss in
  (* now_ms фиксирован: idle:Never его не использует, но передаём для
     полной детерминированности *)
  let merged =
    Merge.merge_partitioned ~now_ms:(fun () -> 0) ~idle:Merge.Never streams in
  let wms = ref [] in
  let rec drain () = match merged () with
    | None -> ()
    | Some (Mf_event.Watermark w) -> wms := w :: !wms; drain ()
    | Some _ -> drain ()
  in drain ();
  List.rev !wms

(* Инвариант: последовательность watermark'ов неубывающая. *)
let is_monotone xs =
  let rec go prev = function
    | [] -> true
    | x :: rest -> if x >= prev then go x rest else false
  in go min_int xs

let prop_watermark_monotone =
  Test.make ~count:2000 ~name:"merge: watermark монотонно неубывает"
    arb_partitions
    (fun pss -> is_monotone (emitted_watermarks pss))

let () =
  (* merge предупреждает про завершившиеся без watermark партиции —
     это ожидаемый шум на случайных входах, глушим, чтобы не
     зашумлять вывод property-теста. *)
  Log.set_level Log.Error;
  Printf.printf "==========================================\n";
  Printf.printf "  P-02: watermark monotonicity through merge\n";
  Printf.printf "==========================================\n";
  QCheck_runner.run_tests_main [ prop_watermark_monotone ]
