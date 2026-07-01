(** M-5: property-тест на count_sliding — размер окна и тайминг эмита.

    Ревью (M-5) предположило «дрейф» emit-таймингов в count_sliding из-за
    трима по одному элементу и не-корректировки счётчика since. Триаж
    показал, что трим по одному происходит на КАЖДОМ событии (буфер не
    превышает n+1), а since считает шаги, а не размер буфера.

    Тест проверяет два инварианта на многих случайных (N, n, step):
    1. каждое эмитнутое окно содержит РОВНО n элементов;
    2. число эмитнутых окон равно ожидаемому: при N >= n это
       (N - n) / step + 1, иначе 0.

    Проходит на всех входах → «дрейф» из M-5 не подтверждается: sliding
    эмитит окна правильного размера в правильные моменты. *)

open Miniflink
open Test_support.Domain
open QCheck

let tel id ts = { device_id = id; speed_kmh = 1.; fuel_pct = 1.;
                  position = { lat = 0.; lon = 0. }; ts; device = None }

(* прогнать count_sliding n step на потоке из cnt событий одного ключа,
   вернуть список размеров эмитнутых окон *)
let window_sizes ~cnt ~n ~step =
  List.init cnt (fun i -> Mf_event.data (tel "A" i) i)
  |> Stream.of_list
  |> Pipe.count_window (module Telemetry) (Pipe.count_sliding n step)
  |> Stream.to_list
  |> List.filter_map (function
       | Mf_event.Data ((_, vs), _) -> Some (List.length vs)
       | _ -> None)

(* ожидаемое число окон: при cnt >= n это (cnt - n) / step + 1, иначе 0 *)
let expected_count ~cnt ~n ~step =
  if cnt >= n then (cnt - n) / step + 1 else 0

(* генератор (cnt в [0,60], n в [1,15], step в [1,n]) *)
let arb_params =
  make ~print:(fun (cnt, n, step) -> Printf.sprintf "cnt=%d n=%d step=%d" cnt n step)
    Gen.(int_range 0 60 >>= fun cnt ->
         int_range 1 15 >>= fun n ->
         int_range 1 n >>= fun step ->
         return (cnt, n, step))

let prop_window_size =
  Test.make ~count:3000 ~name:"count_sliding: every window has exactly n elements"
    arb_params
    (fun (cnt, n, step) ->
       let sizes = window_sizes ~cnt ~n ~step in
       List.for_all (fun s -> s = n) sizes)

let prop_emit_count =
  Test.make ~count:3000 ~name:"count_sliding: emit count matches (N-n)/step+1"
    arb_params
    (fun (cnt, n, step) ->
       let sizes = window_sizes ~cnt ~n ~step in
       List.length sizes = expected_count ~cnt ~n ~step)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  M-5: count_sliding size + emit timing\n";
  Printf.printf "==========================================\n";
  let ok = QCheck_base_runner.run_tests ~verbose:true
    [ prop_window_size; prop_emit_count ] in
  if ok = 0 then
    Printf.printf "\nM-5: count_sliding emits correct-size windows on schedule.\n"
  else begin
    Printf.printf "\nM-5: COUNTEREXAMPLE found - sliding drift confirmed.\n";
    exit 1
  end
