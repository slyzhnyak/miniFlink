(** Property-инвариант для назначения sliding-окон (Pipe.assign).

    Sliding-окна перекрываются (step < size), и логика «в какие окна
    попадает событие с временем ts» — классический источник off-by-one
    ошибок. assign экспонирована именно для тестирования этой логики;
    здесь — свойства для ЛЮБЫХ ts/size/step.

    P1 (накрытие): каждое окно (s, s+size), назначенное для ts,
        действительно накрывает ts: s <= ts < s+size.

    P2 (выравнивание): старт каждого окна кратен step (окна
        выровнены от epoch, как во Flink).

    P3 (полнота для ts>=0): число назначенных окон равно числу
        выровненных по step стартов в (ts-size, ts], усечённых снизу
        нулём — независимый пересчёт. Это ловит как пропуск окна, так и
        лишнее. *)

open Miniflink
open QCheck

(* size > 0, step in [1, size] (перекрытие или стык) *)
let gen_spec : (int * int) Gen.t =
  let open Gen in
  let* size = int_range 1 60 in
  let* step = int_range 1 size in
  return (size, step)

let arb = make
  ~print:(fun ((size, step), ts) ->
    Printf.sprintf "size=%d step=%d ts=%d" size step ts)
  (Gen.pair gen_spec (Gen.int_range 0 300))

let windows_for (size, step) ts =
  Pipe.assign (Pipe.sliding size step) ts

let prop_covers =
  Test.make ~count:2000 ~name:"sliding assign: каждое окно накрывает ts"
    arb
    (fun ((size, step), ts) ->
       List.for_all (fun (s, e) -> s <= ts && ts < e)
         (windows_for (size, step) ts))

let prop_aligned =
  Test.make ~count:2000 ~name:"sliding assign: старт окна кратен step"
    arb
    (fun ((size, step), ts) ->
       List.for_all (fun (s, _) -> s mod step = 0)
         (windows_for (size, step) ts))

(* oracle: старты, кратные step, в диапазоне (ts-size, ts], не ниже 0 *)
let oracle_count (size, step) ts =
  let count = ref 0 in
  let s = ref ((ts / step) * step) in
  while !s + size > ts && !s >= 0 do
    incr count;
    s := !s - step
  done;
  !count

let prop_count =
  Test.make ~count:2000 ~name:"sliding assign: число окон = oracle (ts>=0)"
    arb
    (fun ((size, step), ts) ->
       List.length (windows_for (size, step) ts)
       = oracle_count (size, step) ts)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Property: sliding window assignment\n";
  Printf.printf "==========================================\n";
  QCheck_runner.run_tests_main [ prop_covers; prop_aligned; prop_count ]
