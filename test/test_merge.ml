open Miniflink
(* merge_partitioned: слить N watermarked-потоков (партиций) в один,
   ведя per-partition watermark. Общий watermark = минимум по АКТИВНЫМ
   партициям. Idle-стратегия (выбирается при создании) решает, считать ли
   молчащую партицию активной. *)

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let wms_of stream =
  Stream.to_list stream
  |> List.filter_map (function Mf_event.Watermark w -> Some w | _ -> None)

(* ── Never: общий watermark = минимум по ВСЕМ партициям ───── *)
let test_never_takes_min () =
  Printf.printf "\n-- Never: combined watermark is min over all partitions\n";
  (* p0 дошёл до 100, p1 только до 30 → общий wm не превышает 30 *)
  let p0 = Stream.of_list [Mf_event.data 1 10; Mf_event.wm 100] in
  let p1 = Stream.of_list [Mf_event.data 2 20; Mf_event.wm 30] in
  let wms = Merge.merge_partitioned ~idle:Merge.Never [p0; p1] |> wms_of in
  let mx = List.fold_left max min_int wms in
  check "combined watermark capped at slow partition (<=30)" (mx <= 30);
  (* монотонность *)
  let rec mono = function a::(b::_ as r) -> a<=b && mono r | _ -> true in
  check "watermarks monotone" (mono wms)

(* ── Wall_clock_timeout: молчащая партиция исключается ───── *)
(* p1 «молчит» (вообще не даёт watermark после первого), idle timeout
   мал → p1 помечается idle, общий watermark продвигается по p0 *)
let test_wallclock_excludes_idle () =
  Printf.printf "\n-- Wall_clock idle: silent partition excluded, wm advances\n";
  let p0 = Stream.of_list [Mf_event.data 1 10; Mf_event.wm 50; Mf_event.wm 100] in
  let p1 = Stream.of_list [Mf_event.data 2 5] in  (* нет watermark вообще *)
  (* idle_ms=0 → p1 сразу считается idle, не держит минимум *)
  let wms = Merge.merge_partitioned ~idle:(Merge.Wall_clock_timeout 0) [p0; p1] |> wms_of in
  let mx = List.fold_left max min_int wms in
  check "watermark advanced past silent partition (reached 100)" (mx >= 100)

(* ── корректность: пока ОБЕ активны — честный минимум; когда одна
   завершилась, watermark идёт по оставшейся (как в union) ──── *)
let test_both_active_min () =
  Printf.printf "\n-- both active: honest min; finished partition lets wm advance\n";
  let p0 = Stream.of_list [Mf_event.wm 40; Mf_event.wm 80] in
  let p1 = Stream.of_list [Mf_event.wm 60; Mf_event.wm 90] in
  let wms = Merge.merge_partitioned ~idle:Merge.Never [p0; p1] |> wms_of in
  (* пока обе активны, watermark не обгоняет минимум (40, потом 80);
     после завершения p0 оставшаяся p1 доводит до 90 — это корректно
     (p0 исчерпана, данных <90 больше не будет) *)
  let rec mono = function a::(b::_ as r) -> a<=b && mono r | _ -> true in
  check "watermarks monotone" (mono wms);
  check "passes through 80 (min while both active)" (List.mem 80 wms);
  check "final reaches 90 after p0 finished" (List.mem 90 wms);
  check "never exceeds 90" (List.for_all (fun w -> w <= 90) wms)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Partitioned merge with idle strategy\n";
  Printf.printf "==========================================\n";
  test_never_takes_min ();
  test_wallclock_excludes_idle ();
  test_both_active_min ();
  Printf.printf "\nMerge tests passed.\n"
