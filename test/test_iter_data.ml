(** Тесты для семейства helper'ов {!Pipe.iter_data}, {!Pipe.fold_data},
    {!Pipe.count_data}, {!Pipe.iter_events}. *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Pipe.iter_data / fold_data / count_data\n";
  Printf.printf "==========================================\n";

  (* Тестовый поток: 3 Data, 2 Watermark, 1 Retract *)
  let events = [
    Mf_event.data "a" 0;
    Mf_event.wm 1000;
    Mf_event.data "b" 1500;
    Mf_event.retract "x" 2000;
    Mf_event.data "c" 2500;
    Mf_event.wm 3000;
  ] in

  (* ── 1. iter_data — только Data, в порядке ─────────────── *)
  Printf.printf "\n-- 1. iter_data\n";
  let collected = ref [] in
  events |> Stream.of_list
  |> Pipe.iter_data (fun s -> collected := s :: !collected);
  check (Printf.sprintf "got %s (expect [a;b;c])"
           (String.concat ";" (List.rev !collected)))
    (List.rev !collected = ["a"; "b"; "c"]);

  (* ── 2. fold_data — сумма длин строк ───────────────────── *)
  Printf.printf "\n-- 2. fold_data\n";
  let total_len =
    events |> Stream.of_list
    |> Pipe.fold_data ~init:0 ~f:(fun acc s -> acc + String.length s) in
  check (Printf.sprintf "sum of lengths = %d (expect 3)" total_len)
    (total_len = 3);

  (* ── 3. count_data — игнорирует Watermark и Retract ────── *)
  Printf.printf "\n-- 3. count_data\n";
  let n = events |> Stream.of_list |> Pipe.count_data in
  check (Printf.sprintf "count = %d (expect 3)" n) (n = 3);

  (* ── 4. iter_events — видит ВСЕ события ────────────────── *)
  Printf.printf "\n-- 4. iter_events\n";
  let data_count = ref 0 in
  let wm_count = ref 0 in
  let retract_count = ref 0 in
  events |> Stream.of_list
  |> Pipe.iter_events (function
    | Mf_event.Data _    -> incr data_count
    | Mf_event.Watermark _ -> incr wm_count
    | Mf_event.Retract _ -> incr retract_count
    | Mf_event.Update _ -> incr retract_count);
  check (Printf.sprintf "data=%d wm=%d retract=%d"
           !data_count !wm_count !retract_count)
    (!data_count = 3 && !wm_count = 2 && !retract_count = 1);

  (* ── 5. Пустой поток ──────────────────────────────────── *)
  Printf.printf "\n-- 5. Empty stream\n";
  let acc = ref 0 in
  Stream.empty |> Pipe.iter_data (fun _ -> incr acc);
  check "iter_data on empty does nothing" (!acc = 0);
  check "count_data on empty = 0"
    (Pipe.count_data Stream.empty = 0);
  check "fold_data on empty = init"
    (Pipe.fold_data ~init:42 ~f:(fun a _ -> a + 1) Stream.empty = 42);

  (* ── 6. Equivalence: collect ≡ accumulate via iter_data ─ *)
  Printf.printf "\n-- 6. iter_data vs collect equivalence\n";
  let via_collect = events |> Stream.of_list |> Pipe.collect in
  let via_iter = ref [] in
  events |> Stream.of_list
  |> Pipe.iter_data (fun v -> via_iter := v :: !via_iter);
  check "iter_data accumulated = collect"
    (List.rev !via_iter = via_collect);

  Printf.printf "\nAll iter_data family tests passed.\n"
