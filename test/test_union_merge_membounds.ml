(** Границы памяти для union/merge при перекосе скоростей входов
    (review 8.3 #8).

    Ревью отмечало риск неограниченного роста буфера в union при
    «значительно разных скоростях входных потоков». В pull-based модели
    этот рост структурно не возникает: union держит по одному
    «заглянутому» элементу с каждого входа (O(1)) и не тянет из быстрого
    входа быстрее, чем downstream тянет из union. Этот тест ФИКСИРУЕТ это
    свойство — защита от регресса, если union/merge когда-нибудь
    переведут в push-режим.

    Проверяем live_words до и после прогона: при перекосе он не должен
    расти пропорционально числу обработанных событий. *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let live_words () = (Gc.stat ()).Gc.live_words

(* прогнать поток до конца, вернуть прирост live_words *)
let drain_delta produce =
  Gc.full_major ();
  let before = live_words () in
  let s = produce () in
  let cont = ref true and n = ref 0 in
  while !cont do
    (match s () with None -> cont := false | Some _ -> incr n)
  done;
  Gc.full_major ();
  (live_words () - before, !n)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  union/merge memory bounds under skew\n";
  Printf.printf "==========================================\n";

  (* 1. union: один вход — 1 событие далеко в будущем, другой — поток из
     1М событий в прошлом. union эмитит все «прошлые» по одному, держа
     «будущее». Память должна остаться O(1), а не O(числа событий). *)
  Printf.printf "\n-- union: extreme ts skew (1 future vs 1M past)\n";
  let (delta, n) = drain_delta (fun () ->
    let a = Stream.of_list [ Mf_event.data "future" 1_000_000_000 ] in
    let cnt = ref 0 in
    let b () =
      if !cnt >= 1_000_000 then None
      else (incr cnt; Some (Mf_event.data "past" !cnt)) in
    Mf_event.union a b) in
  Printf.printf "    обработано %d событий, прирост live_words=%d\n" n delta;
  (* O(1): прирост должен быть крошечным относительно 1М событий.
     Порог щедрый (10000 слов ≈ 80KB) — но это НЕ ~1M слов. *)
  check "union: память O(1) под перекосом (не растёт с числом событий)"
    (delta < 10_000);

  (* 2. merge_partitioned: 4 партиции, одна «быстрая» (много событий),
     остальные молчат. out-буфер ограничен числом партиций, не потоком. *)
  Printf.printf "\n-- merge_partitioned: one hot partition, others quiet\n";
  let (delta2, n2) = drain_delta (fun () ->
    let hot_cnt = ref 0 in
    let hot () =
      if !hot_cnt >= 500_000 then None
      else (incr hot_cnt;
            Some (Mf_event.data "hot" !hot_cnt)) in
    let quiet1 = Stream.of_list [ Mf_event.wm 10 ] in
    let quiet2 = Stream.of_list [ Mf_event.wm 10 ] in
    let quiet3 = Stream.of_list [ Mf_event.wm 10 ] in
    Merge.merge_partitioned ~now_ms:(fun () -> 0) ~idle:Merge.Never
      [ hot; quiet1; quiet2; quiet3 ]) in
  Printf.printf "    обработано %d событий, прирост live_words=%d\n" n2 delta2;
  check "merge: память O(партиций), не O(потока)"
    (delta2 < 10_000);

  Printf.printf "\nunion/merge memory-bounds tests passed.\n"
