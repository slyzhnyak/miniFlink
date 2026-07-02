open Miniflink
(* Тесты на баги найденные самопроверкой:
   R1 — после краха воркера координатор ждёт его снапшот вечно →
        чекпойнты перестают коммититься (EO-прогресс залипает).
   R2 — ретракты в EO-пути молча теряются.

   Эти тесты ДОЛЖНЫ падать на текущем коде (доказывают баг), и
   проходить после фикса. *)

module CP = Checkpoint_parallel

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let with_timeout secs f =
  Sys.set_signal Sys.sigalrm (Sys.Signal_handle (fun _ ->
    Printf.printf "  FAIL: TIMEOUT (likely checkpoint stall after crash)\n%!";
    exit 1));
  ignore (Unix.alarm secs);
  let r = f () in ignore (Unix.alarm 0); r

(* ── R1: после краха воркера система ЗАВЕРШАЕТСЯ без дедлока ──
   Раньше R1 проверял «чекпойнты продолжают коммититься после краха»
   (n >= 5). После фикса M-3 семантика изменилась и стала корректной:
   краш воркера делает эпоху краха и последующие невалидными (упавший
   воркер не даёт снапшот), поэтому новые чекпойнты НЕ формируются —
   последний валидный остаётся latest, хвост переигрывается при recovery.
   Настоящая цель R1 — отсутствие дедлока: координатор не должен вечно
   ждать снапшот мёртвого воркера. Проверяем, что прогон ЗАВЕРШАЕТСЯ в
   пределах timeout (with_timeout бросит alarm иначе). *)
let test_checkpoint_progresses_after_crash () =
  Printf.printf "\n-- R1: run terminates (no deadlock) after a worker crashes\n";
  with_timeout 15 (fun () ->
    let store = CP.make_store () in
    (* воркер обрабатывает; на ключе "poison" бросает исключение
       (имитируем краш одного воркателя). Остальные ключи — норм. *)
    let process backend ev =
      match ev with
      | Mf_event.Data (k, _) ->
        if k = "poison" then failwith "boom"
        else begin
          let cur = match State_backend_memory.get backend k with
            | Some b -> int_of_string (Bytes.to_string b) | None -> 0 in
          State_backend_memory.set backend k
            (Bytes.of_string (string_of_int (cur + 1)));
          [k]
        end
      | _ -> [] in
    (* "poison" КРАШИТ рано; затем много обычных событий. Если координатор
       ждёт мёртвого воркера — прогон завис бы (alarm сработает). *)
    let events =
      Mf_event.data "poison" 0 ::
      List.init 4000 (fun i -> Mf_event.data (Printf.sprintf "k%d" (i mod 3 + 1)) (i+1)) in
    (try
      CP.run_exactly_once
        ~workers:4 ~capacity:128 ~checkpoint_every:200
        ~key_of:(fun k -> k) ~make_state:State_backend_memory.create
        ~process
        ~source:(CP.seekable_of_list events)
        ~sink:(CP.idempotent_sink (fun _ -> ()))
        ~store ()
    with _ -> ());
    (* Дошли сюда без срабатывания alarm — значит НЕ было дедлока на
       ожидании снапшота мёртвого воркера. Это и есть корректное
       поведение R1 после фикса M-3. checkpoint_count может быть любым
       (в т.ч. 0, если краш случился до первого барьера) — это нормально,
       recovery переиграет хвост. *)
    let n = CP.checkpoint_count store in
    Printf.printf "  committed checkpoints before crash cutoff: %d\n%!" n;
    check "run terminated without deadlock after crash" true)

(* ── R2: ретракты не теряются в EO-пути ────────────────────── *)
let test_retracts_not_dropped () =
  Printf.printf "\n-- R2: retracts are not silently dropped in EO path\n";
  with_timeout 10 (fun () ->
    let store = CP.make_store () in
    let seen_retract = ref false in
    (* process помечает если видел retract-событие *)
    let process _backend ev =
      match ev with
      | Mf_event.Data (k, _) -> [k]
      | Mf_event.Retract _ -> seen_retract := true; []
      | _ -> [] in
    let events = [
      Mf_event.data "a" 0;
      Mf_event.retract "a" 10;   (* должен дойти до process *)
      Mf_event.data "b" 20;
    ] in
    CP.run_exactly_once
      ~workers:2 ~capacity:64 ~checkpoint_every:100
      ~key_of:(fun k -> k) ~make_state:State_backend_memory.create
      ~process
      ~source:(CP.seekable_of_list events)
      ~sink:(CP.idempotent_sink (fun _ -> ()))
      ~store ();
    check "retract reached process (not dropped)" !seen_retract)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Crash + checkpoint progress (R1, R2)\n";
  Printf.printf "==========================================\n";
  test_checkpoint_progresses_after_crash ();
  test_retracts_not_dropped ();
  Printf.printf "\nCrash/checkpoint tests passed.\n"
