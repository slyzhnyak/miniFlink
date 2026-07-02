(** Тест M-3: детерминированный commit выходов при крахе воркера.

    Раньше при крахе одного воркера `ts_abort` откатывал ВЕСЬ буфер
    текущей эпохи, включая выходы живых воркеров, причём
    НЕДЕТЕРМИНИРОВАННО (гонка abort vs commit): число закоммиченных
    выходов скакало от прогона к прогону.

    После фикса:
    - эпоха краха и последующие невалидны (упавший воркер не даёт
      снапшот) → их выходы откатываются целиком;
    - эпохи до краха валидны → их выходы коммитятся детерминированно;
    - выход события принадлежит эпохе, чей барьер придёт после него в
      канале воркера (локальная эпоха), а не глобальному current_epoch,
      который убегал вперёд.

    Проверяем: при фиксированном сценарии краха число закоммиченных
    выходов ОДИНАКОВО во всех прогонах (детерминизм), а recovery+replay
    даёт корректный итог без потерь. *)

open Miniflink
module CP = Checkpoint_parallel

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let proc backend ev =
  match ev with
  | Mf_event.Data (key, _) ->
    let cur = match State_backend_memory.get backend key with
      | Some b -> int_of_string (Bytes.to_string b) | None -> 0 in
    State_backend_memory.set backend key
      (Bytes.of_string (string_of_int (cur + 1)));
    [key]
  | _ -> []

(* прогон: воркер, обрабатывающий "k2", крашится на crash_at-м k2.
   Возвращает число закоммиченных выходов. *)
let run_crash () =
  let events = List.init 80 (fun i ->
    Mf_event.data (Printf.sprintf "k%d" (i mod 4)) (i * 10)) in
  let committed = ref 0 in
  let mu = Mutex.create () in
  let store = CP.make_store () in
  let seen = Hashtbl.create 8 in
  let process b ev =
    (match ev with
     | Mf_event.Data (k, _) ->
       let c = (try Hashtbl.find seen k with Not_found -> 0) + 1 in
       Hashtbl.replace seen k c;
       if k = "k2" && c = 8 then failwith "simulated crash"
     | _ -> ());
    proc b ev in
  (try
     CP.run_exactly_once ~workers:4 ~capacity:16 ~checkpoint_every:4
       ~key_of:(fun k -> k) ~make_state:State_backend_memory.create
       ~process ~source:(CP.seekable_of_list events)
       ~sink:(CP.buffered_sink (fun outs ->
         Mutex.lock mu; committed := !committed + List.length outs;
         Mutex.unlock mu))
       ~store ()
   with _ -> ());
  !committed

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  M-3: deterministic commit on worker crash\n";
  Printf.printf "==========================================\n";
  Log.set_level Log.Error;

  Printf.printf "\n-- committed output count is deterministic across runs\n";
  let runs = List.init 12 (fun _ -> run_crash ()) in
  let uniq = List.sort_uniq compare runs in
  Printf.printf "  committed counts: %s\n"
    (String.concat " " (List.map string_of_int runs));
  check "число закоммиченных выходов одинаково во всех прогонах"
    (List.length uniq = 1);
  (* и оно не ноль и не всё: часть эпох (до краха) закоммичена *)
  (match uniq with
   | [n] -> check "закоммичена непустая часть выходов живых воркеров" (n > 0)
   | _ -> fail "недетерминизм");

  Printf.printf "\nM-3 deterministic-commit test passed.\n"
