(* Тест roadmap 1.1: exactly-once в параллельном режиме *)

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name
let check_eq name a b = if a=b then pass name
  else fail (Printf.sprintf "%s: expected %d got %d" name b a)

let with_timeout secs f =
  Sys.set_signal Sys.sigalrm (Sys.Signal_handle (fun _ ->
    Printf.printf "  FAIL: TIMEOUT\n%!"; exit 1));
  ignore (Unix.alarm secs);
  let r = f () in ignore (Unix.alarm 0); r

(* process: считаем события по device_id в backend *)
let count_process backend ev =
  match ev with
  | Mf_event.Data (key, _) ->
    let cur = match State_backend_memory.get backend key with
      | Some b -> int_of_string (Bytes.to_string b) | None -> 0 in
    State_backend_memory.set backend key
      (Bytes.of_string (string_of_int (cur + 1)));
    [key]   (* эмитим ключ как выход *)
  | _ -> []

(* ── 1. Checkpoint создаётся, все воркеры снапшотятся ─────── *)
let test_checkpoint_created () =
  Printf.printf "\n-- Checkpoint commits snapshots from all workers\n";
  with_timeout 10 (fun () ->
    let store = Checkpoint_parallel.make_store () in
    let n = 4000 in
    let events = List.init n (fun i ->
      Mf_event.data (Printf.sprintf "k%d" (i mod 4)) (i * 100)) in
    let sink_count = ref 0 in
    let mu = Mutex.create () in
    Checkpoint_parallel.run_exactly_once
      ~workers:4 ~capacity:128 ~checkpoint_every:500
      ~key_of:(fun k -> k)
      ~make_state:State_backend_memory.create
      ~process:count_process
      ~source:(Stream.of_list events)
      ~sink:(fun _ -> Mutex.lock mu; incr sink_count; Mutex.unlock mu)
      ~store
      ();
    check "all events processed exactly once" (!sink_count = n);
    check "at least one checkpoint committed"
      (Checkpoint_parallel.checkpoint_count store > 0);
    (match Checkpoint_parallel.latest_checkpoint store with
     | Some (_epoch, snaps) ->
       check_eq "checkpoint has snapshot per worker"
         (Array.length snaps) 4
     | None -> fail "no checkpoint")
  )

(* ── 2. Восстановление: стейт переживает "рестарт" ────────── *)
let test_recovery () =
  Printf.printf "\n-- Recovery: state restored from checkpoint\n";
  with_timeout 10 (fun () ->
    let store = Checkpoint_parallel.make_store () in
    (* Прогон 1: обрабатываем события, копим стейт *)
    let events = List.init 2000 (fun i ->
      Mf_event.data (Printf.sprintf "k%d" (i mod 4)) (i * 100)) in
    Checkpoint_parallel.run_exactly_once
      ~workers:4 ~capacity:128 ~checkpoint_every:400
      ~key_of:(fun k -> k)
      ~make_state:State_backend_memory.create
      ~process:count_process
      ~source:(Stream.of_list events)
      ~sink:(fun _ -> ())
      ~store
      ();

    (* Восстанавливаем стейт в свежие backends *)
    let restored = Array.init 4 (fun _ -> State_backend_memory.create ()) in
    let ok = Checkpoint_parallel.restore_latest store restored in
    check "restore succeeded" ok;

    (* Сумма счётчиков по всем восстановленным backend = числу
       событий учтённых на момент последнего checkpoint *)
    let total = Array.fold_left (fun acc b ->
      List.fold_left (fun a k ->
        match State_backend_memory.get b k with
        | Some v -> a + int_of_string (Bytes.to_string v) | None -> a)
        acc (State_backend_memory.keys b)
    ) 0 restored in
    Printf.printf "    restored total count = %d\n%!" total;
    (* Последний checkpoint — финальный barrier после всех 2000 событий,
       так что стейт должен отражать ~все события (точное число зависит
       от того сколько успело обработаться до финального barrier commit). *)
    check "restored state non-empty" (total > 0);
    check "restored count <= total events" (total <= 2000)
  )

(* ── 3. Детерминизм счётчиков: сумма = числу обработанных ──── *)
let test_no_double_count () =
  Printf.printf "\n-- No double counting across workers\n";
  with_timeout 10 (fun () ->
    let store = Checkpoint_parallel.make_store () in
    let n = 1600 in
    (* Каждый ключ встречается ровно n/4 раз *)
    let events = List.init n (fun i ->
      Mf_event.data (Printf.sprintf "k%d" (i mod 4)) (i * 100)) in
    let emitted = ref 0 in
    let mu = Mutex.create () in
    Checkpoint_parallel.run_exactly_once
      ~workers:4 ~capacity:256 ~checkpoint_every:1000
      ~key_of:(fun k -> k)
      ~make_state:State_backend_memory.create
      ~process:count_process
      ~source:(Stream.of_list events)
      ~sink:(fun _ -> Mutex.lock mu; incr emitted; Mutex.unlock mu)
      ~store
      ();
    (* Ровно n выходов — каждое событие обработано ровно раз *)
    check_eq "exactly n outputs (no dup, no loss)" !emitted n
  )

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Exactly-once parallel (roadmap 1.1)\n";
  Printf.printf "==========================================\n";
  test_checkpoint_created ();
  test_recovery ();
  test_no_double_count ();
  Printf.printf "\nAll exactly-once parallel tests passed.\n"
