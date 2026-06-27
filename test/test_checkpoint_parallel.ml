open Miniflink
(* Тесты end-to-end exactly-once: offset + 2PC sink + recovery + durable *)

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

let count_process backend ev =
  match ev with
  | Mf_event.Data (key, _) ->
    let cur = match State_backend_memory.get backend key with
      | Some b -> int_of_string (Bytes.to_string b) | None -> 0 in
    State_backend_memory.set backend key
      (Bytes.of_string (string_of_int (cur + 1)));
    [key]
  | _ -> []

let mk_events n =
  List.init n (fun i -> Mf_event.data (Printf.sprintf "k%d" (i mod 4)) (i * 100))

(* ── 1. Checkpoint содержит offset источника ──────────────── *)
let test_checkpoint_has_offset () =
  Printf.printf "\n-- Checkpoint records source offset\n";
  with_timeout 10 (fun () ->
    let store = Checkpoint_parallel.make_store () in
    let events = mk_events 4000 in
    let emitted = ref 0 in
    let mu = Mutex.create () in
    Checkpoint_parallel.run_exactly_once
      ~workers:4 ~capacity:128 ~checkpoint_every:500
      ~key_of:(fun k -> k)
      ~make_state:State_backend_memory.create
      ~process:count_process
      ~source:(Checkpoint_parallel.seekable_of_list events)
      ~sink:(Checkpoint_parallel.idempotent_sink
               (fun _ -> Mutex.lock mu; incr emitted; Mutex.unlock mu))
      ~store
      ();
    check "exactly n outputs" (!emitted = 4000);
    (match Checkpoint_parallel.latest_checkpoint store with
     | Some cp ->
       check "checkpoint offset > 0" (cp.Checkpoint_parallel.cp_offset > 0);
       check_eq "snapshot per worker"
         (Array.length cp.Checkpoint_parallel.cp_snapshots) 4
     | None -> fail "no checkpoint")
  )

(* ── 2. Recovery: offset + state восстанавливаются ────────── *)
let test_recovery_with_offset () =
  Printf.printf "\n-- Recovery restores state AND seeks source to offset\n";
  with_timeout 10 (fun () ->
    let store = Checkpoint_parallel.make_store () in
    let events = mk_events 2000 in
    Checkpoint_parallel.run_exactly_once
      ~workers:4 ~capacity:128 ~checkpoint_every:400
      ~key_of:(fun k -> k)
      ~make_state:State_backend_memory.create
      ~process:count_process
      ~source:(Checkpoint_parallel.seekable_of_list events)
      ~sink:(Checkpoint_parallel.idempotent_sink (fun _ -> ()))
      ~store
      ();
    let src2 = Checkpoint_parallel.seekable_of_list events in
    let backends = Checkpoint_parallel.recover
      ~workers:4 ~make_state:State_backend_memory.create
      ~source:src2 store in
    let cp = Option.get (Checkpoint_parallel.latest_checkpoint store) in
    check_eq "source seeked to checkpoint offset"
      (src2.Checkpoint_parallel.position ()) cp.Checkpoint_parallel.cp_offset;
    let total = Array.fold_left (fun acc b ->
      List.fold_left (fun a k -> match State_backend_memory.get b k with
        | Some v -> a + int_of_string (Bytes.to_string v) | None -> a)
        acc (State_backend_memory.keys b)) 0 backends in
    check "restored state non-empty" (total > 0);
    (* Корректный инвариант exactly-once: восстановленное состояние =
       числу Data в префиксе потока [0, cp_offset). Раньше тут стояло
       total = cp_offset, что держалось лишь на баге §4.1. *)
    let arr = Array.of_list events in
    let data_in_prefix = let c = ref 0 in
      for i = 0 to min cp.Checkpoint_parallel.cp_offset (Array.length arr) - 1 do
        (match arr.(i) with Mf_event.Data _ -> incr c | _ -> ()) done; !c in
    check "restored count = Data in prefix [0, cp_offset)"
      (total = data_in_prefix)
  )

(* ── 3. Транзакционный sink: commit делает видимым ────────── *)
let test_transactional_sink () =
  Printf.printf "\n-- Transactional (buffered) sink: visible only after commit\n";
  with_timeout 10 (fun () ->
    let store = Checkpoint_parallel.make_store () in
    let events = mk_events 1600 in
    let published = ref 0 in
    let mu = Mutex.create () in
    let sink = Checkpoint_parallel.buffered_sink
      (fun batch -> Mutex.lock mu; published := !published + List.length batch;
                    Mutex.unlock mu) in
    Checkpoint_parallel.run_exactly_once
      ~workers:4 ~capacity:256 ~checkpoint_every:400
      ~key_of:(fun k -> k)
      ~make_state:State_backend_memory.create
      ~process:count_process
      ~source:(Checkpoint_parallel.seekable_of_list events)
      ~sink ~store ();
    check_eq "all outputs published after commits" !published 1600
  )

(* ── 4. Durable storage: checkpoint переживает рестарт ────── *)
let test_durable_storage () =
  Printf.printf "\n-- Durable checkpoint survives process restart\n";
  with_timeout 10 (fun () ->
    let dir = Printf.sprintf "/tmp/mf_cp_%d_%d" (Unix.getpid ()) (Random.int 1000000) in
    let events = mk_events 2000 in
    let store1 = Checkpoint_parallel.durable_store ~dir in
    Checkpoint_parallel.run_exactly_once
      ~workers:4 ~capacity:128 ~checkpoint_every:400
      ~key_of:(fun k -> k)
      ~make_state:State_backend_memory.create
      ~process:count_process
      ~source:(Checkpoint_parallel.seekable_of_list events)
      ~sink:(Checkpoint_parallel.idempotent_sink (fun _ -> ()))
      ~store:store1 ();
    let cp1 = Option.get (Checkpoint_parallel.latest_checkpoint store1) in
    let store2 = Checkpoint_parallel.load_durable ~dir in
    (match Checkpoint_parallel.latest_checkpoint store2 with
     | Some cp2 ->
       check_eq "epoch survived restart"
         cp2.Checkpoint_parallel.cp_epoch cp1.Checkpoint_parallel.cp_epoch;
       check_eq "offset survived restart"
         cp2.Checkpoint_parallel.cp_offset cp1.Checkpoint_parallel.cp_offset;
       check "snapshots survived"
         (Array.length cp2.Checkpoint_parallel.cp_snapshots = 4)
     | None -> fail "durable checkpoint not loaded after restart")
  )

(* ── 5. Полный цикл: recover → продолжение без дублей ─────── *)
let test_no_duplicate_after_recovery () =
  Printf.printf "\n-- End-to-end: recovery continues without duplicates\n";
  with_timeout 10 (fun () ->
    let store = Checkpoint_parallel.make_store () in
    let events = mk_events 2000 in
    Checkpoint_parallel.run_exactly_once
      ~workers:4 ~capacity:128 ~checkpoint_every:400
      ~key_of:(fun k -> k)
      ~make_state:State_backend_memory.create
      ~process:count_process
      ~source:(Checkpoint_parallel.seekable_of_list events)
      ~sink:(Checkpoint_parallel.idempotent_sink (fun _ -> ()))
      ~store ();
    let src2 = Checkpoint_parallel.seekable_of_list events in
    let backends = Checkpoint_parallel.recover
      ~workers:4 ~make_state:State_backend_memory.create ~source:src2 store in
    let rec drain () = match src2.Checkpoint_parallel.pull () with
      | None -> ()
      | Some ev ->
        (match ev with Mf_event.Data (k,_) ->
          let sh = Checkpoint_parallel.hash_key k 4 in
          ignore (count_process backends.(sh) ev) | _ -> ());
        drain ()
    in drain ();
    let total = Array.fold_left (fun acc b ->
      List.fold_left (fun a k -> match State_backend_memory.get b k with
        | Some v -> a + int_of_string (Bytes.to_string v) | None -> a)
        acc (State_backend_memory.keys b)) 0 backends in
    check_eq "total after recovery+replay = n (no duplicates)" total 2000
  )

let () =
  Random.self_init ();
  Printf.printf "==========================================\n";
  Printf.printf "  End-to-end exactly-once (offset+2PC+recovery)\n";
  Printf.printf "==========================================\n";
  test_checkpoint_has_offset ();
  test_recovery_with_offset ();
  test_transactional_sink ();
  test_durable_storage ();
  test_no_duplicate_after_recovery ();
  Printf.printf "\nAll exactly-once tests passed.\n"
