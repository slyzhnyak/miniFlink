(* Тест queue-depth хука наблюдаемости. Библиотека сообщает глубину
   входных каналов через ?on_queue_depth; что с ней делать — дело
   вызывающего. Проверяем что хук вызывается с массивом по числу
   воркеров и значения осмысленны. *)

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let with_timeout secs f =
  Sys.set_signal Sys.sigalrm (Sys.Signal_handle (fun _ ->
    Printf.printf "  FAIL: TIMEOUT\n%!"; exit 1));
  ignore (Unix.alarm secs);
  let r = f () in ignore (Unix.alarm 0); r

let test_hook_invoked () =
  Printf.printf "\n-- on_queue_depth invoked with per-worker array\n";
  with_timeout 15 (fun () ->
    let workers = 4 in
    let n = 20000 in
    let events = List.init n (fun i ->
      Mf_event.data (Printf.sprintf "k%d" (i mod workers)) (i * 10)) in
    let observed = ref [] in
    let mu = Mutex.create () in
    Parallel.run_parallel_simple
      ~on_queue_depth:(fun depths ->
        Mutex.lock mu; observed := depths :: !observed; Mutex.unlock mu)
      ~workers ~capacity:16
      ~key_of:(fun k -> k)
      ~pipeline:(fun src ->
        (* медленный воркер: усложняем, чтобы очереди наполнялись *)
        src |> Stream.map (fun ev -> ignore (Sys.opaque_identity 0); ev))
      ~source:(Stream.of_list events)
      ~sink:(fun _ -> ())
      ();
    let samples = !observed in
    check "hook was invoked at least once" (List.length samples > 0);
    check "each sample has one entry per worker"
      (List.for_all (fun a -> Array.length a = workers) samples);
    check "depths are non-negative"
      (List.for_all (Array.for_all (fun d -> d >= 0)) samples);
    check "depths never exceed capacity"
      (List.for_all (Array.for_all (fun d -> d <= 16)) samples))

let test_hook_optional () =
  Printf.printf "\n-- without hook: runs normally (hook is optional)\n";
  with_timeout 10 (fun () ->
    let n = 1000 in
    let events = List.init n (fun i -> Mf_event.data (Printf.sprintf "k%d" (i mod 4)) i) in
    let cnt = ref 0 in
    let mu = Mutex.create () in
    Parallel.run_parallel_simple
      ~workers:4 ~capacity:32
      ~key_of:(fun k -> k)
      ~pipeline:(fun src -> src)
      ~source:(Stream.of_list events)
      ~sink:(fun _ -> Mutex.lock mu; incr cnt; Mutex.unlock mu)
      ();
    check "runs without hook, all processed" (!cnt = n))

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Queue depth observability hook\n";
  Printf.printf "==========================================\n";
  test_hook_invoked ();
  test_hook_optional ();
  Printf.printf "\nAll queue-depth tests passed.\n"
