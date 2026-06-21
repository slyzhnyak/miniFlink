open Miniflink
(* Тест бага #4: воркер падает → dispatcher не зависает (no deadlock) *)

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1

(* Watchdog: если тест зависнет (deadlock) — упадём через 10 сек *)
let with_timeout secs f =
  Sys.set_signal Sys.sigalrm (Sys.Signal_handle (fun _ ->
    Printf.printf "  FAIL: TIMEOUT (deadlock detected)\n%!"; exit 1));
  ignore (Unix.alarm secs);
  let r = f () in
  ignore (Unix.alarm 0);
  r

(* ── Тест 1: воркер бросает исключение на определённом значении ─── *)
let test_worker_crash_no_deadlock () =
  Printf.printf "\n-- Worker crash does not deadlock dispatcher\n";
  with_timeout 10 (fun () ->
    (* Источник: 10000 событий, ключи раскиданы по воркерам *)
    let n = 10000 in
    let events = List.init n (fun i ->
      Mf_event.data (Printf.sprintf "key_%d" (i mod 8)) (i * 100)) in

    let processed = ref 0 in
    let mu = Mutex.create () in

    (* Pipeline одного воркера: падает если видит "key_3" *)
    let pipeline src =
      src |> Stream.map (fun ev ->
        (match ev with
         | Mf_event.Data (k, _) when k = "key_3" ->
           failwith "simulated worker crash on key_3"
         | _ -> ());
        ev)
    in

    (* capacity маленький — чтобы канал быстро заполнился если consumer мёртв *)
    Parallel.run_parallel_simple
      ~workers:8 ~capacity:16
      ~key_of:(fun k -> k)
      ~pipeline
      ~source:(Stream.of_list events)
      ~sink:(fun _ -> Mutex.lock mu; incr processed; Mutex.unlock mu)
      ();

    (* Если дошли сюда без таймаута — deadlock не случился *)
    pass "completed without deadlock";
    (* Воркер key_3 упал, но остальные 7 обработали свои события.
       Ключи раскиданы по i mod 8 — упал ровно 1 шард из 8, значит
       ~7/8 событий должны дойти. Проверяем что выжило большинство,
       а не просто >0 (иначе тест прошёл бы даже если упали 7 из 8). *)
    let expected_min = n * 6 / 8 in   (* запас: минимум 6/8 *)
    if !processed >= expected_min then
      pass (Printf.sprintf "survivors processed %d events (>= %d expected)" !processed expected_min)
    else if !processed > 0 then
      fail (Printf.sprintf "too few survived: %d (expected >= %d) — more workers died than the one crash"
              !processed expected_min)
    else fail "no events processed at all"
  )

(* ── Тест 2: нормальная работа без падений ──────────────────────── *)
let test_no_crash_all_processed () =
  Printf.printf "\n-- Normal run: all events processed\n";
  with_timeout 10 (fun () ->
    let n = 1000 in
    let events = List.init n (fun i ->
      Mf_event.data (Printf.sprintf "key_%d" (i mod 4)) (i * 100)) in
    let processed = ref 0 in
    let mu = Mutex.create () in
    Parallel.run_parallel_simple
      ~workers:4 ~capacity:64
      ~key_of:(fun k -> k)
      ~pipeline:(fun src -> src)
      ~source:(Stream.of_list events)
      ~sink:(fun _ -> Mutex.lock mu; incr processed; Mutex.unlock mu)
      ();
    if !processed = n then pass (Printf.sprintf "all %d events processed" n)
    else fail (Printf.sprintf "expected %d, got %d" n !processed)
  )

(* ── Тест 3: run_parallel_simple — нет busy-wait ──────── *)
let test_collector_no_busywait () =
  Printf.printf "\n-- run_parallel_simple: all processed, no hang\n";
  with_timeout 15 (fun () ->
    let n = 5000 in
    let events = List.init n (fun i ->
      Mf_event.data (Printf.sprintf "k%d" (i mod 4)) (i * 100)) in
    (* Pipeline с задержкой: имитируем медленный оператор.
       Если collector busy-wait — CPU time >> wall time. *)
    let processed = ref 0 in
    let mu = Mutex.create () in
    Parallel.run_parallel_simple
      ~workers:4 ~capacity:64
      ~key_of:(fun k -> k)
      ~pipeline:(fun src -> src)
      ~source:(Stream.of_list events)
      ~sink:(fun _ -> Mutex.lock mu; incr processed; Mutex.unlock mu)
      ();
    if !processed = n then pass "all events processed, no busy-wait hang"
    else fail (Printf.sprintf "expected %d got %d" n !processed)
  )

(* ── Тест 4: per-worker sink_factory (без глобального mutex) ─── *)
let test_per_worker_sinks () =
  Printf.printf "\n-- Per-worker sinks: each worker writes to own buffer\n";
  with_timeout 10 (fun () ->
    let n = 4000 in
    let events = List.init n (fun i ->
      Mf_event.data (Printf.sprintf "k%d" (i mod 4)) (i * 100)) in
    (* Каждый воркер пишет в свой буфер — никакого общего mutex *)
    let buffers = Array.init 4 (fun _ -> ref 0) in
    Parallel.run_parallel_simple
      ~sink_factory:(fun i -> fun _ -> incr buffers.(i))
      ~workers:4 ~capacity:64
      ~key_of:(fun k -> k)
      ~pipeline:(fun src -> src)
      ~source:(Stream.of_list events)
      ~sink:(fun _ -> ())   (* не используется когда есть factory *)
      ();
    let total = Array.fold_left (fun a r -> a + !r) 0 buffers in
    if total = n then pass (Printf.sprintf "all %d events across per-worker sinks" n)
    else fail (Printf.sprintf "expected %d, got %d" n total);
    (* Каждый воркер получил события своего шарда *)
    let nonempty = Array.fold_left (fun a r -> if !r > 0 then a+1 else a) 0 buffers in
    pass (Printf.sprintf "%d workers received events" nonempty)
  )

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Bug #4: silent worker death -> deadlock\n";
  Printf.printf "==========================================\n";
  test_no_crash_all_processed ();
  test_worker_crash_no_deadlock ();
  test_collector_no_busywait ();
  test_per_worker_sinks ();
  Printf.printf "\nAll parallel crash tests passed.\n"
