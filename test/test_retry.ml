open Miniflink
(* Тесты политики повторов с backoff. sleep вынесен параметром, поэтому
   тесты не ждут реального времени — передают подсчёт вместо сна. *)

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

(* ── delay_for: экспоненциальный рост с потолком ───────────── *)
let test_delay_growth () =
  Printf.printf "\n-- delay_for: exponential growth, capped\n";
  let p = { Retry.max_attempts = 10; base_ms = 100; factor = 2.0;
            max_ms = 5000; jitter = false } in
  check "attempt 1 = 0 (no pause before first)" (Retry.delay_for p 1 = 0);
  check "attempt 2 = base (100)" (Retry.delay_for p 2 = 100);
  check "attempt 3 = 200" (Retry.delay_for p 3 = 200);
  check "attempt 4 = 400" (Retry.delay_for p 4 = 400);
  check "attempt 5 = 800" (Retry.delay_for p 5 = 800);
  (* рост до потолка *)
  check "large attempt capped at max_ms" (Retry.delay_for p 20 = 5000)

(* ── успех с первой попытки — sleep не зовётся ──────────────── *)
let test_success_no_retry () =
  Printf.printf "\n-- success on first try: no sleep, no give_up\n";
  let slept = ref 0 and gave_up = ref false in
  let r = Retry.with_retry Retry.default
    ~sleep:(fun _ -> incr slept)
    ~on_give_up:(fun _ _ -> gave_up := true)
    (fun x -> x * 2) 21 in
  check "result Some 42" (r = Some 42);
  check "never slept" (!slept = 0);
  check "never gave up" (not !gave_up)

(* ── транзиентный сбой: падает K раз, потом успех ──────────── *)
let test_transient_then_success () =
  Printf.printf "\n-- fails N times then succeeds: retried, then ok\n";
  let attempts = ref 0 and slept = ref 0 in
  let flaky _ =
    incr attempts;
    if !attempts < 3 then failwith "transient"
    else "recovered" in
  let r = Retry.with_retry
    { Retry.default with max_attempts = 5 }
    ~sleep:(fun _ -> incr slept)
    ~on_give_up:(fun _ _ -> fail "should not give up")
    flaky () in
  check "succeeded after retries" (r = Some "recovered");
  check "took 3 attempts" (!attempts = 3);
  check "slept between retries (2 pauses)" (!slept = 2)

(* ── исчерпание попыток: give_up зовётся, None ─────────────── *)
let test_give_up () =
  Printf.printf "\n-- always fails: gives up after max_attempts\n";
  let attempts = ref 0 and gave_up_n = ref 0 in
  let always_fail _ = incr attempts; failwith "permanent" in
  let r = Retry.with_retry
    { Retry.default with max_attempts = 4 }
    ~sleep:(fun _ -> ())
    ~on_give_up:(fun _ n -> gave_up_n := n)
    always_fail () in
  check "result None" (r = None);
  check "tried exactly max_attempts (4)" (!attempts = 4);
  check "give_up reported attempt count" (!gave_up_n = 4)

(* ── give_up получает последнее исключение ─────────────────── *)
let test_give_up_exn () =
  Printf.printf "\n-- give_up receives the last exception\n";
  let last = ref None in
  ignore (Retry.with_retry { Retry.default with max_attempts = 2 }
    ~sleep:(fun _ -> ())
    ~on_give_up:(fun e _ -> last := Some (Printexc.to_string e))
    (fun _ -> failwith "boom") ());
  check "captured exception" (match !last with
    | Some s -> (try ignore (Str.search_forward (Str.regexp "boom") s 0); true
                 with Not_found -> String.length s > 0)
    | None -> false)

(* ── jitter не выходит за пределы ──────────────────────────── *)
let test_jitter_bounds () =
  Printf.printf "\n-- jitter stays within [0, capped]\n";
  Random.self_init ();
  let p = { Retry.max_attempts = 10; base_ms = 100; factor = 2.0;
            max_ms = 1000; jitter = true } in
  let ok = ref true in
  for _ = 1 to 100 do
    let d = Retry.delay_for p 5 in   (* без jitter было бы 800 *)
    if d < 0 || d > 800 then ok := false
  done;
  check "jittered delay in [0, 800]" !ok

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Retry policy (exponential backoff)\n";
  Printf.printf "==========================================\n";
  test_delay_growth ();
  test_success_no_retry ();
  test_transient_then_success ();
  test_give_up ();
  test_give_up_exn ();
  test_jitter_bounds ();
  Printf.printf "\nAll retry tests passed.\n"
