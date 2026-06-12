open Miniflink
(* Прямые тесты Channel — фундамент параллелизма, тестировался только
   косвенно (0 прямых тестов). Проверяем FIFO, bounded backpressure,
   close-семантику, try_push/try_pop на границах. *)

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let with_timeout secs f =
  Sys.set_signal Sys.sigalrm (Sys.Signal_handle (fun _ ->
    Printf.printf "  FAIL: TIMEOUT (вероятно deadlock)\n%!"; exit 1));
  ignore (Unix.alarm secs);
  let r = f () in ignore (Unix.alarm 0); r

(* ── 1. FIFO-порядок ────────────────────────────────────────── *)
let test_fifo () =
  Printf.printf "\n-- FIFO order preserved\n";
  with_timeout 5 (fun () ->
    let ch = Channel.make_unbounded () in
    List.iter (fun x -> Channel.push ch x) [1;2;3;4;5];
    Channel.close ch;
    let out = ref [] in
    let rec drain () = match Channel.pop ch with
      | Some x -> out := x :: !out; drain () | None -> () in
    drain ();
    check "order 1..5 preserved" (List.rev !out = [1;2;3;4;5]))

(* ── 2. length отражает содержимое ──────────────────────────── *)
let test_length () =
  Printf.printf "\n-- length tracks contents\n";
  with_timeout 5 (fun () ->
    let ch = Channel.make_unbounded () in
    check "empty length 0" (Channel.length ch = 0);
    Channel.push ch 10; Channel.push ch 20;
    check "after 2 pushes length 2" (Channel.length ch = 2);
    ignore (Channel.pop ch);
    check "after 1 pop length 1" (Channel.length ch = 1))

(* ── 3. try_pop на пустом → None (не блокирует) ─────────────── *)
let test_try_pop_empty () =
  Printf.printf "\n-- try_pop on empty returns None (non-blocking)\n";
  with_timeout 5 (fun () ->
    let ch = Channel.make_unbounded () in
    check "try_pop empty = None" (Channel.try_pop ch = None);
    Channel.push ch 7;
    check "try_pop after push = Some 7" (Channel.try_pop ch = Some 7);
    check "try_pop drained = None" (Channel.try_pop ch = None))

(* ── 3.5 capacity делает что обещает (регресс OCaml 5) ─────────
   На OCaml 5 channel — SPSC ring buffer с sentinel-ячейкой; нужна
   была capacity = N+1 чтобы N элементов реально влезали. До фикса
   make_bounded 2 давал реальную ёмкость 1, и второй try_push зависал
   в spin-loop'е. На OCaml 4 (Queue+Mutex) этого никогда не было.
   Этот тест явно проверяет что capacity = что просили. *)
let test_capacity_honors_request () =
  Printf.printf "\n-- make_bounded N accepts N elements without blocking\n";
  with_timeout 5 (fun () ->
    let ch = Channel.make_bounded 3 in
    check "push 1 of 3 non-blocking" (Channel.try_push ch 11 = true);
    check "push 2 of 3 non-blocking" (Channel.try_push ch 22 = true);
    check "push 3 of 3 non-blocking" (Channel.try_push ch 33 = true);
    check "length = 3 after 3 pushes" (Channel.length ch = 3);
    (* и сейчас при попытке 4-го должна быть блокировка — проверяем
       через подсчёт, что producer не закончил мгновенно *)
    let done4 = ref false in
    let producer = Thread.create (fun () ->
      ignore (Channel.try_push ch 44);
      done4 := true) () in
    Thread.delay 0.1;
    check "4th push blocks (capacity full)" (not !done4);
    ignore (Channel.pop ch);
    Thread.join producer;
    check "after pop, blocked push completed" !done4)

(* ── 4. close: pop отдаёт остаток, потом None ───────────────── *)
let test_close_drains () =
  Printf.printf "\n-- close: pop drains remaining then None\n";
  with_timeout 5 (fun () ->
    let ch = Channel.make_unbounded () in
    Channel.push ch 1; Channel.push ch 2;
    Channel.close ch;
    check "pop remaining 1" (Channel.pop ch = Some 1);
    check "pop remaining 2" (Channel.pop ch = Some 2);
    check "pop after drained+closed = None" (Channel.pop ch = None);
    check "pop again still None" (Channel.pop ch = None))

(* ── 5. try_push: backpressure (ждёт места), НЕ роняет ──────
   ВАЖНО: вопреки названию, try_push на полном bounded НЕ возвращает
   сразу false — он блокируется с backpressure (как push) и возвращает
   false ТОЛЬКО если канал закрыли пока он ждал. Это осознанная
   семантика для exactly-once (нельзя терять события). Имя вводит в
   заблуждение — см. test_try_push_naming ниже. *)
let test_try_push_backpressure () =
  Printf.printf "\n-- try_push blocks (backpressure), returns true when space frees\n";
  with_timeout 10 (fun () ->
    let ch = Channel.make_bounded 2 in
    check "push 1 ok" (Channel.try_push ch 1 = true);
    check "push 2 ok (now full)" (Channel.try_push ch 2 = true);
    (* push 3 на полном заблокируется — запускаем в потоке *)
    let done3 = ref false in
    let res3 = ref false in
    let producer = Thread.create (fun () ->
      res3 := Channel.try_push ch 3; done3 := true) () in
    Thread.delay 0.2;
    check "try_push on full BLOCKS (not instant false)" (not !done3);
    ignore (Channel.pop ch);          (* освободили место *)
    Thread.join producer;
    check "try_push delivered after space freed" (!res3 = true && !done3))

(* ── 6. bounded backpressure: блокирующий push ждёт места ───── *)
let test_backpressure () =
  Printf.printf "\n-- bounded push blocks until consumer frees space\n";
  with_timeout 10 (fun () ->
    let ch = Channel.make_bounded 1 in
    Channel.push ch 100;             (* канал полон *)
    let pushed = ref false in
    (* producer-поток: push должен заблокироваться пока не освободим *)
    let producer = Thread.create (fun () ->
      Channel.push ch 200;           (* блокируется здесь *)
      pushed := true) () in
    Thread.delay 0.2;
    check "push blocked while full" (not !pushed);
    let got = Channel.pop ch in       (* освобождаем место *)
    check "consumer got first item" (got = Some 100);
    Thread.join producer;
    check "producer unblocked after pop" !pushed;
    check "second item now present" (Channel.pop ch = Some 200))

(* ── 7. close разблокирует ждущего producer (try_push) ─────── *)
let test_close_unblocks () =
  Printf.printf "\n-- close lets try_push return false instead of hanging\n";
  with_timeout 10 (fun () ->
    let ch = Channel.make_bounded 1 in
    Channel.push ch 1;                (* полон *)
    Channel.close ch;
    (* на закрытом полном канале try_push не должен висеть вечно *)
    let r = Channel.try_push ch 2 in
    check "try_push on closed full = false" (r = false))

(* ── 8. producer/consumer стресс: ничего не теряется ───────── *)
let test_producer_consumer () =
  Printf.printf "\n-- producer/consumer: no loss, no duplication\n";
  with_timeout 15 (fun () ->
    let ch = Channel.make_bounded 8 in
    let n = 5000 in
    let result = ref (0, 0) in
    let consumer = Thread.create (fun () ->
      let sum = ref 0 and cnt = ref 0 in
      let rec loop () = match Channel.pop ch with
        | Some x -> sum := !sum + x; incr cnt; loop ()
        | None -> () in
      loop ();
      result := (!sum, !cnt)) () in
    for i = 1 to n do Channel.push ch i done;
    Channel.close ch;
    Thread.join consumer;
    let (sum, cnt) = !result in
    check "consumed all items" (cnt = n);
    check "sum correct (nothing lost/dup)" (sum = n * (n + 1) / 2))

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Channel tests (FIFO, backpressure, close)\n";
  Printf.printf "==========================================\n";
  test_fifo ();
  test_length ();
  test_try_pop_empty ();
  test_capacity_honors_request ();
  test_close_drains ();
  test_try_push_backpressure ();
  test_backpressure ();
  test_close_unblocks ();
  test_producer_consumer ();
  Printf.printf "\nAll channel tests passed.\n"
