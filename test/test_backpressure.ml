open Miniflink
(* Тест сквозного backpressure. В pull/bounded-архитектуре backpressure
   распространяется естественно: медленный sink → воркер реже читает →
   канал заполняется → dispatcher блокируется на push → source перестаёт
   вычитываться. Этот тест доказывает что цепочка реально тормозит чтение
   источника, а не буферизует всё в памяти неограниченно. *)

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let with_timeout secs f =
  Sys.set_signal Sys.sigalrm (Sys.Signal_handle (fun _ ->
    Printf.printf "  FAIL: TIMEOUT\n%!"; exit 1));
  ignore (Unix.alarm secs);
  let r = f () in ignore (Unix.alarm 0); r

(* Источник, считающий сколько событий реально вычитано *)
let counting_source n pulled =
  let i = ref 0 in
  fun () ->
    if !i >= n then None
    else begin
      incr i;
      Mutex.lock (fst pulled); incr (snd pulled); Mutex.unlock (fst pulled);
      Some (Mf_event.data (Printf.sprintf "k%d" (!i mod 4)) !i)
    end

(* ── медленный sink тормозит вычитывание источника ─────────── *)
let test_slow_sink_throttles_source () =
  Printf.printf "\n-- slow sink throttles source consumption (no unbounded buffering)\n";
  with_timeout 30 (fun () ->
    let n = 100000 in
    let pulled_mu = Mutex.create () in
    let pulled = ref 0 in
    let processed = ref 0 in
    let pmu = Mutex.create () in
    let stop = ref false in
    (* sink медленный: спит, обрабатывая каждое событие *)
    let slow_sink _ =
      Mutex.lock pmu; incr processed; Mutex.unlock pmu;
      if not !stop then Thread.delay 0.0001 in
    (* запускаем параллельно в отдельном потоке чтобы замерить в середине *)
    let runner = Thread.create (fun () ->
      Parallel.run_parallel_simple
        ~workers:2 ~capacity:64
        ~key_of:(fun k -> k)
        ~pipeline:(fun src -> src)
        ~source:(counting_source n (pulled_mu, pulled))
        ~sink:slow_sink
        ()) () in
    (* даём поработать чуть-чуть, потом смотрим разрыв pulled vs processed *)
    Thread.delay 0.3;
    Mutex.lock pulled_mu; let p = !pulled in Mutex.unlock pulled_mu;
    Mutex.lock pmu; let done_ = !processed in Mutex.unlock pmu;
    (* Ключевая проверка: source НЕ убежал далеко вперёд от sink.
       Разрыв ограничен ёмкостью каналов (64*2) + буферы воркеров, а не
       равен n. Если бы backpressure не работал, pulled ≈ n мгновенно. *)
    let gap = p - done_ in
    check "source did not drain fully ahead of slow sink"
      (p < n);                       (* не вычитал все 100k мгновенно *)
    check "pulled-processed gap is bounded (backpressure holds)"
      (gap < 2000);                  (* разрыв ~ ёмкость каналов, не n *)
    stop := true;
    Thread.join runner;
    (* после снятия торможения — всё обработано *)
    Mutex.lock pmu; let final = !processed in Mutex.unlock pmu;
    check "all events eventually processed" (final = n))

(* ── быстрый sink: источник вычитывается полностью ─────────── *)
let test_fast_sink_full_throughput () =
  Printf.printf "\n-- fast sink: everything flows through\n";
  with_timeout 20 (fun () ->
    let n = 50000 in
    let pulled_mu = Mutex.create () in
    let pulled = ref 0 in
    let processed = ref 0 in
    let pmu = Mutex.create () in
    Parallel.run_parallel_simple
      ~workers:4 ~capacity:128
      ~key_of:(fun k -> k)
      ~pipeline:(fun src -> src)
      ~source:(counting_source n (pulled_mu, pulled))
      ~sink:(fun _ -> Mutex.lock pmu; incr processed; Mutex.unlock pmu)
      ();
    check "all pulled" (!pulled = n);
    check "all processed" (!processed = n))

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  End-to-end backpressure\n";
  Printf.printf "==========================================\n";
  test_slow_sink_throttles_source ();
  test_fast_sink_full_throughput ();
  Printf.printf "\nAll backpressure tests passed.\n"
