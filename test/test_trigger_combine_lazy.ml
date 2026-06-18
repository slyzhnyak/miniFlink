(** Тест: Trigger.combine больше не материализует source.

    Раньше combine вызывал Stream.to_list source — на бесконечном
    источнике это зависало. На finite — копировал ВЕСЬ stream в
    память даже если триггер сработал на первом событии.

    Этот тест НЕ запускает бесконечный source (мы бы не смогли
    проверить отсутствие зависания без timeout), но проверяет:

    1. combine на finite stream работает корректно (regression check)
    2. Output эмитится постепенно (lazy), а не одним блоком после
       полной материализации входа — проверяется через side-effect
       счётчик чтения из source vs счётчик прочитанных алертов. *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let () =
  Printf.printf "Test: Trigger.combine — non-materializing\n%!";

  (* ── 1. Regression: combine продолжает работать на finite ─── *)
  Printf.printf "\n-- 1. Finite stream regression\n";
  let trigger_low = Trigger.create
    ~name:"low_v"
    ~condition:(Trigger.less_than 3.0)
    ~problem_for:(Time.seconds 0)
    ~severity:Trigger.Warning
    ~produce_alert:(fun ~key ~value ~ts -> `Low (key, value, ts))
    ~produce_recovery:(fun ~key ~ts -> `LowRec (key, ts))
    () in
  let trigger_hi = Trigger.create
    ~name:"hi_v"
    ~condition:(Trigger.greater_than 5.0)
    ~problem_for:(Time.seconds 0)
    ~severity:Trigger.Warning
    ~produce_alert:(fun ~key ~value ~ts -> `High (key, value, ts))
    ~produce_recovery:(fun ~key ~ts -> `HighRec (key, ts))
    () in

  let events = [
    Mf_event.data ("A", 2.5) 0;     (* low fire *)
    Mf_event.wm 1000;
    Mf_event.data ("B", 6.0) 1500;  (* high fire *)
    Mf_event.wm 2500;
    Mf_event.data ("A", 4.0) 3000;  (* low recovery *)
    Mf_event.wm 4000;
  ] in
  let source = Stream.of_list events in
  let combined = Trigger.combine [trigger_low; trigger_hi] source in
  let alerts = Pipe.collect combined in
  Printf.printf "  got %d alerts\n" (List.length alerts);
  check "at least 2 alerts (low fire + high fire)"
    (List.length alerts >= 2);

  (* ── 2. Lazy evaluation: combine читает source инкрементально ── *)
  Printf.printf "\n-- 2. Lazy reading from source\n";
  (* Создаём source с side-effect счётчиком чтения *)
  let read_count = ref 0 in
  let events2 = List.init 1000 (fun i ->
    Mf_event.data (Printf.sprintf "K%d" (i mod 10), float (i mod 7) +. 1.0) (i * 100)
  ) in
  let raw_source = Stream.of_list events2 in
  let counting_source () =
    let r = raw_source () in
    if r <> None then incr read_count;
    r
  in

  let trigger1 = Trigger.create
    ~name:"t1"
    ~condition:(Trigger.less_than 2.5)
    ~problem_for:(Time.seconds 0)
    ~severity:Trigger.Warning
    ~produce_alert:(fun ~key ~value ~ts -> (key, value, ts))
    ~produce_recovery:(fun ~key ~ts -> (key, 0.0, ts))
    () in
  let trigger2 = Trigger.create
    ~name:"t2"
    ~condition:(Trigger.greater_than 5.5)
    ~problem_for:(Time.seconds 0)
    ~severity:Trigger.Warning
    ~produce_alert:(fun ~key ~value ~ts -> (key, value, ts))
    ~produce_recovery:(fun ~key ~ts -> (key, 0.0, ts))
    () in

  let combined2 = Trigger.combine [trigger1; trigger2] counting_source in

  (* Читаем только первый alert. Раньше combine материализовал
     ВЕСЬ source перед эмитом первого алерта (read_count = 1000).
     После fix — должно прочитаться существенно меньше. *)
  let first = combined2 () in
  Printf.printf "  first alert: %s\n" (match first with
    | Some _ -> "Some _"
    | None -> "None");
  Printf.printf "  read_count after first emit: %d / 1000\n" !read_count;
  (* После fix: read_count должен быть существенно меньше 1000
     (точное число зависит от того когда первый trigger сработает,
     но точно не должно быть = 1000). *)
  check "source NOT fully read before first emit"
    (!read_count < 1000);

  (* Дочитываем остаток *)
  let _ = Pipe.collect combined2 in
  Printf.printf "  read_count after full drain: %d\n" !read_count;
  check "source fully read after full drain"
    (!read_count = 1000);

  Printf.printf "\nTest passed.\n"
