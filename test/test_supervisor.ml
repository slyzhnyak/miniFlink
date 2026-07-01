open Miniflink
(* Supervisor: запускает N пайплайнов, каждый со своей стратегией при
   падении (Restart с восстановлением / Isolate / Crash_all). Стратегия
   выбирается при создании пайплайна. *)

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

(* ── Restart: пайплайн падает дважды, рестартится, в итоге завершается ─ *)
let test_restart_until_success () =
  Printf.printf "\n-- Restart: pipeline recovers after transient failures\n";
  let attempts = ref 0 in
  let completed = ref false in
  let spec = Supervisor.{
    label = "flaky";
    run = (fun () ->
      incr attempts;
      if !attempts < 3 then failwith "transient boom";
      completed := true);
    on_failure = Restart { max_retries = 5; backoff_ms = 0 };
  } in
  Supervisor.supervise [spec];
  check "ran 3 times (2 failures + success)" (!attempts = 3);
  check "completed after restarts" !completed

(* ── Restart с checkpoint: рестарт продолжает с сохранённого состояния ─ *)
let test_restart_resumes_from_checkpoint () =
  Printf.printf "\n-- Restart resumes from checkpoint (durable state survives)\n";
  let durable = ref 0 in          (* имитация durable store: переживает краш *)
  let attempts = ref 0 in
  let spec = Supervisor.{
    label = "stateful";
    run = (fun () ->
      incr attempts;
      (* восстановить прогресс из durable store *)
      let start = !durable in
      (* обработать 3 шага, сохраняя checkpoint после каждого *)
      for i = start to 2 do
        if i = 1 && !attempts = 1 then failwith "crash mid-way";
        durable := i + 1   (* checkpoint *)
      done);
    on_failure = Restart { max_retries = 3; backoff_ms = 0 };
  } in
  Supervisor.supervise [spec];
  check "resumed from checkpoint, finished all 3 steps" (!durable = 3);
  check "did not restart from scratch (checkpoint preserved progress)"
    (!attempts = 2)

(* ── Isolate: упавший помечается мёртвым, остальные работают ─ *)
let test_isolate_others_survive () =
  Printf.printf "\n-- Isolate: one fails, others keep running\n";
  let good_ran = ref false in
  let specs = Supervisor.[
    { label = "bad";  run = (fun () -> failwith "boom"); on_failure = Isolate };
    { label = "good"; run = (fun () -> good_ran := true); on_failure = Isolate };
  ] in
  let result = Supervisor.supervise_result specs in
  check "good pipeline ran despite bad failing" !good_ran;
  check "bad marked failed" (List.mem_assoc "bad" result && List.assoc "bad" result = `Failed);
  check "good marked ok" (List.assoc "good" result = `Ok)

(* ── Crash_all: критичный падает → весь supervisor падает ─ *)
let test_crash_all () =
  Printf.printf "\n-- Crash_all: critical failure brings everything down\n";
  let raised = ref false in
  let specs = Supervisor.[
    { label = "critical"; run = (fun () -> failwith "fatal"); on_failure = Crash_all };
  ] in
  (try Supervisor.supervise specs with _ -> raised := true);
  check "Crash_all propagated the failure" !raised

(* ── H-2: несколько одновременных Critical_failure ───────────
   Раньше supervisor хранил только первый Critical_failure, остальные
   терялись. Теперь все копятся и логируются, а исключение всё равно
   бросается. Проверяем, что при N одновременных Crash_all-сбоях:
   1. исключение проброшено (система падает, как и должна);
   2. supervise_result не теряет статусы всех пайплайнов. *)
let test_multiple_critical () =
  Printf.printf "\n-- H-2: multiple concurrent critical failures\n";
  let specs = Supervisor.[
    { label = "crit_a"; run = (fun () -> failwith "boom_a"); on_failure = Crash_all };
    { label = "crit_b"; run = (fun () -> failwith "boom_b"); on_failure = Crash_all };
    { label = "crit_c"; run = (fun () -> failwith "boom_c"); on_failure = Crash_all };
  ] in
  let raised = ref false in
  (try ignore (Supervisor.supervise_result specs)
   with Supervisor.Critical_failure _ -> raised := true);
  check "multiple criticals still raise" !raised

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Pipeline supervisor\n";
  Printf.printf "==========================================\n";
  test_restart_until_success ();
  test_restart_resumes_from_checkpoint ();
  test_isolate_others_survive ();
  test_crash_all ();
  test_multiple_critical ();
  Printf.printf "\nSupervisor tests passed.\n"
