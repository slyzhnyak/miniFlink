(** End-to-end persistence test для Pipe.window_fold на
    Mock_source.Default.

    Sliding window 60с/15с — каждые 15с эмитим (key, sum_voltage)
    по последним 60 секундам. Это создаёт ~4 одновременных окна на
    каждого шахтёра в любой момент времени.

    Сценарий "crash mid-stream":
    1. Baseline: window_fold на phase1+phase2 событиях подряд.
       Считаем эмиссии per ключ.
    2. Phase 1: до t=180с с backend. Многие окна в state=open или
       только-что fired в backend.
    3. "Crash" — stream больше не дёргается.
    4. Phase 2: новый window_fold с тем же backend, от t=180с.
       restore_all загружает все per-window states из backend.
    5. Verify: phase1_emits + phase2_emits = baseline_emits. *)

open Miniflink
open Ex07_location_lib

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

(* Pipeline: window_fold (sliding 60s/15s) суммирует voltage*100
   per lamp. allowed_lateness=30с — чтобы FFired окна оставались в
   backend в зоне допустимого опоздания.

   ВАЖНО: пайплайн НЕ упоминает persistence. Один и тот же код
   работает в ephemeral и durable — persistence задаётся снаружи
   через Runtime_context.with_context на вызывающей стороне. Это и
   есть цель ортогонального дизайна. *)
let voltage_window events =
  events |> Stream.of_list
  |> Pipe.window_fold
       (module struct
         type t = Domain.packet
         let key (p : Domain.packet) = p.lamp
       end)
       ~allowed_lateness:(Time.seconds 30)
       (Pipe.sliding (Time.seconds 60) (Time.seconds 15))
       ~init:(fun () -> 0)
       ~add:(fun acc (p : Domain.packet) ->
         acc + int_of_float (p.voltage *. 100.))

(* Считаем Data-эмиссии per key. *)
let count_per_key stream =
  let tbl = Hashtbl.create 16 in
  let rec loop () = match stream () with
    | None -> ()
    | Some (Mf_event.Data ((key, _), _)) ->
      let n = try Hashtbl.find tbl key with Not_found -> 0 in
      Hashtbl.replace tbl key (n + 1);
      loop ()
    | Some _ -> loop ()
  in loop ();
  tbl

let split_at_ts events cutoff =
  let before, after = List.partition
    (fun ev -> Mf_event.ts ev < cutoff) events in
  (before @ [Mf_event.wm cutoff], after)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  E2E window_fold persistence — crash mid-stream\n";
  Printf.printf "==========================================\n";

  let all_events = Mock_source.Default.read () |> Stream.to_list in
  let phase1_events, phase2_events = split_at_ts all_events 180_000 in
  let phase2_with_wm = phase2_events @ [Mf_event.wm 500_000] in

  Printf.printf "Mock_source: %d events total\n" (List.length all_events);
  Printf.printf "  phase 1: %d events, phase 2: %d events\n"
    (List.length phase1_events) (List.length phase2_events);

  (* ── Baseline: те же события подряд без crash (EPHEMERAL) ──── *)
  Printf.printf "\n-- Baseline: phase1+phase2 events concatenated (ephemeral)\n";

  let combined = phase1_events @ phase2_with_wm in
  let baseline_stream = voltage_window combined in
  let baseline = count_per_key baseline_stream in
  Hashtbl.iter (fun k n -> Printf.printf "  %s: %d emit(s)\n" k n) baseline;
  let baseline_total = Hashtbl.fold (fun _ v a -> a + v) baseline 0 in
  Printf.printf "  baseline total: %d\n" baseline_total;
  check "baseline produces emissions" (baseline_total >= 1);

  (* ── Phase 1: ТОТ ЖЕ пайплайн, но в DURABLE-контексте ──────── *)
  Printf.printf "\n-- Phase 1: run up to t=180s in durable context\n";

  let tbl = Hashtbl.create 64 in
  let backend = Persistence_backend.of_memory tbl in
  let ctx = Runtime_context.durable backend in

  (* Тот же voltage_window, без изменений — persistence снаружи. *)
  let phase1_counts =
    Runtime_context.with_context ctx (fun () ->
      count_per_key (voltage_window phase1_events)) in
  let phase1_total = Hashtbl.fold (fun _ v a -> a + v) phase1_counts 0 in
  Printf.printf "  phase 1 emits: %d\n" phase1_total;

  let backend_records = Hashtbl.length tbl in
  Printf.printf "  backend has %d window records\n" backend_records;
  check "phase 1: backend has window records" (backend_records >= 1);

  (* ── Phase 2: новый window_fold в том же контексте → restore ── *)
  Printf.printf "\n-- Phase 2: new window_fold picks up state from backend\n";

  let phase2_counts =
    Runtime_context.with_context ctx (fun () ->
      count_per_key (voltage_window phase2_with_wm)) in
  let phase2_total = Hashtbl.fold (fun _ v a -> a + v) phase2_counts 0 in
  Printf.printf "  phase 2 emits: %d\n" phase2_total;

  (* Главный invariant: per-key *)
  let all_keys = Hashtbl.fold (fun k _ acc -> k :: acc) baseline [] in
  List.iter (fun key ->
    let base = try Hashtbl.find baseline key with Not_found -> 0 in
    let p1 = try Hashtbl.find phase1_counts key with Not_found -> 0 in
    let p2 = try Hashtbl.find phase2_counts key with Not_found -> 0 in
    check (Printf.sprintf "%s: phase1+phase2 (%d+%d=%d) = baseline (%d)"
             key p1 p2 (p1+p2) base)
      (p1 + p2 = base)
  ) all_keys;

  let total_with_crash = phase1_total + phase2_total in
  check (Printf.sprintf "total: with crash %d = baseline %d"
           total_with_crash baseline_total)
    (total_with_crash = baseline_total);

  Printf.printf "\nE2E window_fold persistence test passed.\n"
