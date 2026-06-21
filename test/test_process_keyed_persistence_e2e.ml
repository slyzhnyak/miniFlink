(** End-to-end persistence test для Pipe.process_keyed на
    Mock_source.Default.

    FSM: per-lamp state {Active | Silent}, event_timer для
    дозревания alert'а «нет пакетов дольше threshold». Это
    воспроизводит паттерн ex07.connectivity_alerts, но как
    самостоятельный тест — не трогает существующий код ex07.

    Сценарий "crash mid-stream":
    1. Baseline: FSM на полном Mock_source. Считаем alerts.
    2. Phase 1: до t=180с с backend. Записывает state каждой FSM.
    3. "Crash" — stream больше не дёргается.
    4. Phase 2: новый process_keyed с тем же backend, от t=180с
       до конца. Restore подгружает state + event_timers.
    5. Verify: phase1_alerts + phase2_alerts = baseline_alerts. *)

open Miniflink
open Ex07_location_lib

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

(* Тип состояния FSM. *)
type fsm_state = {
  mutable last_seen_ts : Time.t;
  mutable timer_at     : Time.t;
}

(* Threshold: "молчание дольше 30 секунд = alert". *)
let silence_threshold = Time.seconds 30

(* Алерт (просто строка для подсчёта). *)
type alert = No_packets of string * Time.t

(* Главный pipeline: process_keyed с FSM logic.
   ОРТОГОНАЛЬНО: пайплайн не упоминает persistence — режим задаётся
   снаружи через Runtime_context.with_context. ~name даёт стабильный
   namespace состояния в backend. *)
let connectivity_pipeline events =
  events |> Stream.of_list
  |> Pipe.process_keyed
       (module struct
         type t = Domain.packet
         let key (p : Domain.packet) = p.lamp
       end)
       ~name:"fsm_e2e"
       ~init:(fun () -> { last_seen_ts = 0; timer_at = 0 })
       ~on_event:(fun ctx _key st (p : Domain.packet) ->
         (* Cancel предыдущий таймер если был *)
         if st.timer_at > 0 then ctx.cancel_event_timer st.timer_at;
         st.last_seen_ts <- p.ts;
         st.timer_at <- p.ts + silence_threshold;
         ctx.set_event_timer st.timer_at)
       ~on_timer:(fun ctx key st t _kind ->
         (* Таймер сработал — шахтёр молчит. Emit alert. *)
         ctx.emit (No_packets (key, t));
         st.timer_at <- 0)  (* сбрасываем чтобы не cancel'ить дважды *)

(* Собрать все алерты per-key и total. *)
let count_alerts stream =
  let per_key = Hashtbl.create 16 in
  let rec loop () = match stream () with
    | None -> ()
    | Some (Mf_event.Data (No_packets (key, _), _)) ->
      let n = try Hashtbl.find per_key key with Not_found -> 0 in
      Hashtbl.replace per_key key (n + 1);
      loop ()
    | Some _ -> loop ()
  in loop ();
  per_key

(* Helper для split события по ts. *)
let split_at_ts events cutoff_ms =
  let before, after = List.partition
    (fun ev -> Mf_event.ts ev < cutoff_ms) events in
  (before @ [Mf_event.wm cutoff_ms], after)

let mock_packet_events () =
  Mock_source.Default.read () |> Stream.to_list

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  E2E process_keyed persistence — crash mid-stream\n";
  Printf.printf "==========================================\n";

  let all_events = mock_packet_events () in
  Printf.printf "Mock_source.Default: %d events\n" (List.length all_events);

  let phase1_events, phase2_events = split_at_ts all_events 180_000 in
  let phase2_with_wm = phase2_events @ [Mf_event.wm 500_000] in
  Printf.printf "  phase 1 events: %d, phase 2 events: %d\n"
    (List.length phase1_events) (List.length phase2_events);

  (* Baseline: те же phase1+phase2 events подряд, но БЕЗ crash —
     один pipeline. Это даёт корректное сравнение с phase1+phase2
     с crash через persistence. Использовать full mock_source было
     бы неверно: Mock_source.Default добавляет late events в КОНЕЦ
     stream'а, и split нарушает эту последовательность. *)
  Printf.printf "\n-- Baseline: same events without crash\n";
  let combined_events = phase1_events @ phase2_with_wm in
  let baseline_stream = connectivity_pipeline combined_events in
  let baseline = count_alerts baseline_stream in
  Hashtbl.iter (fun k n ->
    Printf.printf "  %s: %d alert(s)\n" k n) baseline;
  let baseline_total = Hashtbl.fold (fun _ v a -> a + v) baseline 0 in
  Printf.printf "  baseline total: %d\n" baseline_total;
  check "baseline produces alerts" (baseline_total >= 1);

  (* ── Phase 1 ──────────────────────────────────────────────── *)
  Printf.printf "\n-- Phase 1: run up to t=180s in durable context\n";

  let tbl = Hashtbl.create 64 in
  let backend = Persistence_backend.of_memory tbl in
  let ctx = Runtime_context.durable backend in

  (* Тот же connectivity_pipeline, без изменений — persistence снаружи. *)
  let phase1_alerts =
    Runtime_context.with_context ctx (fun () ->
      count_alerts (connectivity_pipeline phase1_events)) in
  let phase1_total = Hashtbl.fold (fun _ v a -> a + v) phase1_alerts 0 in
  Printf.printf "  phase 1 alerts: %d\n" phase1_total;

  let backend_keys = Hashtbl.length tbl in
  Printf.printf "  backend has %d FSM records\n" backend_keys;
  check "phase 1: backend has FSM records" (backend_keys >= 1);

  (* ── Phase 2: новый pipeline, тот же контекст → restore ──────── *)
  Printf.printf "\n-- Phase 2: new pipeline picks up state from backend\n";

  let phase2_alerts =
    Runtime_context.with_context ctx (fun () ->
      count_alerts (connectivity_pipeline phase2_with_wm)) in
  let phase2_total = Hashtbl.fold (fun _ v a -> a + v) phase2_alerts 0 in
  Printf.printf "  phase 2 alerts: %d\n" phase2_total;

  (* Главный invariant: per-key и total *)
  let all_keys = Hashtbl.fold (fun k _ acc -> k :: acc) baseline [] in
  List.iter (fun key ->
    let base = try Hashtbl.find baseline key with Not_found -> 0 in
    let p1 = try Hashtbl.find phase1_alerts key with Not_found -> 0 in
    let p2 = try Hashtbl.find phase2_alerts key with Not_found -> 0 in
    check (Printf.sprintf "%s: phase1+phase2 (%d+%d=%d) = baseline (%d)"
             key p1 p2 (p1+p2) base)
      (p1 + p2 = base)
  ) all_keys;

  let total_with_crash = phase1_total + phase2_total in
  check (Printf.sprintf "total: with crash %d = baseline %d"
           total_with_crash baseline_total)
    (total_with_crash = baseline_total);

  Printf.printf "\nE2E process_keyed persistence test passed.\n"
