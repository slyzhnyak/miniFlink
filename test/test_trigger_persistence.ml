(** Trigger persistence — ОРТОГОНАЛЬНАЯ модель.

    Тот же триггерный пайплайн с persistence и без; режим — снаружи
    через Runtime_context. Проверяем ПОВЕДЕНИЕ: ephemeral как раньше,
    durable пишет в backend и переживает рестарт (state-машина
    восстанавливается, debounce-таймер продолжает дозревать). *)

open Miniflink

type alert = Above of string * float * Time.t | Resolved of string * Time.t

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let make_spec () =
  Trigger.create
    ~name:"test_above5"
    ~condition:(Trigger.greater_than 5.0)
    ~problem_for:(Time.seconds 10)
    ~severity:Trigger.Warning
    ~produce_alert:(fun ~key ~value ~ts -> Above (key, value, ts))
    ~produce_recovery:(fun ~key ~ts -> Resolved (key, ts))
    ()

let count_data stream =
  let n = ref 0 in
  let rec loop () = match stream () with
    | None -> ()
    | Some (Mf_event.Data _) -> incr n; loop ()
    | Some _ -> loop ()
  in loop (); !n

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Trigger — orthogonal persistence\n";
  Printf.printf "==========================================\n";

  (* ── 1. Ephemeral: как раньше ──────────────────────────────── *)
  Printf.printf "\n-- 1. Ephemeral: existing behavior\n";
  let evs = [ Mf_event.data ("A", 7.0) 0; Mf_event.wm 20_000 ] in
  let n = count_data (evs |> Stream.of_list |> Trigger.of_stream (make_spec ())) in
  check (Printf.sprintf "ephemeral: 1 Data alert (got %d)" n) (n = 1);

  (* ── 2. Durable: alert эмитится + backend получает запись ──── *)
  Printf.printf "\n-- 2. Durable: writes happen on transitions\n";
  let tbl = Hashtbl.create 16 in
  let backend = Persistence_backend.of_memory tbl in
  let ctx = Runtime_context.durable backend in
  Runtime_context.with_context ctx (fun () ->
    let evs = [ Mf_event.data ("A", 7.0) 0; Mf_event.wm 20_000 ] in
    let n = count_data (evs |> Stream.of_list |> Trigger.of_stream (make_spec ())) in
    check "durable: alert still emitted" (n = 1);
    check "durable: backend has record" (Hashtbl.length tbl >= 1));

  (* ── 3. Restore: debounce-таймер дозревает через рестарт ───── *)
  Printf.printf "\n-- 3. Restore: pending problem matures across restart\n";
  let tbl3 = Hashtbl.create 16 in
  let backend3 = Persistence_backend.of_memory tbl3 in
  let ctx3 = Runtime_context.durable backend3 in
  (* Phase 1: условие выполнено (B=8 > 5) на t=0, но wm только 3000 —
     problem_for=10000 не дозрел, alert ещё нет. State в Pending_problem
     записан в backend. *)
  let p1n = Runtime_context.with_context ctx3 (fun () ->
    let p1 = [ Mf_event.data ("B", 8.0) 0; Mf_event.wm 3000 ] in
    count_data (p1 |> Stream.of_list |> Trigger.of_stream (make_spec ()))) in
  check "phase 1: no alert yet (debounce not matured)" (p1n = 0);
  check "phase 1: backend has Pending state" (Hashtbl.length tbl3 >= 1);
  (* Phase 2: новый trigger, тот же backend → restore Pending_problem
     с fire_at=10000. wm 20000 дозревает таймер → alert. Без restore
     state был бы S_ok и alert не сработал бы. *)
  let p2n = Runtime_context.with_context ctx3 (fun () ->
    let p2 = [ Mf_event.wm 20_000 ] in
    count_data (p2 |> Stream.of_list |> Trigger.of_stream (make_spec ()))) in
  check "restore: pending timer fires alert after restart" (p2n = 1);

  Printf.printf "\nAll trigger persistence tests passed.\n"
