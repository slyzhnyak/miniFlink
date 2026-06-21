(** silence_age persistence — ОРТОГОНАЛЬНАЯ модель.

    Тот же пайплайн с persistence и без; режим — снаружи через
    Runtime_context. Проверяем ПОВЕДЕНИЕ: ephemeral как раньше,
    durable пишет в backend и переживает рестарт. *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let collect_ages stream =
  let outs = ref [] in
  let rec drain () = match stream () with
    | None -> ()
    | Some (Mf_event.Data (v, _)) -> outs := v :: !outs; drain ()
    | Some _ -> drain ()
  in drain (); List.rev !outs

let sa_pipe events =
  events |> Stream.of_list
  |> Item.silence_age ~name:"np" ~by:(fun k -> k) ~tick:(Time.seconds 10)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  silence_age — orthogonal persistence\n";
  Printf.printf "==========================================\n";

  (* ── 1. Ephemeral: как раньше ──────────────────────────────── *)
  Printf.printf "\n-- 1. Ephemeral: existing behavior\n";
  let events = [
    Mf_event.data 7 0;
    Mf_event.wm 30_000;
    Mf_event.wm 60_000;
  ] in
  let pairs = collect_ages (sa_pipe events) in
  check "ephemeral: >= 3 emissions (1 zero + ticks)" (List.length pairs >= 3);
  check "first is (7, 0)" (List.hd pairs = (7, 0));

  (* ── 2. Durable: backend получает записи ───────────────────── *)
  Printf.printf "\n-- 2. Durable: writes happen\n";
  let tbl = Hashtbl.create 16 in
  let backend = Persistence_backend.of_memory tbl in
  let ctx = Runtime_context.durable backend in
  Runtime_context.with_context ctx (fun () ->
    let evs = [ Mf_event.data 42 0; Mf_event.wm 5000 ] in
    ignore (collect_ages (sa_pipe evs));
    check "durable: backend has record" (Hashtbl.length tbl >= 1));

  (* ── 3. Restore: state переживает рестарт ──────────────────── *)
  Printf.printf "\n-- 3. Restore: pending timer survives restart\n";
  let tbl3 = Hashtbl.create 16 in
  let backend3 = Persistence_backend.of_memory tbl3 in
  let ctx3 = Runtime_context.durable backend3 in
  (* Phase 1: событие key=5 на t=0, wm только 3000 — таймер (fire_at
     10000) ещё не сработал, но записан в backend. *)
  Runtime_context.with_context ctx3 (fun () ->
    let p1 = [ Mf_event.data 5 0; Mf_event.wm 3000 ] in
    ignore (collect_ages (sa_pipe p1)));
  check "phase 1: timer persisted" (Hashtbl.length tbl3 >= 1);
  (* Phase 2: новый instance, тот же backend → restore таймера.
     wm 15000 должен вызвать tick для key=5 (восстановлен из backend). *)
  let outs2 = Runtime_context.with_context ctx3 (fun () ->
    let p2 = [ Mf_event.wm 15_000 ] in
    collect_ages (sa_pipe p2)) in
  check "restore: tick fires for key 5 from restored timer"
    (List.exists (fun (k, _) -> k = 5) outs2);

  Printf.printf "\nAll silence_age persistence tests passed.\n"
