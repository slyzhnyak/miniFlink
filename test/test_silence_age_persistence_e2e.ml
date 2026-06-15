(** End-to-end persistence test для Item.silence_age на реальном
    Mock_source.

    Сценарий "crash mid-stream":
    1. Baseline: silence_age на всём Mock_source.Default. Подсчёт
       реальных событий (key, 0) per key — должно совпасть с числом
       packets для этого key.
    2. Phase 1 (with backend): подаём events до t=180с, записывается
       состояние last_seen + pending timers.
    3. "Crash" — Phase 1 stream больше не дёргается.
    4. Phase 2 (new instance, same backend): подаём events от t=180с
       до конца. Restore подгружает last_seen + timers.
    5. Verify: phase1_zeros + phase2_zeros = baseline_zeros для
       каждого key. Crash был "невидимым" для итогового вывода. *)

open Miniflink
open Ex07_location_lib

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

(* Считаем (key, age=0) emissions per key (реальные packets). *)
let count_zeros_per_key stream =
  let tbl = Hashtbl.create 16 in
  let rec loop () = match stream () with
    | None -> ()
    | Some (Mf_event.Data ((key, 0), _ts)) ->
      let n = try Hashtbl.find tbl key with Not_found -> 0 in
      Hashtbl.replace tbl key (n + 1);
      loop ()
    | Some _ -> loop ()
  in loop ();
  tbl

(* Конструируем packet events Mock_source.Default. *)
let mock_packet_events () =
  Mock_source.Default.read () |> Stream.to_list

(* Разделить events по ts. *)
let split_at_ts events cutoff_ms =
  let before, after = List.partition
    (fun ev -> Mf_event.ts ev < cutoff_ms) events in
  (* Watermark в конце первой части чтобы выровнять обработку. *)
  (before @ [Mf_event.wm cutoff_ms], after)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  E2E silence_age persistence — crash mid-stream\n";
  Printf.printf "==========================================\n";

  let all_events = mock_packet_events () in
  Printf.printf "Mock_source.Default: %d events total\n"
    (List.length all_events);

  let mk_silence_age ?backend events =
    events |> Stream.of_list
    |> Item.silence_age
         ?backend
         ?backend_name:(if backend <> None then Some "e2e_test" else None)
         ?serialize_key:(if backend <> None
                         then Some (fun k -> `String k) else None)
         ?deserialize_key:(if backend <> None
                           then Some (fun j -> Yojson.Safe.Util.to_string j)
                           else None)
         ~by:(fun (p : Domain.packet) -> p.lamp)
         ~tick:(Time.seconds 30)
  in

  (* ── Baseline: full run без backend ────────────────────────── *)
  Printf.printf "\n-- Baseline: silence_age on full source\n";

  let baseline_stream = mk_silence_age all_events in
  let baseline_zeros = count_zeros_per_key baseline_stream in
  Hashtbl.iter (fun key n ->
    Printf.printf "  %s: %d zero-emissions\n" key n) baseline_zeros;
  let baseline_total = Hashtbl.fold (fun _ v a -> a + v) baseline_zeros 0 in
  Printf.printf "  baseline total zeros: %d\n" baseline_total;

  (* ── Phase 1: подаём events до t=180с с backend ─────────────── *)
  Printf.printf "\n-- Phase 1: run up to t=180s with backend\n";

  let tbl = Hashtbl.create 64 in
  let backend = Persistence_backend.of_memory tbl in

  let phase1_events, phase2_events = split_at_ts all_events 180_000 in
  Printf.printf "  phase 1 events: %d, phase 2 events: %d\n"
    (List.length phase1_events) (List.length phase2_events);

  let phase1_stream = mk_silence_age ~backend phase1_events in
  let phase1_zeros = count_zeros_per_key phase1_stream in
  let phase1_total = Hashtbl.fold (fun _ v a -> a + v) phase1_zeros 0 in
  Printf.printf "  phase 1 zeros: %d (per key: " phase1_total;
  Hashtbl.iter (fun k n -> Printf.printf "%s=%d " k n) phase1_zeros;
  Printf.printf ")\n";

  let backend_keys = Hashtbl.length tbl in
  Printf.printf "  backend has %d state records\n" backend_keys;
  check "phase 1: backend has records for active keys"
    (backend_keys >= 1);

  (* Проверим что backend содержит last_seen для каждого активного ключа *)
  let backend_keys_list = Hashtbl.fold (fun k _ a -> k :: a) tbl [] in
  let has_m1 = List.exists
    (fun bk -> String.length bk > 0 &&
               bk = "item:silence_age:e2e_test:\"M1\"")
    backend_keys_list in
  check "backend has M1 record" has_m1;

  (* ── Phase 2: новый instance с тем же backend ──────────────── *)
  Printf.printf "\n-- Phase 2: new silence_age picks up state\n";

  let phase2_stream = mk_silence_age ~backend phase2_events in
  let phase2_zeros = count_zeros_per_key phase2_stream in
  let phase2_total = Hashtbl.fold (fun _ v a -> a + v) phase2_zeros 0 in
  Printf.printf "  phase 2 zeros: %d (per key: " phase2_total;
  Hashtbl.iter (fun k n -> Printf.printf "%s=%d " k n) phase2_zeros;
  Printf.printf ")\n";

  (* Главный инвариант: zeros по каждому ключу суммируются как в baseline. *)
  let all_keys = ref [] in
  Hashtbl.iter (fun k _ -> all_keys := k :: !all_keys) baseline_zeros;
  List.iter (fun key ->
    let base = try Hashtbl.find baseline_zeros key with Not_found -> 0 in
    let p1 = try Hashtbl.find phase1_zeros key with Not_found -> 0 in
    let p2 = try Hashtbl.find phase2_zeros key with Not_found -> 0 in
    check (Printf.sprintf "%s: phase1+phase2 (%d+%d=%d) = baseline (%d)"
             key p1 p2 (p1+p2) base)
      (p1 + p2 = base)
  ) !all_keys;

  let total_with_crash = phase1_total + phase2_total in
  check (Printf.sprintf "total zeros: with crash %d = baseline %d"
           total_with_crash baseline_total)
    (total_with_crash = baseline_total);

  Printf.printf "\nE2E silence_age persistence test passed.\n"
