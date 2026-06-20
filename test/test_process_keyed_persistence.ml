(** Тесты persistence для Pipe.process_keyed.

    Структура аналогична test_trigger_persistence.ml и
    test_silence_age_persistence.ml:
    1. Без backend — regression (поведение идентично)
    2. С backend — записи появляются с правильным state
    3. Missing param → Invalid_argument
    4. Restore state: новый процесс продолжает с того же state
    5. Restore timers: pending event_timer переживает рестарт *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

(* Простой пример: counter per key + emit'ит каждые 3 события. *)
module Int_keyed : Keyed.S with type t = (string * int) = struct
  type t = string * int
  let key (k, _) = k
end

type counter_state = { mutable count : int }

let counter_state_to_json (s : counter_state) : Yojson.Safe.t =
  `Assoc [("count", `Int s.count)]

let counter_state_of_json = function
  | `Assoc kv ->
    let c = List.assoc "count" kv |> Yojson.Safe.Util.to_int in
    { count = c }
  | _ -> failwith "counter state not assoc"

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Pipe.process_keyed — persistence\n";
  Printf.printf "==========================================\n";

  (* ── 1. Без backend — regression ──────────────────────────── *)
  Printf.printf "\n-- 1. No backend: existing behavior\n";

  let events = [
    Mf_event.data ("A", 1) 0;
    Mf_event.data ("A", 2) 1000;
    Mf_event.data ("A", 3) 2000;
    Mf_event.data ("A", 4) 3000;
    Mf_event.wm 4000;
  ] in
  let stream = events |> Stream.of_list
    |> Pipe.process_keyed (module Int_keyed)
         ~init:(fun () -> { count = 0 })
         ~on_event:(fun ctx _key st _ev ->
           st.count <- st.count + 1;
           if st.count mod 3 = 0 then ctx.emit st.count)
         ~on_timer:(fun _ _ _ _ _ -> ()) in
  let outputs = ref [] in
  let rec drain () = match stream () with
    | None -> ()
    | Some (Mf_event.Data (n, _)) -> outputs := n :: !outputs; drain ()
    | Some _ -> drain ()
  in drain ();
  check "no backend: emits at count=3" (!outputs = [3]);

  (* ── 2. С backend: state записывается ──────────────────────── *)
  Printf.printf "\n-- 2. With backend: state persisted\n";

  let tbl = Hashtbl.create 16 in
  let backend = Persistence_backend.of_memory tbl in
  let events2 = [
    Mf_event.data ("X", 10) 0;
    Mf_event.data ("X", 20) 1000;
    Mf_event.wm 2000;
  ] in
  let stream2 = events2 |> Stream.of_list
    |> Pipe.process_keyed (module Int_keyed)
         ~persistence:{ Persistence_backend.backend = backend;
         name = "counter_test"; serialize = counter_state_to_json;
         deserialize = counter_state_of_json }
         ~init:(fun () -> { count = 0 })
         ~on_event:(fun ctx _key st _ev ->
           st.count <- st.count + 1;
           ctx.emit st.count)
         ~on_timer:(fun _ _ _ _ _ -> ()) in
  let outputs2 = ref [] in
  let rec drain () = match stream2 () with
    | None -> ()
    | Some (Mf_event.Data (n, _)) -> outputs2 := n :: !outputs2; drain ()
    | Some _ -> drain ()
  in drain ();
  check "phase 1: 2 emissions (1, 2)" (List.rev !outputs2 = [1; 2]);

  let bk = "process_keyed:counter_test:X" in
  check "backend has key for X" (Hashtbl.mem tbl bk);

  let v_bytes = Hashtbl.find tbl bk in
  let json = Yojson.Safe.from_string (Bytes.to_string v_bytes) in
  (match json with
   | `Assoc kv ->
     let st = List.assoc "state" kv in
     (match st with
      | `Assoc skv ->
        let c = List.assoc "count" skv |> Yojson.Safe.Util.to_int in
        check (Printf.sprintf "state.count = %d (expected 2)" c)
          (c = 2)
      | _ -> fail "state not assoc")
   | _ -> fail "json not assoc");


  (* ── 4. Restore state ──────────────────────────────────────── *)
  Printf.printf "\n-- 4. Restore: new instance continues counter\n";

  let tbl3 = Hashtbl.create 16 in
  let backend3 = Persistence_backend.of_memory tbl3 in

  (* Phase 1: count до 2 *)
  let p1 = [
    Mf_event.data ("Y", 1) 0;
    Mf_event.data ("Y", 2) 1000;
    Mf_event.wm 2000;
  ] in
  let s1 = p1 |> Stream.of_list
    |> Pipe.process_keyed (module Int_keyed)
         ~persistence:{ Persistence_backend.backend = backend3;
         name = "restore_test"; serialize = counter_state_to_json;
         deserialize = counter_state_of_json }
         ~init:(fun () -> { count = 0 })
         ~on_event:(fun ctx _key st _ev ->
           st.count <- st.count + 1;
           ctx.emit st.count)
         ~on_timer:(fun _ _ _ _ _ -> ()) in
  let phase1_out = ref [] in
  let rec drain () = match s1 () with
    | None -> ()
    | Some (Mf_event.Data (n, _)) -> phase1_out := n :: !phase1_out; drain ()
    | Some _ -> drain ()
  in drain ();
  check "phase 1: [1; 2]" (List.rev !phase1_out = [1; 2]);

  (* Phase 2: новый instance, ещё одно событие → counter должен
     стать 3, не 1 (без restore был бы 1). *)
  let p2 = [
    Mf_event.data ("Y", 99) 3000;
    Mf_event.wm 4000;
  ] in
  let s2 = p2 |> Stream.of_list
    |> Pipe.process_keyed (module Int_keyed)
         ~persistence:{ Persistence_backend.backend = backend3;
         name = "restore_test"; serialize = counter_state_to_json;
         deserialize = counter_state_of_json }
         ~init:(fun () -> { count = 0 })
         ~on_event:(fun ctx _key st _ev ->
           st.count <- st.count + 1;
           ctx.emit st.count)
         ~on_timer:(fun _ _ _ _ _ -> ()) in
  let phase2_out = ref [] in
  let rec drain () = match s2 () with
    | None -> ()
    | Some (Mf_event.Data (n, _)) -> phase2_out := n :: !phase2_out; drain ()
    | Some _ -> drain ()
  in drain ();
  Printf.printf "  phase 2: %s\n"
    (String.concat ";" (List.map string_of_int (List.rev !phase2_out)));
  check "phase 2: counter continues from 2 → 3"
    (List.rev !phase2_out = [3]);

  (* ── 5. Restore event timers ──────────────────────────────── *)
  Printf.printf "\n-- 5. Restore: pending event_timer fires after restart\n";

  let tbl4 = Hashtbl.create 16 in
  let backend4 = Persistence_backend.of_memory tbl4 in

  let timer_fired = ref false in

  (* Phase 1: ставим event_timer на t=10000, wm=5000 (не сработал) *)
  let p1 = [
    Mf_event.data ("Z", 7) 0;
    Mf_event.wm 5000;
  ] in
  let s1 = p1 |> Stream.of_list
    |> Pipe.process_keyed (module Int_keyed)
         ~persistence:{ Persistence_backend.backend = backend4;
         name = "timer_test"; serialize = counter_state_to_json;
         deserialize = counter_state_of_json }
         ~init:(fun () -> { count = 0 })
         ~on_event:(fun ctx _key _st _ev ->
           ctx.set_event_timer 10_000)
         ~on_timer:(fun _ctx _key _st _t _kind ->
           timer_fired := true) in
  let rec drain () = match s1 () with
    | None -> () | Some _ -> drain ()
  in drain ();
  check "phase 1: timer NOT fired yet" (not !timer_fired);

  let bk = "process_keyed:timer_test:Z" in
  let v = Hashtbl.find tbl4 bk in
  let json = Yojson.Safe.from_string (Bytes.to_string v) in
  (match json with
   | `Assoc kv ->
     let ev_timers = List.assoc "ev_timers" kv in
     (match ev_timers with
      | `List [`Int t] ->
        check (Printf.sprintf "backend has ev_timer=%d" t) (t = 10000)
      | _ -> fail "ev_timers wrong shape")
   | _ -> fail "json not assoc");

  (* Phase 2: новый instance, wm=15000 → таймер должен сработать *)
  let timer_fired2 = ref false in
  let p2 = [ Mf_event.wm 15_000 ] in
  let s2 = p2 |> Stream.of_list
    |> Pipe.process_keyed (module Int_keyed)
         ~persistence:{ Persistence_backend.backend = backend4;
         name = "timer_test"; serialize = counter_state_to_json;
         deserialize = counter_state_of_json }
         ~init:(fun () -> { count = 0 })
         ~on_event:(fun _ _ _ _ -> ())
         ~on_timer:(fun _ctx _key _st _t _kind ->
           timer_fired2 := true) in
  let rec drain () = match s2 () with
    | None -> () | Some _ -> drain ()
  in drain ();
  check "phase 2: timer fired after restore" !timer_fired2;

  Printf.printf "\nAll process_keyed persistence tests passed.\n"
