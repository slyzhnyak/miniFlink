(** window_fold persistence — ОРТОГОНАЛЬНАЯ модель.

    Пайплайн НЕ меняется между «без persistence» и «с persistence»:
    тот же вызов window_fold, режим задаётся снаружи через
    Runtime_context.with_context. Здесь проверяем ПОВЕДЕНИЕ (а не
    формат байтов в backend): ephemeral работает как раньше, durable
    переживает рестарт (restore продолжает накопление), FFired-окно с
    late-событием после рестарта даёт атомарный Update. *)

open Miniflink

module Int_keyed : Keyed.S with type t = string * int = struct
  type t = string * int
  let key (k, _) = k
end

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let sum_window events =
  events |> Stream.of_list
  |> Pipe.window_fold (module Int_keyed)
       (Pipe.tumbling (Time.seconds 10))
       ~init:(fun () -> 0)
       ~add:(fun acc (_, n) -> acc + n)

let sum_window_late events =
  events |> Stream.of_list
  |> Pipe.window_fold (module Int_keyed)
       ~allowed_lateness:(Time.seconds 30)
       (Pipe.tumbling (Time.seconds 10))
       ~init:(fun () -> 0)
       ~add:(fun acc (_, n) -> acc + n)

let collect_data stream =
  let outs = ref [] in
  let rec drain () = match stream () with
    | None -> ()
    | Some (Mf_event.Data (v, _)) -> outs := v :: !outs; drain ()
    | Some _ -> drain ()
  in drain (); List.rev !outs

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  window_fold — orthogonal persistence\n";
  Printf.printf "==========================================\n";

  (* ── 1. Ephemeral (вне durable-контекста) — как раньше ─────── *)
  Printf.printf "\n-- 1. Ephemeral: existing behavior\n";
  let events = [
    Mf_event.data ("A", 10) 0;
    Mf_event.data ("A", 20) 1000;
    Mf_event.data ("A", 30) 2000;
    Mf_event.wm 15_000;
  ] in
  check "ephemeral: 1 emit (A, 60)" (collect_data (sum_window events) = [("A", 60)]);

  (* ── 2. Durable: backend получает записи на watermark ──────── *)
  Printf.printf "\n-- 2. Durable: state persisted on watermark\n";
  let tbl = Hashtbl.create 16 in
  let backend = Persistence_backend.of_memory tbl in
  let ctx = Runtime_context.durable backend in
  Runtime_context.with_context ctx (fun () ->
    let events2 = [
      Mf_event.data ("X", 5) 0;
      Mf_event.data ("X", 7) 1000;
      Mf_event.wm 5000;   (* окно [0,10s) ещё открыто, snapshot записан *)
    ] in
    let _ = collect_data (sum_window_late events2) in
    check "durable: backend has >=1 record" (Hashtbl.length tbl >= 1));

  (* ── 3. Restore: открытое окно продолжает накопление ──────── *)
  Printf.printf "\n-- 3. Restore: open window continues accumulator\n";
  let tbl3 = Hashtbl.create 16 in
  let backend3 = Persistence_backend.of_memory tbl3 in
  let ctx3 = Runtime_context.durable backend3 in
  (* Phase 1: sum=15, wm только до 5000 — окно не закрылось *)
  Runtime_context.with_context ctx3 (fun () ->
    let p1 = [ Mf_event.data ("Y", 5) 0;
               Mf_event.data ("Y", 10) 1000;
               Mf_event.wm 5000 ] in
    ignore (collect_data (sum_window p1)));
  (* Phase 2: новый instance, тот же backend → restore. Итог 15+25=40 *)
  let outs2 = Runtime_context.with_context ctx3 (fun () ->
    let p2 = [ Mf_event.data ("Y", 25) 3000; Mf_event.wm 15_000 ] in
    collect_data (sum_window p2)) in
  check "restore: window emits (Y, 40) = 15+25, not 25" (outs2 = [("Y", 40)]);

  (* ── 4. Restore FFired + late → атомарный Update ───────────── *)
  Printf.printf "\n-- 4. Restore FFired: late event re-emits as atomic Update\n";
  let tbl4 = Hashtbl.create 16 in
  let backend4 = Persistence_backend.of_memory tbl4 in
  let ctx4 = Runtime_context.durable backend4 in
  (* Phase 1: окно закрывается (FFired в backend) *)
  let outs1 = Runtime_context.with_context ctx4 (fun () ->
    let p1 = [ Mf_event.data ("Z", 100) 0;
               Mf_event.data ("Z", 200) 1000;
               Mf_event.wm 15_000 ] in
    collect_data (sum_window_late p1)) in
  check "phase 1: emit (Z, 300)" (outs1 = [("Z", 300)]);
  (* Phase 2: late event (ts=5000 ∈ [0,10000)), окно ещё FFired в
     backend (в пределах allowed_lateness). restore → late применяется
     → атомарный Update 300→350. *)
  let updates2 = Runtime_context.with_context ctx4 (fun () ->
    let p2 = [ Mf_event.data ("Z", 50) 5000; Mf_event.wm 20_000 ] in
    let s2 = p2 |> Stream.of_list
      |> Pipe.window_fold (module Int_keyed)
           ~allowed_lateness:(Time.seconds 30)
           (Pipe.tumbling (Time.seconds 10))
           ~init:(fun () -> 0)
           ~add:(fun acc (_, n) -> acc + n) in
    let acc = ref [] in
    let rec drain () = match s2 () with
      | None -> ()
      | Some (Mf_event.Update { old; new_value; _ }) ->
        acc := (old, new_value) :: !acc; drain ()
      | Some _ -> drain ()
    in drain (); List.rev !acc) in
  check "phase 2: late event → atomic Update (Z,300)→(Z,350)"
    (updates2 = [(("Z", 300), ("Z", 350))]);

  Printf.printf "\nAll window_fold persistence tests passed.\n"
