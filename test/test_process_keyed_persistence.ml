(** process_keyed persistence — ОРТОГОНАЛЬНАЯ модель.

    Тот же пайплайн работает с persistence и без; режим — снаружи
    через Runtime_context.with_context. Проверяем ПОВЕДЕНИЕ: ephemeral
    как раньше, durable переживает рестарт (restore продолжает
    счётчик и таймеры). *)

open Miniflink

module Int_keyed : Keyed.S with type t = string * int = struct
  type t = string * int
  let key (k, _) = k
end

type counter_state = { mutable count : int }

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

(* Пайплайн: считает события per ключ, эмитит когда count кратен 3.
   ~name задаёт стабильный namespace состояния. *)
let counter_pipe events =
  events |> Stream.of_list
  |> Pipe.process_keyed (module Int_keyed)
       ~name:"counter"
       ~init:(fun () -> { count = 0 })
       ~on_event:(fun ctx _key st _ev ->
         st.count <- st.count + 1;
         if st.count mod 3 = 0 then ctx.emit st.count)
       ~on_timer:(fun _ _ _ _ _ -> ())

let collect stream =
  let outs = ref [] in
  let rec drain () = match stream () with
    | None -> ()
    | Some (Mf_event.Data (n, _)) -> outs := n :: !outs; drain ()
    | Some _ -> drain ()
  in drain (); List.rev !outs

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  process_keyed — orthogonal persistence\n";
  Printf.printf "==========================================\n";

  (* ── 1. Ephemeral: как раньше ──────────────────────────────── *)
  Printf.printf "\n-- 1. Ephemeral: existing behavior\n";
  let events = [
    Mf_event.data ("A", 1) 0;
    Mf_event.data ("A", 2) 1000;
    Mf_event.data ("A", 3) 2000;
    Mf_event.data ("A", 4) 3000;
    Mf_event.wm 4000;
  ] in
  check "ephemeral: emits at count=3" (collect (counter_pipe events) = [3]);

  (* ── 2. Durable: backend получает записи ───────────────────── *)
  Printf.printf "\n-- 2. Durable: state persisted\n";
  let tbl = Hashtbl.create 16 in
  let backend = Persistence_backend.of_memory tbl in
  let ctx = Runtime_context.durable backend in
  Runtime_context.with_context ctx (fun () ->
    let evs = [ Mf_event.data ("X", 1) 0; Mf_event.data ("X", 2) 100;
                Mf_event.wm 1000 ] in
    ignore (collect (counter_pipe evs));
    check "durable: backend has record for X" (Hashtbl.length tbl >= 1));

  (* ── 3. Restore: счётчик продолжается через рестарт ────────── *)
  Printf.printf "\n-- 3. Restore: counter continues across restart\n";
  let tbl3 = Hashtbl.create 16 in
  let backend3 = Persistence_backend.of_memory tbl3 in
  let ctx3 = Runtime_context.durable backend3 in
  (* Phase 1: 2 события (count=2, ещё не эмитит) *)
  Runtime_context.with_context ctx3 (fun () ->
    let p1 = [ Mf_event.data ("Y", 1) 0; Mf_event.data ("Y", 2) 100;
               Mf_event.wm 1000 ] in
    check "phase 1: no emit yet (count=2)" (collect (counter_pipe p1) = []));
  (* Phase 2: новый instance, тот же backend → restore count=2.
     Ещё одно событие → count=3 → эмит. Без restore было бы count=1. *)
  let outs2 = Runtime_context.with_context ctx3 (fun () ->
    let p2 = [ Mf_event.data ("Y", 3) 2000; Mf_event.wm 3000 ] in
    collect (counter_pipe p2)) in
  check "restore: emits 3 (count restored to 2, +1)" (outs2 = [3]);

  Printf.printf "\nAll process_keyed persistence tests passed.\n"
