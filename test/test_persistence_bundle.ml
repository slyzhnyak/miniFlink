(** silence_age persistence — ОРТОГОНАЛЬНАЯ модель.

    (Файл исторически тестировал bundle-API для трёх операторов;
    window_fold/process_keyed/silence_age переехали на
    Runtime_context + Managed_state, поэтому здесь — silence_age в
    новой модели: тот же пайплайн с persistence и без, режим снаружи.) *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let silence_pipe events =
  events |> Stream.of_list
  |> Item.silence_age ~name:"test_sa" ~by:string_of_int ~tick:(Time.seconds 10)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  silence_age — orthogonal persistence\n";
  Printf.printf "==========================================\n";

  (* ── 1. Ephemeral: эмитит, ничего не пишет ─────────────────── *)
  Printf.printf "\n-- 1. ephemeral\n";
  let events = [ Mf_event.data 1 0; Mf_event.data 2 1000; Mf_event.wm 5000 ] in
  let outs = Pipe.collect (silence_pipe events) in
  check "ephemeral: emits outputs" (List.length outs > 0);

  (* ── 2. Durable: backend получает записи ───────────────────── *)
  Printf.printf "\n-- 2. durable: state persisted\n";
  let tbl = Hashtbl.create 16 in
  let backend = Persistence_backend.of_memory tbl in
  let ctx = Runtime_context.durable backend in
  Runtime_context.with_context ctx (fun () ->
    let evs = [ Mf_event.data 42 0; Mf_event.wm 30_000 ] in
    Pipe.iter_data (fun _ -> ()) (silence_pipe evs);
    check "durable: backend has records" (Hashtbl.length tbl > 0));

  Printf.printf "\nAll silence_age persistence tests passed.\n"
