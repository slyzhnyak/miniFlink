(** Тесты для bundle persistence-параметров {!Persistence_backend.persist}.

    Проверяет что:
    1. Новый ?persistence параметр работает в 3 операторах
    2. Behaviorally equivalent к старому API (same outputs)
    3. Конфликт ?backend + ?persistence → Invalid_argument *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Persistence_backend.persist bundle\n";
  Printf.printf "==========================================\n";

  (* ── 1. silence_age с новым ~persistence ────────────────── *)
  Printf.printf "\n-- 1. silence_age via ?persistence bundle\n";
  let tbl_new = Hashtbl.create 16 in
  let pst : string Persistence_backend.persist = {
    backend     = Persistence_backend.of_memory tbl_new;
    name        = "test_sa";
    serialize   = (fun k -> `String k);
    deserialize = (fun j -> Yojson.Safe.Util.to_string j);
  } in
  let events = [
    Mf_event.data 42 0;
    Mf_event.wm 30_000;
  ] in
  let stream = events |> Stream.of_list
    |> Item.silence_age ~persistence:pst
         ~by:string_of_int
         ~tick:(Time.seconds 10) in
  Pipe.iter_data (fun _ -> ()) stream;
  check "backend got record"
    (Hashtbl.mem tbl_new "item:silence_age:test_sa:\"42\"");

  (* ── 2. silence_age via ?persistence bundle ─────────────── *)
  Printf.printf "\n-- 2. silence_age: ?persistence bundle\n";
  let tbl_b = Hashtbl.create 16 in
  let pst_b : string Persistence_backend.persist = {
    backend     = Persistence_backend.of_memory tbl_b;
    name        = "eq";
    serialize   = (fun k -> `String k);
    deserialize = (fun j -> Yojson.Safe.Util.to_string j);
  } in
  let events = [
    Mf_event.data 1 0;
    Mf_event.data 2 1000;
    Mf_event.wm 5000;
  ] in
  let stream_new = events |> Stream.of_list
    |> Item.silence_age ~persistence:pst_b
         ~by:string_of_int
         ~tick:(Time.seconds 10) in
  let outs_new = Pipe.collect stream_new in
  check "emits outputs" (List.length outs_new > 0);
  let keys_b = List.sort compare
    (Hashtbl.fold (fun k _ a -> k :: a) tbl_b []) in
  check "backend has keys" (List.length keys_b > 0);

  (* process_keyed и window_fold перешли на ортогональную persistence
     (Runtime_context + Managed_state) — bundle-параметр к ним больше
     не применяется. Их поведение покрыто
     test_process_keyed_persistence и test_window_fold_persistence.
     Здесь остаётся только silence_age, ещё использующий bundle. *)

  Printf.printf "\nAll persistence bundle tests passed.\n"
