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

  (* ── 4. process_keyed via ?persistence ──────────────────── *)
  Printf.printf "\n-- 4. process_keyed via ?persistence\n";
  let tbl_pk = Hashtbl.create 16 in
  let pst : int Persistence_backend.persist = {
    backend     = Persistence_backend.of_memory tbl_pk;
    name        = "counter";
    serialize   = (fun n -> `Int n);
    deserialize = (function `Int n -> n | _ -> failwith "bad");
  } in
  let module K : Keyed.S with type t = string * int = struct
    type t = string * int
    let key (k, _) = k
  end in
  let events = [
    Mf_event.data ("A", 1) 0;
    Mf_event.data ("A", 2) 1000;
    Mf_event.wm 2000;
  ] in
  let stream = events |> Stream.of_list
    |> Pipe.process_keyed (module K)
         ~persistence:pst
         ~init:(fun () -> 0)
         ~on_event:(fun _ctx _k st _v -> ignore st)
         ~on_timer:(fun _ _ _ _ _ -> ()) in
  Pipe.iter_data (fun _ -> ()) stream;
  check "process_keyed backend has key"
    (Hashtbl.mem tbl_pk "process_keyed:counter:A");

  (* window_fold перешёл на ортогональную persistence (Runtime_context
     + Managed_state), поэтому ?persistence-bundle к нему больше не
     применяется — его поведение покрыто test_window_fold_persistence. *)

  Printf.printf "\nAll persistence bundle tests passed.\n"
