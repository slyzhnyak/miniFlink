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

  (* ── 2. silence_age equivalence: new == old API ─────────── *)
  Printf.printf "\n-- 2. silence_age: ?persistence ≡ ?backend\n";
  let tbl_a = Hashtbl.create 16 in
  let backend_a = Persistence_backend.of_memory tbl_a in
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
  let stream_old = events |> Stream.of_list
    |> Item.silence_age
         ~backend:backend_a ~backend_name:"eq"
         ~serialize_key:(fun k -> `String k)
         ~deserialize_key:(fun j -> Yojson.Safe.Util.to_string j)
         ~by:string_of_int
         ~tick:(Time.seconds 10) in
  let outs_old = Pipe.collect stream_old in
  let stream_new = events |> Stream.of_list
    |> Item.silence_age ~persistence:pst_b
         ~by:string_of_int
         ~tick:(Time.seconds 10) in
  let outs_new = Pipe.collect stream_new in
  check "same outputs" (outs_old = outs_new);
  let keys_a = List.sort compare
    (Hashtbl.fold (fun k _ a -> k :: a) tbl_a []) in
  let keys_b = List.sort compare
    (Hashtbl.fold (fun k _ a -> k :: a) tbl_b []) in
  check "same backend keys" (keys_a = keys_b);

  (* ── 3. Конфликт ?persistence + ?backend → raise ────────── *)
  Printf.printf "\n-- 3. Mixing ?persistence with ?backend raises\n";
  let tbl = Hashtbl.create 4 in
  let bk = Persistence_backend.of_memory tbl in
  let pst : string Persistence_backend.persist = {
    backend     = bk; name = "x";
    serialize   = (fun k -> `String k);
    deserialize = (fun j -> Yojson.Safe.Util.to_string j);
  } in
  (try
    let s = Item.silence_age
      ~persistence:pst
      ~backend:bk     (* конфликт! *)
      ~by:string_of_int ~tick:(Time.seconds 10)
      Stream.empty in
    let _ = s () in  (* force eval *)
    fail "should have raised"
  with Invalid_argument _ ->
    pass "raised Invalid_argument on mixed API");

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

  (* ── 5. window_fold via ?persistence ────────────────────── *)
  Printf.printf "\n-- 5. window_fold via ?persistence\n";
  let tbl_wf = Hashtbl.create 16 in
  let pst : int Persistence_backend.persist = {
    backend     = Persistence_backend.of_memory tbl_wf;
    name        = "sum";
    serialize   = (fun n -> `Int n);
    deserialize = (function `Int n -> n | _ -> failwith "bad");
  } in
  let module K2 : Keyed.S with type t = string * int = struct
    type t = string * int
    let key (k, _) = k
  end in
  let events = [
    Mf_event.data ("X", 5) 0;
    Mf_event.data ("X", 7) 1000;
    Mf_event.wm 5000;
  ] in
  let stream = events |> Stream.of_list
    |> Pipe.window_fold (module K2)
         ~persistence:pst
         (Pipe.tumbling (Time.seconds 10))
         ~init:(fun () -> 0)
         ~add:(fun acc (_, n) -> acc + n) in
  Pipe.iter_data (fun _ -> ()) stream;
  check "window_fold backend has window record"
    (Hashtbl.mem tbl_wf "window_fold:sum:X:0:10000");

  Printf.printf "\nAll persistence bundle tests passed.\n"
