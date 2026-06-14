(** Step 2 sanity test: backend получает данные при transitions
    state-машины триггера. Этот тест проверяет ТОЛЬКО запись —
    restore-логика будет в Step 3. *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

(* Простой тестовый alert *)
type alert =
  | Above of string * float * Time.t
  | Resolved of string * Time.t

let alert_to_json = function
  | Above (k, v, t) ->
    `Assoc [("tag", `String "above");
            ("key", `String k); ("v", `Float v); ("ts", `Int t)]
  | Resolved (k, t) ->
    `Assoc [("tag", `String "resolved");
            ("key", `String k); ("ts", `Int t)]

let alert_of_json = function
  | `Assoc kv ->
    let tag = List.assoc "tag" kv |> Yojson.Safe.Util.to_string in
    (match tag with
     | "above" ->
       let k = List.assoc "key" kv |> Yojson.Safe.Util.to_string in
       let v = List.assoc "v"   kv |> Yojson.Safe.Util.to_number in
       let t = List.assoc "ts"  kv |> Yojson.Safe.Util.to_int in
       Above (k, v, t)
     | "resolved" ->
       let k = List.assoc "key" kv |> Yojson.Safe.Util.to_string in
       let t = List.assoc "ts"  kv |> Yojson.Safe.Util.to_int in
       Resolved (k, t)
     | _ -> failwith "unknown tag")
  | _ -> failwith "expected assoc"

let make_spec () =
  Trigger.create
    ~name:"test_above5"
    ~condition:(Trigger.greater_than 5.0)
    ~problem_for:(Time.seconds 10)
    ~severity:Trigger.Warning
    ~produce_alert:(fun ~key ~value ~ts -> Above (key, value, ts))
    ~produce_recovery:(fun ~key ~ts -> Resolved (key, ts))
    ~serialize_key:(fun k -> `String k)
    ~deserialize_key:(fun j -> Yojson.Safe.Util.to_string j)
    ~serialize_value:(fun v -> `Float v)
    ~deserialize_value:(fun j -> Yojson.Safe.Util.to_number j)
    ~serialize_alert:alert_to_json
    ~deserialize_alert:alert_of_json
    ()

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Trigger persistence — Step 2 (snapshot)\n";
  Printf.printf "==========================================\n";

  (* ── 1. Без backend поведение идентично (regression) ──────── *)
  Printf.printf "\n-- 1. No backend: existing behavior preserved\n";

  let spec = make_spec () in
  let evs = [
    Mf_event.data ("A", 7.0) 0;
    Mf_event.wm 20_000;       (* фаер problem-таймера на 10000 *)
  ] in
  let stream = evs |> Stream.of_list |> Trigger.of_stream spec in
  let data_count = ref 0 in
  let rec loop () = match stream () with
    | None -> ()
    | Some (Mf_event.Data _) -> incr data_count; loop ()
    | Some _ -> loop ()
  in loop ();
  check (Printf.sprintf "no backend: 1 Data (got %d)" !data_count)
    (!data_count = 1);

  (* ── 2. С backend: alert эмитится + backend получает запись ── *)
  Printf.printf "\n-- 2. With backend: writes happen on transitions\n";

  let tbl = Hashtbl.create 16 in
  let backend = Trigger.backend_of_memory tbl in
  let evs = [
    Mf_event.data ("A", 7.0) 0;
    Mf_event.wm 20_000;
  ] in
  let stream = evs |> Stream.of_list |> Trigger.of_stream ~backend spec in
  let data_count = ref 0 in
  let rec loop () = match stream () with
    | None -> ()
    | Some (Mf_event.Data _) -> incr data_count; loop ()
    | Some _ -> loop ()
  in loop ();
  check "alert still emitted with backend" (!data_count = 1);

  let keys = Hashtbl.fold (fun k _ a -> k :: a) tbl [] in
  check (Printf.sprintf "backend has %d key(s) under trigger:test_above5:"
           (List.length keys))
    (List.length keys >= 1);

  let key_a_opt = List.find_opt
    (fun k -> String.length k > 0 &&
              k = "trigger:test_above5:\"A\"") keys in
  (match key_a_opt with
   | Some k ->
     let v_bytes = Hashtbl.find tbl k in
     let json = Yojson.Safe.from_string (Bytes.to_string v_bytes) in
     (match json with
      | `Assoc kv ->
        let state = List.assoc "state" kv in
        (match state with
         | `Assoc skv ->
           let tag = List.assoc "tag" skv |> Yojson.Safe.Util.to_string in
           check (Printf.sprintf "state.tag = %s (expected 'problem')" tag)
             (tag = "problem")
         | _ -> fail "state not assoc")
      | _ -> fail "json not assoc")
   | None ->
     Printf.printf "  available keys: %s\n" (String.concat ", " keys);
     fail "no key for A");

  (* ── 3. Missing serializer → Invalid_argument ─────────────── *)
  Printf.printf "\n-- 3. Missing serializer raises on of_stream\n";

  let incomplete_spec = Trigger.create
    ~name:"incomplete"
    ~condition:(Trigger.greater_than 5.0)
    ~severity:Trigger.Warning
    ~produce_alert:(fun ~key ~value ~ts -> Above (key, value, ts))
    ~produce_recovery:(fun ~key ~ts -> Resolved (key, ts))
    ~serialize_value:(fun v -> `Float v)
    (* Намеренно НЕ передаём остальные сериализаторы *)
    () in
  let tbl2 = Hashtbl.create 16 in
  let backend2 = Trigger.backend_of_memory tbl2 in
  (try
    let _stream = Trigger.of_stream ~backend:backend2 incomplete_spec Stream.empty in
    fail "should have raised Invalid_argument"
  with Invalid_argument msg ->
    pass (Printf.sprintf "raised: %s" (String.sub msg 0 (min 60 (String.length msg)))));

  Printf.printf "\nStep 2 tests passed.\n"
