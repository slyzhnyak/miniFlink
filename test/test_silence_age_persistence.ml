(** Тесты persistence для Item.silence_age.

    Структура аналогична test_trigger_persistence.ml:
    1. Без backend — regression (поведение идентично)
    2. С backend — записи появляются
    3. Missing parameter → Invalid_argument
    4. Restore: state переживает создание нового instance'а
    5. Pending timer пересоздаётся из restore *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

(* По умолчанию silence_age работает с любым типом — берём int. *)
let ser_key (k : int) : Yojson.Safe.t = `Int k
let deser_key (j : Yojson.Safe.t) : int = Yojson.Safe.Util.to_int j

(* Помощник: прогнать stream до конца, собрать (key, age) пары. *)
let collect_ages stream =
  let acc = ref [] in
  let rec loop () = match stream () with
    | None -> ()
    | Some (Mf_event.Data ((k, age), _ts)) ->
      acc := (k, age) :: !acc; loop ()
    | Some _ -> loop ()
  in loop ();
  List.rev !acc

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Item.silence_age — persistence\n";
  Printf.printf "==========================================\n";

  (* ── 1. Без backend — regression ──────────────────────────── *)
  Printf.printf "\n-- 1. No backend: existing behavior\n";

  let events = [
    Mf_event.data 7 0;            (* key=7 at t=0 *)
    Mf_event.wm 30_000;           (* tick должен сработать на 10000 *)
    Mf_event.wm 60_000;
  ] in
  let stream =
    events |> Stream.of_list
    |> Item.silence_age ~by:(fun k -> k) ~tick:(Time.seconds 10) in
  let pairs = collect_ages stream in
  Printf.printf "  no backend: %d emissions\n" (List.length pairs);
  check "no backend: at least 3 emissions (1 zero + 2 ticks)"
    (List.length pairs >= 3);
  check "first is (7, 0)" (List.hd pairs = (7, 0));

  (* ── 2. С backend — записи появляются ─────────────────────── *)
  Printf.printf "\n-- 2. With backend: writes happen\n";

  let tbl = Hashtbl.create 16 in
  let backend = Persistence_backend.of_memory tbl in
  let events2 = [
    Mf_event.data 42 0;
    Mf_event.wm 30_000;
  ] in
  let stream2 =
    events2 |> Stream.of_list
    |> Item.silence_age
         ~backend
         ~backend_name:"test_no_packets"
         ~serialize_key:ser_key
         ~deserialize_key:deser_key
         ~by:(fun k -> k) ~tick:(Time.seconds 10) in
  let _ = collect_ages stream2 in

  let keys_in_tbl = Hashtbl.fold (fun k _ a -> k :: a) tbl [] in
  Printf.printf "  backend has %d key(s): %s\n"
    (List.length keys_in_tbl)
    (String.concat ", " keys_in_tbl);
  check "backend has 1 record" (List.length keys_in_tbl = 1);
  let bk_42 = "item:silence_age:test_no_packets:42" in
  check (Printf.sprintf "key is %s" bk_42)
    (Hashtbl.mem tbl bk_42);

  let v_bytes = Hashtbl.find tbl bk_42 in
  let json = Yojson.Safe.from_string (Bytes.to_string v_bytes) in
  (match json with
   | `Assoc kv ->
     let last_seen = List.assoc "last_seen_ts" kv |> Yojson.Safe.Util.to_int in
     let fire_at   = List.assoc "fire_at" kv |> Yojson.Safe.Util.to_int in
     check (Printf.sprintf "last_seen=%d" last_seen) (last_seen = 0);
     (* fire_at = либо 10000 (initial), либо 40000 (после одного тика) *)
     check (Printf.sprintf "fire_at=%d (10000 or 40000)" fire_at)
       (fire_at = 10000 || fire_at = 40000)
   | _ -> fail "json not assoc");

  (* ── 3. Missing parameters → Invalid_argument ─────────────── *)
  Printf.printf "\n-- 3. Missing parameter raises\n";

  let tbl_x = Hashtbl.create 4 in
  let backend_x = Persistence_backend.of_memory tbl_x in
  (* backend есть, backend_name нет *)
  (try
    let _s = Item.silence_age
      ~backend:backend_x
      ~serialize_key:ser_key
      ~deserialize_key:deser_key
      ~by:(fun k -> k) ~tick:(Time.seconds 10)
      Stream.empty in
    fail "should have raised Invalid_argument"
  with Invalid_argument msg ->
    pass (Printf.sprintf "raised: %s"
            (String.sub msg 0 (min 60 (String.length msg)))));

  (* ── 4. Restore: state переживает создание нового instance'а  *)
  Printf.printf "\n-- 4. Restore: new silence_age picks up state\n";

  let tbl3 = Hashtbl.create 16 in
  let backend3 = Persistence_backend.of_memory tbl3 in

  (* Phase 1: подаём событие для key=99 в t=0, watermark 5000.
     Таймер был запланирован на t=10000, ещё не сработал.
     В backend: last_seen=0, fire_at=10000. *)
  let p1 = [
    Mf_event.data 99 0;
    Mf_event.wm 5_000;
  ] in
  let s1 = p1 |> Stream.of_list
    |> Item.silence_age
         ~backend:backend3
         ~backend_name:"recovery_test"
         ~serialize_key:ser_key
         ~deserialize_key:deser_key
         ~by:(fun k -> k) ~tick:(Time.seconds 10) in
  let pairs1 = collect_ages s1 in
  check "phase 1: 1 emission (zero only, timer not fired)"
    (List.length pairs1 = 1);

  (* Phase 2: новый silence_age с тем же backend. Должен подгрузить
     state и зарегистрировать таймер. На watermark 15000 — таймер
     срабатывает, эмитит (99, 15000) (age = 15000 - 0). *)
  let p2 = [ Mf_event.wm 15_000 ] in
  let s2 = p2 |> Stream.of_list
    |> Item.silence_age
         ~backend:backend3
         ~backend_name:"recovery_test"
         ~serialize_key:ser_key
         ~deserialize_key:deser_key
         ~by:(fun k -> k) ~tick:(Time.seconds 10) in
  let pairs2 = collect_ages s2 in
  Printf.printf "  phase 2: %d emissions: %s\n"
    (List.length pairs2)
    (String.concat ", "
       (List.map (fun (k, a) -> Printf.sprintf "(%d,%d)" k a) pairs2));
  check "phase 2: timer fires after restore (>= 1 emission)"
    (List.length pairs2 >= 1);
  check "phase 2: age >= 15000 (was silent 15s)"
    (match pairs2 with
     | (k, age) :: _ -> k = 99 && age >= 15000
     | _ -> false);

  (* ── 5. Изоляция между разными backend_name ───────────────── *)
  Printf.printf "\n-- 5. Different backend_names don't collide\n";

  let tbl4 = Hashtbl.create 16 in
  let backend4 = Persistence_backend.of_memory tbl4 in

  let p_first = [ Mf_event.data 1 0; Mf_event.wm 1000 ] in
  let p_second = [ Mf_event.data 2 0; Mf_event.wm 1000 ] in

  let _ = collect_ages (p_first |> Stream.of_list
    |> Item.silence_age
         ~backend:backend4 ~backend_name:"alpha"
         ~serialize_key:ser_key ~deserialize_key:deser_key
         ~by:(fun k -> k) ~tick:(Time.seconds 10)) in
  let _ = collect_ages (p_second |> Stream.of_list
    |> Item.silence_age
         ~backend:backend4 ~backend_name:"beta"
         ~serialize_key:ser_key ~deserialize_key:deser_key
         ~by:(fun k -> k) ~tick:(Time.seconds 10)) in

  let keys = Hashtbl.fold (fun k _ a -> k :: a) tbl4 [] in
  Printf.printf "  total keys: %d (%s)\n"
    (List.length keys) (String.concat ", " (List.sort compare keys));
  check "2 distinct keys under different namespaces"
    (List.length keys = 2);
  check "alpha namespace exists"
    (List.exists (fun k -> String.length k >= 24
                            && String.sub k 0 24 = "item:silence_age:alpha:1") keys);
  check "beta namespace exists"
    (List.exists (fun k -> String.length k >= 23
                            && String.sub k 0 23 = "item:silence_age:beta:2") keys);

  Printf.printf "\nAll silence_age persistence tests passed.\n"
