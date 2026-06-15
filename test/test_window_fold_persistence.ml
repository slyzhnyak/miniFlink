(** Тесты persistence для Pipe.window_fold.

    Структура аналогична другим persistence-тестам:
    1. Без backend — regression
    2. С backend — записи появляются с правильным state
    3. Missing parameter → Invalid_argument
    4. Restore: окно accumulator переживает рестарт
    5. Restore FFired: closed window state восстанавливается *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

(* Простой пример: считаем sum чисел per key per window. *)
module Int_keyed : Keyed.S with type t = (string * int) = struct
  type t = string * int
  let key (k, _) = k
end

let sum_acc_to_json (s : int) : Yojson.Safe.t = `Int s
let sum_acc_of_json = function
  | `Int n -> n
  | _ -> failwith "sum acc not int"

let collect_outputs stream =
  let outs = ref [] in
  let rec drain () = match stream () with
    | None -> ()
    | Some (Mf_event.Data (v, _)) -> outs := v :: !outs; drain ()
    | Some (Mf_event.Retract _) -> drain ()
    | Some _ -> drain ()
  in drain ();
  List.rev !outs

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Pipe.window_fold — persistence\n";
  Printf.printf "==========================================\n";

  (* ── 1. Без backend — regression ──────────────────────────── *)
  Printf.printf "\n-- 1. No backend: existing behavior\n";

  let events = [
    Mf_event.data ("A", 10) 0;
    Mf_event.data ("A", 20) 1000;
    Mf_event.data ("A", 30) 2000;
    Mf_event.wm 15_000;     (* закрывает окно [0..10s) *)
  ] in
  let stream = events |> Stream.of_list
    |> Pipe.window_fold (module Int_keyed)
         (Pipe.tumbling (Time.seconds 10))
         ~init:(fun () -> 0)
         ~add:(fun acc (_, n) -> acc + n) in
  let outs = collect_outputs stream in
  Printf.printf "  outputs: %s\n"
    (String.concat ", "
       (List.map (fun (k, v) -> Printf.sprintf "(%s,%d)" k v) outs));
  check "no backend: 1 emit (A, 60)" (outs = [("A", 60)]);

  (* ── 2. С backend ──────────────────────────────────────────── *)
  Printf.printf "\n-- 2. With backend: state persisted on watermark\n";

  let tbl = Hashtbl.create 16 in
  let backend = Persistence_backend.of_memory tbl in

  let events2 = [
    Mf_event.data ("X", 5) 0;
    Mf_event.data ("X", 7) 1000;
    Mf_event.wm 5000;       (* окно [0..10s) ещё открыто, но snapshot записан *)
  ] in
  let stream2 = events2 |> Stream.of_list
    |> Pipe.window_fold (module Int_keyed)
         ~backend ~backend_name:"sum_test"
         ~serialize_acc:sum_acc_to_json
         ~deserialize_acc:sum_acc_of_json
         (Pipe.tumbling (Time.seconds 10))
         ~init:(fun () -> 0)
         ~add:(fun acc (_, n) -> acc + n) in
  let _ = collect_outputs stream2 in

  let keys_in_tbl = Hashtbl.fold (fun k _ a -> k :: a) tbl [] in
  Printf.printf "  backend keys: %s\n" (String.concat ", " keys_in_tbl);
  check "backend has 1 record" (List.length keys_in_tbl = 1);

  let bk = "window_fold:sum_test:X:0:10000" in
  check (Printf.sprintf "key is %s" bk) (Hashtbl.mem tbl bk);

  let v_bytes = Hashtbl.find tbl bk in
  let json = Yojson.Safe.from_string (Bytes.to_string v_bytes) in
  (match json with
   | `Assoc kv ->
     let state = List.assoc "state" kv |> Yojson.Safe.Util.to_string in
     let acc   = List.assoc "acc" kv   |> Yojson.Safe.Util.to_int in
     let ne    = List.assoc "nonempty" kv |> Yojson.Safe.Util.to_bool in
     check (Printf.sprintf "state=open (got %s)" state) (state = "open");
     check (Printf.sprintf "acc=12 (got %d)" acc) (acc = 12);
     check "nonempty=true" ne
   | _ -> fail "json not assoc");

  (* ── 3. Missing parameter → Invalid_argument ───────────────── *)
  Printf.printf "\n-- 3. Missing parameter raises\n";

  let tbl_x = Hashtbl.create 4 in
  let backend_x = Persistence_backend.of_memory tbl_x in
  (try
    let _s = Pipe.window_fold (module Int_keyed)
      ~backend:backend_x
      ~serialize_acc:sum_acc_to_json
      ~deserialize_acc:sum_acc_of_json
      (* backend_name отсутствует *)
      (Pipe.tumbling (Time.seconds 10))
      ~init:(fun () -> 0)
      ~add:(fun acc (_, n) -> acc + n)
      Stream.empty in
    fail "should have raised"
  with Invalid_argument msg ->
    pass (Printf.sprintf "raised: %s"
            (String.sub msg 0 (min 60 (String.length msg)))));

  (* ── 4. Restore: окно с открытым state переживает рестарт ── *)
  Printf.printf "\n-- 4. Restore: open window continues accumulator\n";

  let tbl3 = Hashtbl.create 16 in
  let backend3 = Persistence_backend.of_memory tbl3 in

  (* Phase 1: события с sum=15, но wm только до 5000 — окно не закрылось *)
  let p1 = [
    Mf_event.data ("Y", 5) 0;
    Mf_event.data ("Y", 10) 1000;
    Mf_event.wm 5000;
  ] in
  let s1 = p1 |> Stream.of_list
    |> Pipe.window_fold (module Int_keyed)
         ~backend:backend3 ~backend_name:"restore"
         ~serialize_acc:sum_acc_to_json
         ~deserialize_acc:sum_acc_of_json
         (Pipe.tumbling (Time.seconds 10))
         ~init:(fun () -> 0)
         ~add:(fun acc (_, n) -> acc + n) in
  let _ = collect_outputs s1 in

  let bk = "window_fold:restore:Y:0:10000" in
  let json = Yojson.Safe.from_string
    (Bytes.to_string (Hashtbl.find tbl3 bk)) in
  (match json with
   | `Assoc kv ->
     let acc = List.assoc "acc" kv |> Yojson.Safe.Util.to_int in
     check (Printf.sprintf "phase 1: acc=15 (got %d)" acc) (acc = 15)
   | _ -> fail "json not assoc");

  (* Phase 2: новый instance с тем же backend.
     ещё одно событие, потом wm закрывает окно. Result должен быть
     15 + 25 = 40, а не 25 (без restore). *)
  let p2 = [
    Mf_event.data ("Y", 25) 3000;
    Mf_event.wm 15_000;
  ] in
  let s2 = p2 |> Stream.of_list
    |> Pipe.window_fold (module Int_keyed)
         ~backend:backend3 ~backend_name:"restore"
         ~serialize_acc:sum_acc_to_json
         ~deserialize_acc:sum_acc_of_json
         (Pipe.tumbling (Time.seconds 10))
         ~init:(fun () -> 0)
         ~add:(fun acc (_, n) -> acc + n) in
  let outs2 = collect_outputs s2 in
  Printf.printf "  phase 2 outputs: %s\n"
    (String.concat ", "
       (List.map (fun (k, v) -> Printf.sprintf "(%s,%d)" k v) outs2));
  check "phase 2: window emits (Y, 40)" (outs2 = [("Y", 40)]);

  (* ── 5. Restore FFired: late event still works ─────────────── *)
  Printf.printf "\n-- 5. Restore FFired: late event re-emits with retract\n";

  let tbl4 = Hashtbl.create 16 in
  let backend4 = Persistence_backend.of_memory tbl4 in

  (* Phase 1: окно закрывается (FFired в backend), allowed_lateness *)
  let p1 = [
    Mf_event.data ("Z", 100) 0;
    Mf_event.data ("Z", 200) 1000;
    Mf_event.wm 15_000;     (* закрывает окно [0..10s) *)
  ] in
  let s1 = p1 |> Stream.of_list
    |> Pipe.window_fold (module Int_keyed)
         ~backend:backend4 ~backend_name:"ffired"
         ~serialize_acc:sum_acc_to_json
         ~deserialize_acc:sum_acc_of_json
         ~allowed_lateness:(Time.seconds 30)
         (Pipe.tumbling (Time.seconds 10))
         ~init:(fun () -> 0)
         ~add:(fun acc (_, n) -> acc + n) in
  let outs1 = collect_outputs s1 in
  check "phase 1: emit (Z, 300)" (outs1 = [("Z", 300)]);

  let bk = "window_fold:ffired:Z:0:10000" in
  let json = Yojson.Safe.from_string
    (Bytes.to_string (Hashtbl.find tbl4 bk)) in
  (match json with
   | `Assoc kv ->
     let state = List.assoc "state" kv |> Yojson.Safe.Util.to_string in
     check (Printf.sprintf "phase 1: state=fired (got %s)" state) (state = "fired")
   | _ -> fail "json not assoc");

  (* Phase 2: late event прибывает (ts=5000 принадлежит окну [0,10000))
     В пределах allowed_lateness, окно ещё в FFired в backend.
     После restore, late event должен retract старый, emit новый.

     Phase 2 watermark не должен превысить stop + latency + allowed_lateness
     (= 10000 + 0 + 30000 = 40000), иначе окно очистится. *)
  let p2 = [
    Mf_event.data ("Z", 50) 5000;
    Mf_event.wm 20_000;
  ] in
  let s2 = p2 |> Stream.of_list
    |> Pipe.window_fold (module Int_keyed)
         ~backend:backend4 ~backend_name:"ffired"
         ~serialize_acc:sum_acc_to_json
         ~deserialize_acc:sum_acc_of_json
         ~allowed_lateness:(Time.seconds 30)
         (Pipe.tumbling (Time.seconds 10))
         ~init:(fun () -> 0)
         ~add:(fun acc (_, n) -> acc + n) in
  let datas2 = ref [] in
  let retracts2 = ref [] in
  let rec drain () = match s2 () with
    | None -> ()
    | Some (Mf_event.Data (v, _)) -> datas2 := v :: !datas2; drain ()
    | Some (Mf_event.Retract (v, _)) -> retracts2 := v :: !retracts2; drain ()
    | Some _ -> drain ()
  in drain ();
  Printf.printf "  phase 2: %d Data %d Retract\n"
    (List.length !datas2) (List.length !retracts2);
  check "phase 2: 1 retract (old 300)"
    (!retracts2 = [("Z", 300)]);
  check "phase 2: 1 new emit (350 = 300+50)"
    (!datas2 = [("Z", 350)]);

  Printf.printf "\nAll window_fold persistence tests passed.\n"
