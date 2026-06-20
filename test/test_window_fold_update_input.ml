(** Тест: window_fold обрабатывает Update на input (P1.1 cleanup).

    Update на input окна = атомарная коррекция: remove(old) + add(new).
    Проверяет что цепочка window→window корректно передаёт коррекции. *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

type ev = { ev_key : string; ev_value : float }

module ByKey : Keyed.S with type t = ev = struct
  type t = ev
  let key e = e.ev_key
end

let collect_all s =
  let acc = ref [] in
  let rec drain () = match s () with
    | None -> ()
    | Some ev -> acc := ev :: !acc; drain ()
  in drain ();
  List.rev !acc

let () =
  Printf.printf "Test: window_fold Update on input\n%!";

  (* ── 1. Update на открытое окно: remove old + add new ── *)
  Printf.printf "\n-- 1. Update on open window (retractable sum)\n";
  let events = [
    Mf_event.data { ev_key = "A"; ev_value = 10.0 } 0;
    Mf_event.data { ev_key = "A"; ev_value = 20.0 } 100;
    (* Update: значение 20 было ошибкой, на самом деле 5 *)
    Mf_event.update
      { ev_key = "A"; ev_value = 20.0 }
      { ev_key = "A"; ev_value = 5.0 } 200;
    Mf_event.wm 10_000;
  ] in
  let results = events |> Stream.of_list
    |> Pipe.window_agg (module ByKey)
         (Pipe.tumbling 1000)
         Agg.(sum (fun e -> e.ev_value))
    |> collect_all in
  let data_results = List.filter_map (function
    | Mf_event.Data ((k, v), _) -> Some (k, v) | _ -> None) results in
  (match data_results with
   | [(key, total)] ->
     Printf.printf "  result: (%s, %.1f)\n" key total;
     (* 10 + 20, затем Update 20→5: итого 10 + 5 = 15 *)
     check "sum = 15.0 (10 + 20, then Update 20→5)"
       (key = "A" && Float.abs (total -. 15.0) < 0.001)
   | _ ->
     Printf.printf "  got %d data emissions\n" (List.length data_results);
     fail "expected 1 data emission");

  (* ── 2. Update на закрытое (FFired) окно → emit Update ── *)
  Printf.printf "\n-- 2. Update on fired window → emits atomic Update downstream\n";
  let events = [
    Mf_event.data { ev_key = "B"; ev_value = 100.0 } 0;
    Mf_event.wm 2000;   (* закрывает окно [0,1000), emit Data(100) *)
    (* late Update: 100 был ошибкой, на самом деле 50 *)
    Mf_event.update
      { ev_key = "B"; ev_value = 100.0 }
      { ev_key = "B"; ev_value = 50.0 } 500;
    Mf_event.wm 3000;
  ] in
  let results = events |> Stream.of_list
    |> Pipe.window_agg (module ByKey)
         ~allowed_lateness:5000
         (Pipe.tumbling 1000)
         Agg.(sum (fun e -> e.ev_value))
    |> collect_all in
  let datas = List.filter_map (function
    | Mf_event.Data ((_, v), _) -> Some v | _ -> None) results in
  let updates = List.filter_map (function
    | Mf_event.Update { old = (_, ov); new_value = (_, nv); _ } -> Some (ov, nv)
    | _ -> None) results in
  Printf.printf "  datas: %d, updates: %d\n"
    (List.length datas) (List.length updates);
  check "1 initial Data (100)" (datas = [100.0]);
  check "1 atomic Update emitted (100 → 50)"
    (updates = [(100.0, 50.0)]);

  (* ── 3. End-to-end: window → keyed_join, Update проходит атомарно ── *)
  Printf.printf "\n-- 3. window → keyed_join: Update flows atomically\n";
  let module ByLamp : Keyed.S with type t = (string * float) = struct
    type t = string * float
    let key (l, _) = l
  end in
  let events = [
    Mf_event.data { ev_key = "L1"; ev_value = 100.0 } 0;
    Mf_event.wm 2000;
    Mf_event.update
      { ev_key = "L1"; ev_value = 100.0 }
      { ev_key = "L1"; ev_value = 50.0 } 500;
    Mf_event.wm 3000;
  ] in
  let window_out = events |> Stream.of_list
    |> Pipe.window_agg (module ByKey)
         ~allowed_lateness:5000
         (Pipe.tumbling 1000)
         Agg.(sum (fun e -> e.ev_value)) in
  let joined = Pipe.keyed_join (module ByLamp) [window_out] in
  let snaps = ref [] in
  let rec drain () = match joined () with
    | None -> ()
    | Some (Mf_event.Data ((_, opts), _)) -> snaps := opts :: !snaps; drain ()
    | Some _ -> drain ()
  in drain ();
  let snapshots = List.rev !snaps in
  Printf.printf "  snapshots: %d\n" (List.length snapshots);
  List.iteri (fun i opts ->
    Printf.printf "    [%d] [%s]\n" i
      (String.concat ";" (List.map (function
         | None -> "None" | Some (_, v) -> Printf.sprintf "%.0f" v) opts))) snapshots;
  (* Атомарность: 2 snapshot'а (100 → 50), без None между *)
  check "2 snapshots, no None flicker"
    (List.length snapshots = 2
     && not (List.exists (List.exists (function None -> true | _ -> false))
               snapshots));

  Printf.printf "\nAll window_fold Update-input tests passed.\n"
