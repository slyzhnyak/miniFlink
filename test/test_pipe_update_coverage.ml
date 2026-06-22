(** Покрытие Update/Retract семантики в pipe-комбинаторах
    (filter/flat_map/map и их safe_-варианты). Эти ветки — результат
    работы над атомарными Update-событиями (R4); happy-path тесты их
    не задевали. *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let drain s =
  let acc = ref [] in
  let rec go () = match s () with
    | None -> () | Some ev -> acc := ev :: !acc; go ()
  in go (); List.rev !acc

(* Классификатор события для компактных проверок. *)
let tag = function
  | Mf_event.Data (v, _) -> `D v
  | Mf_event.Retract (v, _) -> `R v
  | Mf_event.Update { old; new_value; _ } -> `U (old, new_value)
  | Mf_event.Watermark _ -> `W

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Pipe: Update/Retract combinator coverage\n";
  Printf.printf "==========================================\n";

  (* ── 1. filter на Update: все 4 случая (p old, p new) ──────── *)
  Printf.printf "\n-- 1. filter Update: four (p old, p new) cases\n";
  let p x = x > 10 in
  let f_in = [
    Mf_event.update 20 30 0;   (* (T,T) → Update 20→30 *)
    Mf_event.update 5  30 1;   (* (F,T) → Data 30 (появление) *)
    Mf_event.update 20 5  2;   (* (T,F) → Retract 20 (исчезновение) *)
    Mf_event.update 3  5  3;   (* (F,F) → ничего *)
  ] in
  let out = f_in |> Stream.of_list |> Pipe.filter p |> drain |> List.map tag in
  check "filter Update (T,T) → Update 20→30" (List.nth out 0 = `U (20, 30));
  check "filter Update (F,T) → Data 30" (List.nth out 1 = `D 30);
  check "filter Update (T,F) → Retract 20" (List.nth out 2 = `R 20);
  check "filter Update (F,F) → dropped (only 3 events out)" (List.length out = 3);

  (* filter на Retract: пропускается только если p проходит *)
  Printf.printf "\n-- filter Retract: gated by predicate\n";
  let r_in = [ Mf_event.retract 20 0; Mf_event.retract 5 1 ] in
  let rout = r_in |> Stream.of_list |> Pipe.filter p |> drain |> List.map tag in
  check "filter Retract: p-passing kept, p-failing dropped"
    (rout = [`R 20]);

  (* ── 2. flat_map на Update: N синхронных Update ────────────── *)
  Printf.printf "\n-- 2. flat_map Update: synchronized N updates\n";
  (* f x = [x; x*10] — old=2→[2;20], new=3→[3;30]; zip → Update 2→3, 20→30 *)
  let fm = [ Mf_event.update 2 3 0 ]
    |> Stream.of_list |> Pipe.flat_map (fun x -> [x; x*10]) |> drain |> List.map tag in
  check "flat_map Update → [U 2→3; U 20→30]"
    (fm = [`U (2,3); `U (20,30)]);
  (* flat_map на Retract: каждый выход ретрактится *)
  let fmr = [ Mf_event.retract 5 0 ]
    |> Stream.of_list |> Pipe.flat_map (fun x -> [x; x+1]) |> drain |> List.map tag in
  check "flat_map Retract → [R 5; R 6]" (fmr = [`R 5; `R 6]);

  (* ── 3. map на Update/Retract ──────────────────────────────── *)
  Printf.printf "\n-- 3. map over Update/Watermark\n";
  let mout = [ Mf_event.update 2 3 0; Mf_event.wm 100 ]
    |> Stream.of_list |> Pipe.map (fun x -> x * 100) |> drain |> List.map tag in
  check "map Update → U 200→300, then W"
    (mout = [`U (200,300); `W]);

  (* ── 4. safe_* варианты: Update-ветки + on_error ───────────── *)
  Printf.printf "\n-- 4. safe_map / safe_filter / safe_flat_map\n";
  let errs = ref 0 in
  let on_error _ = incr errs in
  (* safe_map: Update нормально транслируется *)
  let sm = [ Mf_event.update 2 3 0 ]
    |> Stream.of_list |> Pipe.safe_map ~on_error (fun x -> x + 1) |> drain |> List.map tag in
  check "safe_map Update → U 3→4" (sm = [`U (3,4)]);
  (* safe_map: исключение на Data → drop + on_error *)
  errs := 0;
  let sm_err = [ Mf_event.data 1 0; Mf_event.data 2 1 ]
    |> Stream.of_list
    |> Pipe.safe_map ~on_error (fun x -> if x = 1 then failwith "boom" else x) |> drain in
  check "safe_map: failing element dropped" (List.length sm_err = 1);
  check "safe_map: on_error called once" (!errs = 1);
  (* safe_filter: Update-ветка *)
  let sf = [ Mf_event.update 5 20 0 ]
    |> Stream.of_list |> Pipe.safe_filter ~on_error (fun x -> x > 10) |> drain |> List.map tag in
  check "safe_filter Update (new passes) → kept" (sf = [`U (5, 20)]);
  (* safe_flat_map: Update zip *)
  let sfm = [ Mf_event.update 2 3 0 ]
    |> Stream.of_list |> Pipe.safe_flat_map ~on_error (fun x -> [x; x*10]) |> drain |> List.map tag in
  check "safe_flat_map Update → [U 2→3; U 20→30]"
    (sfm = [`U (2,3); `U (20,30)]);

  Printf.printf "\nPipe Update/Retract combinator coverage passed.\n"
