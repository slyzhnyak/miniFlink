(** Покрытие семантики keyed_join: Retract, Update, stale events,
    cross-key Update, atomicity (отсутствие None-flicker).

    keyed_join — snapshot-оператор: эмитит массив последних значений
    каждого источника. Самое тонкое место — atomic Update (без
    промежуточного None) и stale-retract (retract значения которое
    уже перезаписано). *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let collect s =
  let acc = ref [] in
  let rec go () = match s () with None -> () | Some e -> acc := e :: !acc; go () in
  go (); List.rev !acc

(* снимки (списки option) из Data-событий *)
let snapshots out = List.filter_map (function
  | Mf_event.Data ((_, opts), _) -> Some opts | _ -> None) out

module ByKey = struct type t = string * int let key (k, _) = k end

let () =
  Printf.printf "keyed_join semantics: Retract / Update / stale / atomicity\n%!";

  (* ════════════════════════════════════════════════════════════
     1. Retract отзывает последнее значение → slot становится None
     ════════════════════════════════════════════════════════════ *)
  Printf.printf "\n-- 1. Retract reverts the slot it last set\n";
  let s1 = [
    Mf_event.data ("A", 10) 0;
    Mf_event.retract ("A", 10) 100;
  ] |> Stream.of_list in
  let out = Pipe.keyed_join (module ByKey) [s1] |> collect in
  let snaps = snapshots out in
  Printf.printf "  snapshots: %d\n" (List.length snaps);
  List.iter (fun s -> Printf.printf "    [%s]\n"
    (String.concat ";" (List.map (function None -> "_" | Some (_,v) -> string_of_int v) s))) snaps;
  check "after Data then Retract: last snapshot is [None]"
    (match List.rev snaps with [None] :: _ -> true | _ -> false);

  (* ════════════════════════════════════════════════════════════
     2. Stale Retract игнорируется (значение уже перезаписано)
     ════════════════════════════════════════════════════════════ *)
  Printf.printf "\n-- 2. Stale Retract (value already overwritten) is ignored\n";
  let s2 = [
    Mf_event.data ("A", 10) 0;
    Mf_event.data ("A", 20) 100;   (* перезаписали 10 на 20 *)
    Mf_event.retract ("A", 10) 200; (* stale: slot уже 20, не 10 *)
  ] |> Stream.of_list in
  let out = Pipe.keyed_join (module ByKey) [s2] |> collect in
  let snaps = snapshots out in
  (* последний snapshot должен остаться [Some 20], stale retract не тронул *)
  check "stale Retract(10) ignored: slot stays [Some 20]"
    (match List.rev snaps with [Some (_, 20)] :: _ -> true | _ -> false);

  (* ════════════════════════════════════════════════════════════
     3. ATOMIC Update — нет None-flicker между old и new
     ════════════════════════════════════════════════════════════ *)
  Printf.printf "\n-- 3. Atomic Update: no None flicker (the core motivation)\n";
  let s3 = [
    Mf_event.data ("A", 100) 0;
    Mf_event.update ("A", 100) ("A", 50) 100;  (* атомарная коррекция *)
  ] |> Stream.of_list in
  let out = Pipe.keyed_join (module ByKey) [s3] |> collect in
  let snaps = snapshots out in
  Printf.printf "  snapshots: %d\n" (List.length snaps);
  List.iter (fun s -> Printf.printf "    [%s]\n"
    (String.concat ";" (List.map (function None -> "_" | Some (_,v) -> string_of_int v) s))) snaps;
  (* должно быть 2 snapshot: [100], [50]. НЕ должно быть [None] между. *)
  check "2 snapshots [100] then [50], NO None flicker"
    (List.length snaps = 2
     && not (List.exists (List.exists (function None -> true | _ -> false)) snaps));

  (* ════════════════════════════════════════════════════════════
     4. Update двух источников — snapshot объединяет последние
     ════════════════════════════════════════════════════════════ *)
  Printf.printf "\n-- 4. Two sources: snapshot merges latest of each\n";
  let sa = [ Mf_event.data ("K", 1) 0;
             Mf_event.update ("K", 1) ("K", 11) 200 ] |> Stream.of_list in
  let sb = [ Mf_event.data ("K", 2) 100 ] |> Stream.of_list in
  let out = Pipe.keyed_join (module ByKey) [sa; sb] |> collect in
  let snaps = snapshots out in
  Printf.printf "  final snapshot: [%s]\n"
    (match List.rev snaps with s :: _ ->
       String.concat ";" (List.map (function None -> "_" | Some (_,v) -> string_of_int v) s)
     | [] -> "none");
  (* финальный: источник 0 обновился до 11, источник 1 = 2 *)
  check "final snapshot [Some 11; Some 2] (source 0 updated, source 1 intact)"
    (match List.rev snaps with
     | [Some (_, 11); Some (_, 2)] :: _ -> true | _ -> false);

  (* ════════════════════════════════════════════════════════════
     5. Update slot которого ещё не было → ведёт себя как появление
     ════════════════════════════════════════════════════════════ *)
  Printf.printf "\n-- 5. Update on never-seen slot behaves as appearance\n";
  let s5 = [
    Mf_event.update ("A", 5) ("A", 7) 0;  (* old=5 не было в slot *)
  ] |> Stream.of_list in
  let out = Pipe.keyed_join (module ByKey) [s5] |> collect in
  let snaps = snapshots out in
  Printf.printf "  snapshots: %d, last: [%s]\n" (List.length snaps)
    (match List.rev snaps with s :: _ ->
       String.concat ";" (List.map (function None -> "_" | Some (_,v) -> string_of_int v) s)
     | [] -> "none");
  (* stale Update (old не совпал) → применяем new как появление *)
  check "Update with unseen old: new_value (7) ends up in slot"
    (match List.rev snaps with [Some (_, 7)] :: _ -> true | _ -> false);

  Printf.printf "\nDone.\n"
