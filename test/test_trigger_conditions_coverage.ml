(** Покрытие веток Trigger, не задетых основными тестами:
    - все condition-конструкторы (hysteresis, is_some, is_none);
    - обработка upstream Retract;
    - combine с пустым списком и одним spec. *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

(* Прогнать поток через триггер, собрать Data-алерты. *)
let run_alerts spec events =
  events |> Stream.of_list |> Trigger.of_stream spec
  |> (fun s ->
      let acc = ref [] in
      let rec go () = match s () with
        | None -> ()
        | Some (Mf_event.Data (a, _)) -> acc := a :: !acc; go ()
        | Some _ -> go ()
      in go (); List.rev !acc)

let run_alerts_combine specs events =
  events |> Stream.of_list |> Trigger.combine specs
  |> (fun s ->
      let acc = ref [] in
      let rec go () = match s () with
        | None -> ()
        | Some (Mf_event.Data (a, _)) -> acc := a :: !acc; go ()
        | Some _ -> go ()
      in go (); List.rev !acc)

let mk ?(name="t") cond produce =
  Trigger.create ~name ~condition:cond
    ~produce_alert:(fun ~key ~value:_ ~ts:_ -> produce key)
    ~produce_recovery:(fun ~key ~ts:_ -> produce (key ^ "_ok"))
    ()

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Trigger: conditions + Retract + combine\n";
  Printf.printf "==========================================\n";

  (* ── 1. greater_than_with_hysteresis ───────────────────────── *)
  Printf.printf "\n-- 1. hysteresis conditions\n";
  (* вход при v>100, выход при v<50; значение 75 между порогами не
     меняет состояние *)
  let hyst = mk (Trigger.greater_than_with_hysteresis ~problem:100. ~recovery:50.)
               (fun k -> k) in
  let alerts = run_alerts hyst [
    Mf_event.data ("A", 120.) 0;    (* > 100 → Problem, alert A *)
    Mf_event.data ("A", 75.)  100;  (* между порогами → остаётся Problem *)
    Mf_event.data ("A", 30.)  200;  (* < 50 → recovery, alert A_ok *)
    Mf_event.wm 1000;
  ] in
  check "hysteresis: [A; A_ok] (75 doesn't recover)" (alerts = ["A"; "A_ok"]);

  let lhyst = mk (Trigger.less_than_with_hysteresis ~problem:10. ~recovery:50.)
                (fun k -> k) in
  let la = run_alerts lhyst [
    Mf_event.data ("B", 5.)  0;     (* < 10 → Problem *)
    Mf_event.data ("B", 30.) 100;   (* между → остаётся *)
    Mf_event.data ("B", 60.) 200;   (* > 50 → recovery *)
    Mf_event.wm 1000;
  ] in
  check "less-hysteresis: [B; B_ok]" (la = ["B"; "B_ok"]);

  (* ── 2. is_some / is_none ──────────────────────────────────── *)
  Printf.printf "\n-- 2. is_some / is_none on option values\n";
  let some_t = mk ~name:"some" Trigger.is_some (fun k -> k) in
  let sa = run_alerts some_t [
    Mf_event.data ("C", Some 1) 0;   (* Some → Problem *)
    Mf_event.data ("C", None)   100; (* None → recovery *)
    Mf_event.wm 1000;
  ] in
  check "is_some: [C; C_ok]" (sa = ["C"; "C_ok"]);

  let none_t = mk ~name:"none" Trigger.is_none (fun k -> k) in
  let na = run_alerts none_t [
    Mf_event.data ("D", None)   0;   (* None → Problem *)
    Mf_event.data ("D", Some 9) 100; (* Some → recovery *)
    Mf_event.wm 1000;
  ] in
  check "is_none: [D; D_ok]" (na = ["D"; "D_ok"]);

  (* ── 3. upstream Retract игнорируется ──────────────────────── *)
  Printf.printf "\n-- 3. upstream Retract is ignored\n";
  let gt = mk ~name:"gt" (Trigger.greater_than 100.) (fun k -> k) in
  let ra = run_alerts gt [
    Mf_event.data ("E", 150.) 0;            (* Problem, alert E *)
    Mf_event.retract ("E", 150.) 50;        (* Retract — должен игнорироваться *)
    Mf_event.wm 1000;
  ] in
  check "Retract ignored: alert E still emitted, no extra" (ra = ["E"]);

  (* ── 4. combine: пустой список и один spec ─────────────────── *)
  Printf.printf "\n-- 4. combine edge cases\n";
  (* пустой список спеков → пустой поток *)
  let empty_combined =
    [Mf_event.data ("X", 5.) 0; Mf_event.wm 100]
    |> Stream.of_list |> Trigger.combine [] in
  let ec = (let acc = ref 0 in
    let rec go () = match empty_combined () with
      | None -> () | Some _ -> incr acc; go () in go (); !acc) in
  check "combine []: empty stream" (ec = 0);

  (* один spec → эквивалентно of_stream *)
  let one = mk ~name:"one" (Trigger.greater_than 100.) (fun k -> k) in
  let single = run_alerts_combine [one] [
    Mf_event.data ("F", 200.) 0; Mf_event.wm 1000 ] in
  check "combine [one]: behaves like of_stream → [F]" (single = ["F"]);

  Printf.printf "\nTrigger conditions/Retract/combine coverage passed.\n"
