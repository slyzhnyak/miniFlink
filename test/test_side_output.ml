open Miniflink
(* Side output для опоздавших: событие, пришедшее ПОЗЖЕ allowed_lateness
   (его окно уже удалено по watermark), не должно молча теряться или
   создавать «призрачное» окно — оно направляется в on_late callback.
   Тест должен падать на старом коде (нет on_late), проходить на новом. *)

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

module K = Keyed.Make (struct type t = int * int let key (id,_) = string_of_int id end)

(* окно [0,10) tumbling, latency=0, allowed_lateness=5.
   Событие с ts=2 приходит ПОСЛЕ watermark=20 — это позже окончательного
   удаления окна [0,10) (удаляется при wm > 10+0+5=15). Должно уйти в late. *)
let test_late_side_output () =
  Printf.printf "\n-- events past allowed_lateness go to side output, not lost\n";
  let late = ref [] in
  let src = Stream.of_list [
    Mf_event.data (1, 100) 3;     (* в окне [0,10) *)
    Mf_event.wm 20;               (* закрывает и УДАЛЯЕТ окно [0,10) (>15) *)
    Mf_event.data (1, 999) 2;     (* СИЛЬНО опоздавшее: окно [0,10) уже удалено *)
    Mf_event.wm 30;
  ] in
  let out = src
    |> Pipe.window (module K) ~latency:0 ~allowed_lateness:5
         ~on_late:(fun v -> late := v :: !late)
         (Pipe.tumbling 10)
    |> Stream.to_list in
  let windows = List.filter_map (function
    | Mf_event.Data ((_, vs),_) -> Some vs | _ -> None) out in
  Printf.printf "  опоздавших в side output: %d\n%!" (List.length !late);
  check "late event went to side output" (!late = [(1, 999)]);
  check "late event did NOT create a ghost window"
    (not (List.exists (List.mem (1,999)) windows))

(* событие в пределах allowed_lateness НЕ должно попадать в late —
   оно переоткрывает окно (retract+data), как и раньше *)
let test_within_lateness_not_side () =
  Printf.printf "\n-- events within allowed_lateness are NOT side-output\n";
  let late = ref [] in
  let src = Stream.of_list [
    Mf_event.data (1, 100) 3;
    Mf_event.wm 11;               (* закрывает [0,10), но не удаляет (11 < 15) *)
    Mf_event.data (1, 200) 4;     (* опоздавшее, но в пределах lateness *)
    Mf_event.wm 30;
  ] in
  let _ = src
    |> Pipe.window (module K) ~latency:0 ~allowed_lateness:5
         ~on_late:(fun v -> late := v :: !late)
         (Pipe.tumbling 10)
    |> Stream.to_list in
  check "event within lateness not sent to side output" (!late = [])

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Late data side output\n";
  Printf.printf "==========================================\n";
  test_late_side_output ();
  test_within_lateness_not_side ();
  Printf.printf "\nSide output tests passed.\n"
