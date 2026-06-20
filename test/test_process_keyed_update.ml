(** Тест: process_keyed ?on_update callback (Phase 3.3).

    Verifies that when on_update is passed:
    - It's called on Update events with (ctx, key, st, ~old, ~new_value)
    - Allows atomic state rollback + apply

    When on_update is NOT passed:
    - Conservative fallback: on_event called with new_value
    - Same effect as Phase 1 behavior
    *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

(* Простой event type — measurement по lamp *)
type meas = { m_lamp : string; m_value : float }

(* State: история измерений per ключ *)
type st = {
  mutable history : float list;  (* в обратном порядке: newest first *)
  mutable total   : float;
}

module ByLamp : Keyed.S with type t = meas = struct
  type t = meas
  let key m = m.m_lamp
end

type out = Total_emit of string * float

let collect_data stream =
  let acc = ref [] in
  let rec drain () = match stream () with
    | None -> ()
    | Some (Mf_event.Data (v, _)) -> acc := v :: !acc; drain ()
    | Some (Mf_event.Watermark _)
    | Some (Mf_event.Retract _)
    | Some (Mf_event.Update _) -> drain ()
  in drain ();
  List.rev !acc

let () =
  Printf.printf "Test: process_keyed ?on_update callback\n%!";

  (* ── 1. Без on_update: conservative fallback ──────────── *)
  Printf.printf "\n-- 1. Without on_update: conservative (Update as Data on new_value)\n";
  let events = [
    Mf_event.data { m_lamp = "L1"; m_value = 10.0 } 0;
    Mf_event.wm 100;
    Mf_event.update
      { m_lamp = "L1"; m_value = 10.0 }
      { m_lamp = "L1"; m_value = 15.0 } 200;  (* correction *)
    Mf_event.wm 300;
  ] in
  let outs = events |> Stream.of_list
    |> Pipe.process_keyed (module ByLamp)
         ~init:(fun () -> { history = []; total = 0.0 })
         ~on_event:(fun ctx _key st m ->
           st.history <- m.m_value :: st.history;
           st.total <- st.total +. m.m_value;
           ctx.emit (Total_emit (m.m_lamp, st.total)))
         ~on_timer:(fun _ _ _ _ _ -> ())
    |> collect_data in
  Printf.printf "  emissions: %d\n" (List.length outs);
  List.iter (function Total_emit (l, t) ->
    Printf.printf "    %s: total=%.1f\n" l t) outs;
  (* Conservative: Update обрабатывается как Data на new_value=15.
     Без отката — total = 10 + 15 = 25, не 15. *)
  check "conservative: total = 10 + 15 = 25 (no rollback)"
    (match outs with
     | [Total_emit ("L1", 10.0); Total_emit ("L1", 25.0)] -> true
     | _ -> false);

  (* ── 2. С on_update: native atomic rollback + apply ───── *)
  Printf.printf "\n-- 2. With on_update: native atomic state transition\n";
  let events = [
    Mf_event.data { m_lamp = "L2"; m_value = 10.0 } 0;
    Mf_event.wm 100;
    Mf_event.update
      { m_lamp = "L2"; m_value = 10.0 }
      { m_lamp = "L2"; m_value = 15.0 } 200;
    Mf_event.wm 300;
  ] in
  let outs = events |> Stream.of_list
    |> Pipe.process_keyed (module ByLamp)
         ~init:(fun () -> { history = []; total = 0.0 })
         ~on_event:(fun ctx _key st m ->
           st.history <- m.m_value :: st.history;
           st.total <- st.total +. m.m_value;
           ctx.emit (Total_emit (m.m_lamp, st.total)))
         ~on_update:(fun ctx _key st ~old ~new_value ->
           (* Atomic: rollback effect of old, apply new *)
           st.history <- (match st.history with
             | h :: tl when h = old.m_value -> new_value.m_value :: tl
             | _ -> new_value.m_value :: st.history);
           st.total <- st.total -. old.m_value +. new_value.m_value;
           ctx.emit (Total_emit (new_value.m_lamp, st.total)))
         ~on_timer:(fun _ _ _ _ _ -> ())
    |> collect_data in
  Printf.printf "  emissions: %d\n" (List.length outs);
  List.iter (function Total_emit (l, t) ->
    Printf.printf "    %s: total=%.1f\n" l t) outs;
  (* Native: rollback 10 + apply 15 → total = 0 - 10 + 15 = ... wait
     Сначала Data(10) → total=10
     Затем Update(10→15): total = 10 - 10 + 15 = 15
     Это correct! Не 25 (без отката), а 15 (с откатом). *)
  check "native: total = 15 after atomic rollback+apply"
    (match outs with
     | [Total_emit ("L2", 10.0); Total_emit ("L2", 15.0)] -> true
     | _ -> false);

  (* ── 3. on_update vs on_event: разные результаты на одинаковом входе ── *)
  Printf.printf "\n-- 3. on_update DIFFERS from conservative\n";
  Printf.printf "  Conservative: 25 (10 + 15, no rollback)\n";
  Printf.printf "  Native:        15 (10 - 10 + 15, atomic)\n";
  check "results differ — proving native callback runs"
    true;  (* доказано в 1 vs 2 *)

  Printf.printf "\nAll process_keyed on_update tests passed.\n"
