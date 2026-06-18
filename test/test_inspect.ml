(** Тест: Pipe.inspect — non-terminal observer.

    Проверки:
    1. inspect видит все события (Data, Watermark, Retract)
    2. inspect пропускает stream дальше без изменений (events
       видны downstream)
    3. side-effect callback'а выполняется в правильном порядке
    4. label принимается но не влияет на поведение *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let () =
  Printf.printf "Test: Pipe.inspect\n%!";

  let events = [
    Mf_event.data "a" 0;
    Mf_event.wm 1000;
    Mf_event.data "b" 1500;
    Mf_event.retract "x" 2000;
    Mf_event.data "c" 2500;
  ] in

  (* ── 1. inspect видит все события ─────────────────── *)
  Printf.printf "\n-- 1. Sees all event types\n";
  let seen_data = ref 0 in
  let seen_wm = ref 0 in
  let seen_retract = ref 0 in
  let inspected = events |> Stream.of_list
    |> Pipe.inspect (fun ev ->
        match ev with
        | Mf_event.Data _ -> incr seen_data
        | Mf_event.Watermark _ -> incr seen_wm
        | Mf_event.Retract _ -> incr seen_retract) in
  (* Дренируем *)
  let _ = Pipe.collect inspected in
  check (Printf.sprintf "data=%d wm=%d retract=%d (expect 3,1,1)"
           !seen_data !seen_wm !seen_retract)
    (!seen_data = 3 && !seen_wm = 1 && !seen_retract = 1);

  (* ── 2. Stream проходит дальше без изменений ────────── *)
  Printf.printf "\n-- 2. Stream passes through unchanged\n";
  let direct = events |> Stream.of_list |> Pipe.collect in
  let via_inspect = events |> Stream.of_list
    |> Pipe.inspect (fun _ -> ())
    |> Pipe.collect in
  check "direct = via_inspect"
    (direct = via_inspect);

  (* ── 3. Порядок выполнения ──────────────────────────── *)
  Printf.printf "\n-- 3. Side-effect in event order\n";
  let trace = ref [] in
  let _ = events |> Stream.of_list
    |> Pipe.inspect (fun ev ->
        match ev with
        | Mf_event.Data (v, _) -> trace := v :: !trace
        | Mf_event.Watermark _ | Mf_event.Retract _ -> ())
    |> Pipe.collect in
  let in_order = List.rev !trace in
  check (Printf.sprintf "trace: [%s]" (String.concat ";" in_order))
    (in_order = ["a"; "b"; "c"]);

  (* ── 4. label опционален ─────────────────────────────── *)
  Printf.printf "\n-- 4. label is optional\n";
  let cnt = ref 0 in
  let _ = events |> Stream.of_list
    |> Pipe.inspect ~label:"my_debug" (fun _ -> incr cnt)
    |> Pipe.collect in
  check "with label: callback still fires"
    (!cnt = 5);  (* 3 data + 1 wm + 1 retract = 5 *)

  Printf.printf "\nTest passed.\n"
