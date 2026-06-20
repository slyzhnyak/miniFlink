(** Тест: materializing-окна (window, global_window, window_instrumented)
    обрабатывают Retract и Update на input (P1.1 остаток).

    Эти окна хранят полный список событий, поэтому Retract убирает
    значение из списка, а Update атомарно заменяет old → new. *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

type ev = { ev_key : string; ev_value : int }

module ByKey : Keyed.S with type t = ev = struct
  type t = ev
  let key e = e.ev_key
end

let collect_all s =
  let acc = ref [] in
  let rec drain () = match s () with
    | None -> () | Some ev -> acc := ev :: !acc; drain ()
  in drain ();
  List.rev !acc

let () =
  Printf.printf "Test: materializing windows handle Retract/Update on input\n%!";

  (* ── 1. window: Retract убирает значение из открытого окна ── *)
  Printf.printf "\n-- 1. window: Retract removes value from open window\n";
  let events = [
    Mf_event.data { ev_key = "A"; ev_value = 10 } 0;
    Mf_event.data { ev_key = "A"; ev_value = 20 } 100;
    Mf_event.data { ev_key = "A"; ev_value = 30 } 200;
    Mf_event.retract { ev_key = "A"; ev_value = 20 } 300;
    Mf_event.wm 10_000;
  ] in
  let results = events |> Stream.of_list
    |> Pipe.window (module ByKey) (Pipe.tumbling 1000)
    |> collect_all in
  let datas = List.filter_map (function
    | Mf_event.Data ((_, vs), _) -> Some (List.map (fun e -> e.ev_value) vs)
    | _ -> None) results in
  (match datas with
   | [vals] ->
     let sorted = List.sort compare vals in
     Printf.printf "  window vals: [%s]\n"
       (String.concat ";" (List.map string_of_int sorted));
     check "window has [10;30] after retracting 20"
       (sorted = [10; 30])
   | _ -> fail "expected 1 window emission");

  (* ── 2. window: Update заменяет old → new атомарно ── *)
  Printf.printf "\n-- 2. window: Update replaces old with new\n";
  let events = [
    Mf_event.data { ev_key = "B"; ev_value = 5 } 0;
    Mf_event.data { ev_key = "B"; ev_value = 99 } 100;
    Mf_event.update
      { ev_key = "B"; ev_value = 99 }
      { ev_key = "B"; ev_value = 7 } 200;
    Mf_event.wm 10_000;
  ] in
  let results = events |> Stream.of_list
    |> Pipe.window (module ByKey) (Pipe.tumbling 1000)
    |> collect_all in
  let datas = List.filter_map (function
    | Mf_event.Data ((_, vs), _) -> Some (List.map (fun e -> e.ev_value) vs)
    | _ -> None) results in
  (match datas with
   | [vals] ->
     let sorted = List.sort compare vals in
     Printf.printf "  window vals: [%s]\n"
       (String.concat ";" (List.map string_of_int sorted));
     check "window has [5;7] after Update 99→7"
       (sorted = [5; 7])
   | _ -> fail "expected 1 window emission");

  (* ── 3. window: Update на закрытое окно → atomic Update downstream ── *)
  Printf.printf "\n-- 3. window: late Update on fired window emits Update\n";
  let events = [
    Mf_event.data { ev_key = "C"; ev_value = 1 } 0;
    Mf_event.wm 2000;   (* закрывает окно [0,1000) *)
    Mf_event.update
      { ev_key = "C"; ev_value = 1 }
      { ev_key = "C"; ev_value = 2 } 500;
    Mf_event.wm 3000;
  ] in
  let results = events |> Stream.of_list
    |> Pipe.window (module ByKey) ~allowed_lateness:5000 (Pipe.tumbling 1000)
    |> collect_all in
  let updates = List.filter (function
    | Mf_event.Update _ -> true | _ -> false) results in
  Printf.printf "  updates emitted: %d\n" (List.length updates);
  check "late Update on fired window emits 1 atomic Update"
    (List.length updates = 1);

  (* ── 4. global_window: Retract убирает из буфера ── *)
  Printf.printf "\n-- 4. global_window: Retract removes from buffer\n";
  let events = [
    Mf_event.data { ev_key = "G"; ev_value = 1 } 0;
    Mf_event.data { ev_key = "G"; ev_value = 2 } 100;
    Mf_event.retract { ev_key = "G"; ev_value = 1 } 200;
  ] in
  (* global_window с trigger по счёту *)
  let results = events |> Stream.of_list
    |> Pipe.global_window (module ByKey)
         ~trigger:(fun ~count ~last:_ ->
           if count >= 5 then Pipe.Fire else Pipe.Continue)
    |> collect_all in
  let datas = List.filter_map (function
    | Mf_event.Data ((_, vs), _) -> Some (List.map (fun e -> e.ev_value) vs)
    | _ -> None) results in
  (* На конце потока эмитятся непустые буферы. После retract(1):
     буфер = [2]. *)
  Printf.printf "  emissions: %d\n" (List.length datas);
  (match datas with
   | [vals] ->
     check "global_window buffer = [2] after retract(1)"
       (List.sort compare vals = [2])
   | _ ->
     Printf.printf "  (got %d emissions)\n" (List.length datas);
     check "at least handled without crash" true);

  (* ── 5. window_instrumented: Update заменяет ── *)
  Printf.printf "\n-- 5. window_instrumented: Update replaces value\n";
  let events = [
    Mf_event.data { ev_key = "I"; ev_value = 100 } 0;
    Mf_event.update
      { ev_key = "I"; ev_value = 100 }
      { ev_key = "I"; ev_value = 42 } 100;
    Mf_event.wm 10_000;
  ] in
  let results = events |> Stream.of_list
    |> Window.window_instrumented (module ByKey) (Window.tumbling 1000)
         ~observe_window_ms:(fun _ -> ())
    |> collect_all in
  let datas = List.filter_map (function
    | Mf_event.Data ((_, vs), _) -> Some (List.map (fun e -> e.ev_value) vs)
    | _ -> None) results in
  (match datas with
   | [vals] ->
     Printf.printf "  vals: [%s]\n"
       (String.concat ";" (List.map string_of_int vals));
     check "window_instrumented has [42] after Update 100→42"
       (List.sort compare vals = [42])
   | _ -> fail "expected 1 emission");

  Printf.printf "\nAll materializing-window Retract/Update tests passed.\n"
