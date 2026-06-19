(** Тест: Pipe.keyed_join_map — helper над keyed_join + filter+map. *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

type kv = string * float

module By_key : Keyed.S with type t = kv = struct
  type t = kv
  let key (k, _) = k
end

let collect_data s =
  let acc = ref [] in
  let rec drain () = match s () with
    | None -> ()
    | Some (Mf_event.Data (v, _)) -> acc := v :: !acc; drain ()
    | Some (Mf_event.Update { new_value = v; _ }) -> acc := v :: !acc; drain ()
    | Some (Mf_event.Watermark _) | Some (Mf_event.Retract _) -> drain ()
  in drain ();
  List.rev !acc

let () =
  Printf.printf "Test: Pipe.keyed_join_map\n%!";

  (* ── 1. Базовое — критическое сочетание v < 3.5 и c > 50 ── *)
  Printf.printf "\n-- 1. Filter + map combined\n";
  let voltage = [
    Mf_event.data ("M1", 4.0)  0;       (* v ok *)
    Mf_event.data ("M1", 3.0)  1000;    (* v low *)
    Mf_event.data ("M2", 3.0)  2000;    (* v low but no co yet *)
  ] |> Stream.of_list in
  let co = [
    Mf_event.data ("M1", 30.0) 100;     (* co ok *)
    Mf_event.data ("M1", 60.0) 1500;    (* co high *)
  ] |> Stream.of_list in
  let result = Pipe.keyed_join_map (module By_key)
    ~f:(fun key opts ->
      match opts with
      | [Some (_, v); Some (_, c)] when v < 3.5 && c > 50.0 ->
        Some (key, v, c)
      | _ -> None)
    [voltage; co]
    |> collect_data in
  Printf.printf "  results: %s\n"
    (String.concat ", " (List.map (fun (k,_,_) -> k) result));
  check "exactly 1 critical (M1 when v=3.0, c=60.0)"
    (List.length result = 1);
  (match result with
   | [(k, v, c)] ->
     check (Printf.sprintf "M1 v=%.1f c=%.1f" v c)
       (k = "M1" && v = 3.0 && c = 60.0)
   | _ -> fail "wrong shape");

  (* ── 2. Все None — никаких emit ──────────────────────── *)
  Printf.printf "\n-- 2. Filter returns None for all\n";
  let s1 = [Mf_event.data ("X", 1.0) 0] |> Stream.of_list in
  let result = Pipe.keyed_join_map (module By_key)
    ~f:(fun _ _ -> None)
    [s1]
    |> collect_data in
  check "0 emissions"
    (List.length result = 0);

  (* ── 3. Watermark проходит через ─────────────────────── *)
  Printf.printf "\n-- 3. Watermark passes through\n";
  let s1 = [Mf_event.data ("A", 1.0) 0; Mf_event.wm 1000] |> Stream.of_list in
  let stream = Pipe.keyed_join_map (module By_key)
    ~f:(fun _ _ -> None)
    [s1] in
  let wm_seen = ref 0 in
  Pipe.iter_events (fun ev ->
    match ev with
    | Mf_event.Watermark _ -> incr wm_seen
    | Mf_event.Data _ | Mf_event.Retract _ | Mf_event.Update _ -> ()) stream;
  check "watermark passed"
    (!wm_seen >= 1);

  (* ── 4. Эквивалентность keyed_join + manual filter+map ── *)
  Printf.printf "\n-- 4. Equivalence with keyed_join + filter+map\n";
  let mk_streams () = (
    [Mf_event.data ("A", 1.0) 0; Mf_event.data ("A", 5.0) 1000] |> Stream.of_list,
    [Mf_event.data ("A", 10.0) 500; Mf_event.data ("A", 20.0) 1500] |> Stream.of_list
  ) in
  let s1a, s2a = mk_streams () in
  let s1b, s2b = mk_streams () in

  let via_join_map = Pipe.keyed_join_map (module By_key)
    ~f:(fun k opts ->
      match opts with
      | [Some (_, a); Some (_, b)] -> Some (k, a +. b)
      | _ -> None)
    [s1a; s2a]
    |> collect_data in

  let via_manual = Pipe.keyed_join (module By_key) [s1b; s2b] in
  let manual = ref [] in
  Pipe.iter_data (fun (k, opts) ->
    match opts with
    | [Some (_, a); Some (_, b)] -> manual := (k, a +. b) :: !manual
    | _ -> ()) via_manual;
  let via_manual_result = List.rev !manual in

  check (Printf.sprintf "via_join_map (%d) = via_manual (%d)"
           (List.length via_join_map) (List.length via_manual_result))
    (via_join_map = via_manual_result);

  Printf.printf "\nTest passed.\n"
