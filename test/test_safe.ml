open Miniflink
(* Тест изоляции исключений: safe_map/safe_filter ловят исключение из
   пользовательской функции, зовут on_error и пропускают событие, не
   роняя поток. Контраст с обычным map который роняет. *)

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let data_out s =
  Stream.to_list s |> List.filter_map (function
    | Mf_event.Data (v, _) -> Some v | _ -> None)

(* ── обычный map роняет весь поток на исключении ───────────── *)
let test_plain_map_propagates () =
  Printf.printf "\n-- plain map propagates exception (baseline)\n";
  let s = Stream.of_list [Mf_event.data 1 0; Mf_event.data 2 10; Mf_event.data 3 20] in
  let propagated =
    try
      s |> Pipe.map (fun x -> if x = 2 then failwith "boom" else x)
        |> data_out |> ignore;
      false
    with _ -> true in
  check "plain map lets exception kill the pipeline" propagated

(* ── safe_map изолирует: пропускает битое, поток жив ───────── *)
let test_safe_map_isolates () =
  Printf.printf "\n-- safe_map isolates: bad event skipped, stream survives\n";
  let s = Stream.of_list [Mf_event.data 1 0; Mf_event.data 2 10; Mf_event.data 3 20] in
  let errors = ref 0 in
  let out =
    s |> Pipe.safe_map ~on_error:(fun _ -> incr errors)
           (fun x -> if x = 2 then failwith "boom" else x * 10)
      |> data_out in
  check "good events processed, bad skipped" (out = [10; 30]);
  check "on_error called once" (!errors = 1)

(* ── safe_map: watermark проходит ──────────────────────────── *)
let test_safe_map_watermark () =
  Printf.printf "\n-- safe_map passes watermarks\n";
  let s = Stream.of_list [Mf_event.data 1 0; Mf_event.wm 50; Mf_event.data 2 60] in
  let all = s |> Pipe.safe_map ~on_error:(fun _ -> ()) (fun x -> x)
              |> Stream.to_list in
  let wms = List.filter (function Mf_event.Watermark _ -> true | _ -> false) all in
  check "watermark preserved" (List.length wms = 1)

(* ── safe_filter изолирует исключение из предиката ─────────── *)
let test_safe_filter () =
  Printf.printf "\n-- safe_filter isolates predicate exceptions\n";
  let s = Stream.of_list [Mf_event.data 1 0; Mf_event.data 2 10; Mf_event.data 3 20] in
  let errors = ref 0 in
  let out =
    s |> Pipe.safe_filter ~on_error:(fun _ -> incr errors)
           (fun x -> if x = 2 then failwith "bad predicate" else x > 1)
      |> data_out in
  (* x=1: p=false (отброшен), x=2: исключение (отброшен), x=3: p=true *)
  check "kept only x=3" (out = [3]);
  check "predicate exception counted" (!errors = 1)

(* ── все события битые — поток не падает, просто пустой ────── *)
let test_all_bad () =
  Printf.printf "\n-- all events bad: empty output, no crash\n";
  let s = Stream.of_list [Mf_event.data 1 0; Mf_event.data 2 10] in
  let errors = ref 0 in
  let out = s |> Pipe.safe_map ~on_error:(fun _ -> incr errors)
                  (fun _ -> failwith "always")
              |> data_out in
  check "empty output" (out = []);
  check "all errors reported" (!errors = 2)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Exception isolation (safe_map / safe_filter)\n";
  Printf.printf "==========================================\n";
  test_plain_map_propagates ();
  test_safe_map_isolates ();
  test_safe_map_watermark ();
  test_safe_filter ();
  test_all_bad ();
  Printf.printf "\nAll exception isolation tests passed.\n"
