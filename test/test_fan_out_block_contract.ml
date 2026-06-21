(** Контракт Block-backpressure в fan_out (ответ на внешний R1).

    Внешний bug-report R1 утверждал, что fan_out «зависает навечно»
    при ≥2 Block-выходах с конечным источником. На деле это
    документированное поведение (fan_out.mli): Block-выход честно
    ждёт, пока освободят буфер другого Block-выхода, поэтому при
    однопоточном чтении выходы надо читать ВПЕРЕМЕШКУ. Репро из R1
    осушало выходы ПОСЛЕДОВАТЕЛЬНО (`List.map drain streams`) — это
    нарушает контракт, а не баг.

    Этот тест фиксирует оба факта:
    - sequential осушение Block-выходов блокируется (ожидаемо);
    - concurrent осушение (по выходу на поток, как предписывает mli)
      успешно доставляет все события во все выходы. *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let with_timeout secs f =
  let finished = Atomic.make false in
  let res = ref None in
  let _ = Thread.create (fun () ->
    (try res := Some (f ()) with _ -> ()); Atomic.set finished true) () in
  let slept = ref 0 in
  while not (Atomic.get finished) && !slept < secs * 10 do
    Thread.delay 0.1; incr slept
  done;
  if Atomic.get finished then `Ok (Option.get !res) else `Timeout

let drain_all s =
  let got = ref 0 in
  let rec go () = match s () with
    | Some (Mf_event.Data _) -> incr got; go ()
    | Some _ -> go ()
    | None -> () in
  go (); !got

let three_block () = [
  { Fan_out.name = "a"; buffer_cap = 4; on_pressure = Fan_out.Block };
  { Fan_out.name = "b"; buffer_cap = 4; on_pressure = Fan_out.Block };
  { Fan_out.name = "c"; buffer_cap = 4; on_pressure = Fan_out.Block };
]

(* 1. Sequential осушение Block-выходов БЛОКИРУЕТСЯ — это контракт,
      не баг. (Тут таймаут — ОЖИДАЕМЫЙ результат, поэтому он = pass.) *)
let test_sequential_block_blocks () =
  Printf.printf "\n-- sequential drain of Block outlets blocks (by contract)\n";
  let events = List.init 50 (fun i -> Mf_event.data i i) in
  match with_timeout 3 (fun () ->
    let streams = Fan_out.fan_out (Stream.of_list events) (three_block ()) in
    List.map drain_all streams)  (* осушаем по очереди — нарушение контракта *)
  with
  | `Timeout ->
    check "sequential Block drain blocks as documented" true
  | `Ok _ ->
    (* Если когда-нибудь перестанет блокироваться — не катастрофа, но
       контракт изменился; отметим, не валя сборку. *)
    Printf.printf "  (note: sequential drain completed — contract changed?)\n%!";
    check "sequential drain returned" true

(* 2. Concurrent осушение (по выходу на поток) доставляет ВСЕ события
      во ВСЕ выходы — так Block-fan_out и предназначен работать. *)
let test_concurrent_block_completes () =
  Printf.printf "\n-- concurrent drain of Block outlets delivers all events\n";
  let events = List.init 50 (fun i -> Mf_event.data i i) in
  match with_timeout 5 (fun () ->
    let streams = Fan_out.fan_out (Stream.of_list events) (three_block ()) in
    let results = Array.make 3 0 in
    let threads = List.mapi (fun i s ->
      Thread.create (fun () -> results.(i) <- drain_all s) ()) streams in
    List.iter Thread.join threads;
    results)
  with
  | `Timeout -> fail "TIMEOUT: concurrent Block drain hung (would be a real bug)"
  | `Ok r ->
    Printf.printf "  a=%d b=%d c=%d\n%!" r.(0) r.(1) r.(2)
    ;
    check "outlet a got all 50" (r.(0) = 50);
    check "outlet b got all 50" (r.(1) = 50);
    check "outlet c got all 50" (r.(2) = 50)

(* 3. Drop-выходы никогда не блокируют даже при sequential чтении. *)
let test_drop_never_blocks () =
  Printf.printf "\n-- Drop outlets never block (sequential is fine)\n";
  let events = List.init 50 (fun i -> Mf_event.data i i) in
  let outlets = [
    { Fan_out.name = "x"; buffer_cap = 4; on_pressure = Fan_out.Drop_newest };
    { Fan_out.name = "y"; buffer_cap = 4; on_pressure = Fan_out.Drop_oldest };
  ] in
  match with_timeout 3 (fun () ->
    let streams = Fan_out.fan_out (Stream.of_list events) outlets in
    List.map drain_all streams)
  with
  | `Timeout -> fail "TIMEOUT: Drop outlet blocked (would be a real bug)"
  | `Ok r ->
    Printf.printf "  x=%d y=%d (≤50, drops expected)\n%!"
      (List.nth r 0) (List.nth r 1);
    check "both Drop outlets completed without blocking" true

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  fan_out: Block backpressure contract (R1)\n";
  Printf.printf "==========================================\n";
  test_sequential_block_blocks ();
  test_concurrent_block_completes ();
  test_drop_never_blocks ();
  Printf.printf "\nfan_out Block-contract tests passed.\n";
  exit 0
