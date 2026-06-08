open Miniflink
(* Тест idle watermark. Часы (now_ms) и пауза (sleep_ms) вынесены
   параметрами — тест подаёт управляемое время, без реального ожидания. *)

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

(* Управляемые часы *)
let make_clock () =
  let t = ref 0 in
  let now () = !t in
  let advance d = t := !t + d in
  (now, advance)

let collect stream =
  let out = ref [] in
  let rec drain () = match stream () with
    | Some x -> out := x :: !out; drain ()
    | None -> () in
  drain (); List.rev !out

(* ── idle watermark двигается когда источник молчит ────────── *)
let test_idle_advances () =
  Printf.printf "\n-- watermark advances on wall-clock during silence\n";
  let (now, advance) = make_clock () in
  (* источник: одно событие, потом долгая тишина, потом конец *)
  let script = ref [
    `Ev (Mf_event.data "a" 1000);   (* данные на event-time 1000 *)
    `Silent; `Tick;                  (* тишина — продвинем часы *)
    `Silent; `Tick;
    `End
  ] in
  let poll () =
    match !script with
    | [] -> None
    | `Ev e :: rest -> script := rest; Some (Some e)
    | `Silent :: rest -> script := rest; Some None
    | `Tick :: rest -> script := rest; advance 6000; Some None  (* >idle_ms *)
    | `End :: rest -> script := rest; None
  in
  let stream = Mf_event.with_idle_watermarks
    ~latency:0 ~idle_ms:5000 ~idle_advance:1000
    ~now_ms:now ~sleep_ms:(fun _ -> ())
    poll in
  let evs = collect stream in
  (* должны увидеть: Data "a", потом хотя бы один idle Watermark *)
  let has_data = List.exists (function Mf_event.Data ("a", _) -> true | _ -> false) evs in
  let watermarks = List.filter_map (function Mf_event.Watermark w -> Some w | _ -> None) evs in
  check "data event passed through" has_data;
  check "at least one idle watermark emitted" (List.length watermarks >= 1);
  check "watermark advanced beyond data event-time"
    (List.exists (fun w -> w > 1000) watermarks)

(* ── без тишины (данные идут) — idle не вмешивается лишний раз ── *)
let test_no_spurious_idle () =
  Printf.printf "\n-- steady data: idle does not fabricate extra watermarks\n";
  let (now, advance) = make_clock () in
  let script = ref [
    `Ev (Mf_event.data "a" 100);
    `Ev (Mf_event.data "b" 200);
    `Ev (Mf_event.data "c" 300);
    `End ] in
  let poll () = match !script with
    | [] -> None
    | `Ev e :: rest -> script := rest; advance 10; Some (Some e)  (* < idle_ms *)
    | `End :: rest -> script := rest; None
    | _ -> None in
  let stream = Mf_event.with_idle_watermarks
    ~latency:0 ~idle_ms:5000 ~idle_advance:1000
    ~now_ms:now ~sleep_ms:(fun _ -> ())
    poll in
  let evs = collect stream in
  let data = List.filter (function Mf_event.Data _ -> true | _ -> false) evs in
  check "all 3 data events passed" (List.length data = 3);
  (* при активных данных idle-watermark не должен напридумывать лишних:
     допустим только финальный watermark в конце потока *)
  let wms = List.filter (function Mf_event.Watermark _ -> true | _ -> false) evs in
  check "no spurious idle watermarks during steady flow (<=1 final)"
    (List.length wms <= 1)

(* ── конец потока даёт финальный watermark ─────────────────── *)
let test_final_watermark () =
  Printf.printf "\n-- end of stream emits final watermark = max_seen\n";
  let (now, _) = make_clock () in
  let script = ref [ `Ev (Mf_event.data "x" 500); `End ] in
  let poll () = match !script with
    | `Ev e :: rest -> script := rest; Some (Some e)
    | `End :: rest -> script := rest; None
    | _ -> None in
  let stream = Mf_event.with_idle_watermarks
    ~latency:0 ~idle_ms:1000 ~idle_advance:100
    ~now_ms:now ~sleep_ms:(fun _ -> ())
    poll in
  let evs = collect stream in
  let wms = List.filter_map (function Mf_event.Watermark w -> Some w | _ -> None) evs in
  check "final watermark present" (List.exists (fun w -> w = 500) wms)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Idle watermark (wall-clock during silence)\n";
  Printf.printf "==========================================\n";
  test_idle_advances ();
  test_no_spurious_idle ();
  test_final_watermark ();
  Printf.printf "\nAll idle watermark tests passed.\n"
