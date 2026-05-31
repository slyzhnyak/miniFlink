(* ============================================================
   Test_invariants.ml — усиленные инварианты надёжности.

   Три части:
   A. Инварианты КОМПОЗИЦИИ операторов (глубже чем отдельные операторы):
      законы должны держаться для любых данных и любых параметров.
   B. Round-trip стейта: restore(snapshot(s)) = s — фундамент recovery.
   C. Граничные «пустые» случаи: пустой поток, ноль событий, одиночные
      элементы через всю цепочку — частый источник скрытых багов.
   ============================================================ *)

open QCheck
open Domain
open Time

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

(* ── Генераторы ─────────────────────────────────────────── *)
let gen_id = Gen.oneofl ["A"; "B"; "C"; "D"]

let gen_tel =
  Gen.map (fun (id, sp, t) ->
    { device_id = id; speed_kmh = sp; fuel_pct = 80.;
      position = { lat = 55.; lon = 37. }; ts = t; device = None })
    (Gen.tup3 gen_id (Gen.float_range 0. 200.) (Gen.int_range 0 3_600_000))

let gen_events n = Gen.list_size (Gen.return n)
  (Gen.map (fun (t:telemetry) -> Mf_event.data t t.ts) gen_tel)

let data_values s =
  Stream.to_list s |> List.filter_map (function
    | Mf_event.Data (v, _) -> Some v | _ -> None)

(* ════════════════════════════════════════════════════════
   ЧАСТЬ A — Инварианты композиции
   ════════════════════════════════════════════════════════ *)

(* A1. map f >> map g == map (g∘f) по значениям (функториальность) *)
let prop_map_fusion =
  Test.make ~count:300 ~name:"map fusion: map g (map f s) = map (g∘f) s"
    (make (gen_events 30)) (fun evs ->
      let f (t:telemetry) = { t with speed_kmh = t.speed_kmh +. 1. } in
      let g (t:telemetry) = { t with speed_kmh = t.speed_kmh *. 2. } in
      let lhs = Stream.of_list evs |> Pipe.map f |> Pipe.map g |> data_values in
      let rhs = Stream.of_list evs |> Pipe.map (fun x -> g (f x)) |> data_values in
      lhs = rhs)

(* A2. filter p >> filter q == filter (x. p x && q x) *)
let prop_filter_compose =
  Test.make ~count:300 ~name:"filter compose: filter q (filter p) = filter (p&&q)"
    (make (gen_events 40)) (fun evs ->
      let p (t:telemetry) = t.speed_kmh > 50. in
      let q (t:telemetry) = t.speed_kmh < 150. in
      let lhs = Stream.of_list evs |> Pipe.filter p |> Pipe.filter q |> data_values in
      let rhs = Stream.of_list evs |> Pipe.filter (fun x -> p x && q x) |> data_values in
      lhs = rhs)

(* A3. window сохраняет события при ЛЮБОМ размере окна (>0) *)
let prop_window_conserves_any_size =
  Test.make ~count:300 ~name:"window conserves events for any size>0"
    (make (Gen.tup2 (gen_events 50) (Gen.int_range 1 100))) (fun (evs, sz) ->
      let total_in = List.length evs in
      let total_out =
        Stream.of_list evs
        |> Mf_event.with_watermarks ~latency:(seconds 1)
        |> Pipe.window (module Telemetry) (Pipe.tumbling (seconds sz))
        |> Pipe.aggregate (fun _k es -> List.length es)
        |> Stream.to_list
        |> List.filter_map (function Mf_event.Data (n,_) -> Some n | _ -> None)
        |> List.fold_left (+) 0
      in total_out = total_in)

(* A4. dedup идемпотентен при ЛЮБОМ cooldown *)
let prop_dedup_idempotent_any =
  Test.make ~count:200 ~name:"dedup idempotent for any cooldown"
    (make (Gen.tup2
       (Gen.list_size (Gen.int_range 0 30)
          (Gen.map (fun (r, t) ->
             { id = Printf.sprintf "a%d" t; rule = r; device_id = "A";
               severity = Warning; message = ""; ts = t })
             (Gen.tup2 (Gen.oneofl ["over";"low";"geo"]) (Gen.int_range 0 600000))))
       (Gen.int_range 1 600000)))
    (fun (alerts, cd) ->
      let dd src = src |> Pipe.dedup (module Alert) ~rule:(fun a -> a.rule) ~cooldown:cd in
      let run s = Stream.of_list (List.map (fun a -> Mf_event.data a a.ts) s) |> dd |> data_values in
      let once = run alerts in
      let twice = run once in
      once = twice)

(* A5. enrich сохраняет порядок и число для любой таблицы *)
let prop_enrich_preserves =
  Test.make ~count:200 ~name:"enrich preserves count & order (left join)"
    (make (gen_events 30)) (fun evs ->
      let devices = Table.of_list [("A", "owner-A"); ("C", "owner-C")] in
      let inp = data_values (Stream.of_list evs) in
      let out =
        Stream.of_list evs
        |> Pipe.enrich (module Telemetry) ~from:devices
             ~merge:(fun t _d -> t)
        |> data_values
      in
      List.length out = List.length inp
      && List.map (fun (t:telemetry) -> t.device_id) out
         = List.map (fun (t:telemetry) -> t.device_id) inp)

(* ════════════════════════════════════════════════════════
   ЧАСТЬ B — Round-trip стейта (фундамент recovery)
   ════════════════════════════════════════════════════════ *)

let prop_state_roundtrip =
  Test.make ~count:200 ~name:"state: restore(snapshot(s)) preserves all keys"
    (make (Gen.list_size (Gen.int_range 0 40)
       (Gen.tup2 (Gen.string_size (Gen.int_range 1 8) ~gen:Gen.printable)
                 (Gen.string_size (Gen.int_range 0 16) ~gen:Gen.printable))))
    (fun kvs ->
      let s = State_backend_memory.create () in
      List.iter (fun (k, v) -> State_backend_memory.set s k (Bytes.of_string v)) kvs;
      let snap = State_backend_memory.snapshot s in
      let s2 = State_backend_memory.create () in
      State_backend_memory.restore s2 snap;
      (* все ключи и значения совпадают *)
      List.for_all (fun (k, _) ->
        State_backend_memory.get s k = State_backend_memory.get s2 k)
        kvs
      && State_backend_memory.size s = State_backend_memory.size s2)

(* ════════════════════════════════════════════════════════
   ЧАСТЬ C — Граничные «пустые»/одиночные случаи
   ════════════════════════════════════════════════════════ *)

let test_empty_and_singleton () =
  Printf.printf "\n-- Edge cases: empty stream, no data, single element\n";

  (* пустой поток через всю цепочку — не падает, даёт пусто *)
  let empty_out =
    Stream.empty
    |> Mf_event.with_watermarks ~latency:(seconds 1)
    |> Pipe.window (module Telemetry) (Pipe.tumbling (seconds 30))
    |> Pipe.aggregate (fun _k es -> List.length es)
    |> Stream.to_list
  in
  check "empty stream → empty output (no crash)" (empty_out = []);

  (* только watermark, без data — окна не выдают результатов *)
  let wm_only =
    Stream.of_list [Mf_event.wm (seconds 100)]
    |> Pipe.window (module Telemetry) (Pipe.tumbling (seconds 30))
    |> data_values
  in
  check "watermark-only stream → no windows fired" (wm_only = []);

  (* одно событие — попадает ровно в одно окно *)
  let one =
    Stream.of_list [Mf_event.data
      { device_id = "A"; speed_kmh = 60.; fuel_pct = 50.;
        position = { lat = 55.; lon = 37. }; ts = seconds 5; device = None } (seconds 5)]
    |> Mf_event.with_watermarks ~latency:0
    |> Pipe.window (module Telemetry) (Pipe.tumbling (seconds 30))
    |> Pipe.aggregate (fun _k es -> List.length es)
    |> Stream.to_list
    |> List.filter_map (function Mf_event.Data (n,_) -> Some n | _ -> None)
  in
  check "single event → exactly one window with count 1" (one = [1]);

  (* пустой стейт: snapshot/restore пустого backend *)
  let s = State_backend_memory.create () in
  let snap = State_backend_memory.snapshot s in
  let s2 = State_backend_memory.create () in
  State_backend_memory.restore s2 snap;
  check "empty state round-trip → still empty" (State_backend_memory.size s2 = 0);

  (* dedup пустого потока *)
  let dd_empty =
    Stream.empty |> Pipe.dedup (module Alert) ~rule:(fun a -> a.rule) ~cooldown:(minutes 5)
    |> Stream.to_list in
  check "dedup empty → empty" (dd_empty = []);

  (* table пустой — enrich на пустой таблице не падает *)
  let empty_tbl = Table.of_list [] in
  let enr =
    Stream.of_list [Mf_event.data
      { device_id = "Z"; speed_kmh = 1.; fuel_pct = 1.;
        position = { lat = 0.; lon = 0. }; ts = 0; device = None } 0]
    |> Pipe.enrich (module Telemetry) ~from:empty_tbl ~merge:(fun t _ -> t)
    |> data_values in
  check "enrich with empty table → passes through" (List.length enr = 1)

(* ── Runner ─────────────────────────────────────────────── *)
let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Reliability invariants (composition/state/edges)\n";
  Printf.printf "==========================================\n";
  Printf.printf "\n-- Part A: composition invariants (QCheck)\n";
  let props = [
    prop_map_fusion; prop_filter_compose; prop_window_conserves_any_size;
    prop_dedup_idempotent_any; prop_enrich_preserves; prop_state_roundtrip;
  ] in
  let r = QCheck_base_runner.run_tests ~verbose:false props in
  if r <> 0 then (Printf.printf "  FAIL: property violated\n"; exit 1);
  pass "all composition + state-roundtrip properties hold";
  test_empty_and_singleton ();
  Printf.printf "\nAll invariant tests passed.\n"
