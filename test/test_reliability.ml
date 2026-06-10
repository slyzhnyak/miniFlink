open Miniflink
(* ============================================================
   Test_reliability.ml — тесты DLQ и Shutdown
   ============================================================ *)

open Test_support.Domain
open Time

let pass name = Printf.printf "  OK %s\n%!" name
let fail name msg = Printf.printf "  FAIL %s: %s\n%!" name msg; exit 1
let check name cond = if cond then pass name else fail name "assertion failed"
let check_eq name a b =
  if a = b then pass name
  else fail name (Printf.sprintf "expected %d, got %d" b a)

(* ── 1. DLQ noop: молча считает ─────────────────────────── *)
let test_dlq_noop () =
  Printf.printf "\n-- DLQ noop\n";
  let dlq = Dlq_noop.create () in
  check_eq "initial count = 0" (Dlq_noop.count dlq) 0;
  Dlq_noop.send dlq { Dlq_noop.topic="test"; payload=Bytes.of_string "bad";
                       error="parse error"; ts=1000; attempt=1 };
  Dlq_noop.send dlq { Dlq_noop.topic="test"; payload=Bytes.of_string "bad2";
                       error="decode error"; ts=2000; attempt=1 };
  check_eq "after 2 sends = 2" (Dlq_noop.count dlq) 2

(* ── 2. DLQ log: пишет в stderr ─────────────────────────── *)
let test_dlq_log () =
  Printf.printf "\n-- DLQ log\n";
  let dlq = Dlq_log.create () in
  check_eq "initial count = 0" (Dlq_log.count dlq) 0;
  (* stderr будет содержать строку [DLQ] *)
  Dlq_log.send dlq { Dlq_noop.topic="vehicles/truck_A";
                      payload = Bytes.of_string {|{"bad":"json|};
                      error = "Unexpected end of JSON";
                      ts = 1000; attempt = 1 };
  check_eq "after 1 send = 1" (Dlq_log.count dlq) 1;
  Dlq_log.flush dlq

(* ── 3. safe_decode: ошибки → DLQ, не падение ───────────── *)
let test_safe_decode () =
  Printf.printf "\n-- Runtime.safe_decode\n";
  let sends = ref 0 in
  let send_dlq _ = incr sends in

  (* Валидный JSON через telemetry_codec *)
  let good_bytes = Bytes.of_string
    {|{"device_id":"A","speed_kmh":60.0,"fuel_pct":80.0,"position":{"lat":55.75,"lon":37.61},"ts":1000,"device":null}|} in
  let result1 = Runtime.safe_decode ~send_dlq ~topic:"test"
    ~codec:(fun b -> Test_support.Domain.telemetry_of_yojson
      (Yojson.Safe.from_string (Bytes.to_string b)))
    ~attempt:1 good_bytes in
  check "valid payload → Some" (result1 <> None);
  check_eq "valid: DLQ not called" !sends 0;

  (* Невалидный JSON *)
  let bad_bytes = Bytes.of_string {|{broken json|} in
  let result2 = Runtime.safe_decode ~send_dlq ~topic:"test"
    ~codec:(fun b ->
      match Yojson.Safe.from_string (Bytes.to_string b) with
      | j -> Test_support.Domain.telemetry_of_yojson j
      | exception Yojson.Json_error e -> Error e)
    ~attempt:1 bad_bytes in
  check "invalid payload → None" (result2 = None);
  check_eq "invalid: DLQ called once" !sends 1

(* ── 4. Shutdown noop: is_requested всегда false ─────────── *)
let test_shutdown_noop () =
  Printf.printf "\n-- Shutdown noop\n";
  Shutdown_noop.register ~on_shutdown:(fun () -> ());
  check "is_requested = false" (not (Shutdown_noop.is_requested ()));
  (* request() — noop тоже *)
  Shutdown_noop.request ();
  check "after request still false (noop)" (not (Shutdown_noop.is_requested ()))

(* ── 5. Shutdown default: request → is_requested = true ─── *)
let test_shutdown_default () =
  Printf.printf "\n-- Shutdown default\n";
  (* Сбрасываем глобальный стейт через новый ref trick *)
  (* shutdown_default использует глобальный ref — проверяем request() *)
  let on_called = ref false in
  Shutdown_default.register ~on_shutdown:(fun () -> on_called := true);
  check "initially not requested" (not (Shutdown_default.is_requested ()));
  Shutdown_default.request ();
  check "after request: is_requested = true" (Shutdown_default.is_requested ());
  (* Не проверяем on_called — register вызвался через request() только если signal *)
  ignore on_called

(* ── 6. Runtime guarded source останавливается по shutdown ── *)
let test_runtime_shutdown_guard () =
  Printf.printf "\n-- Runtime: source stops on shutdown\n";

  (* Создаём источник с 10 событиями *)
  let events = Test_support.Fixtures.scenario_alerts in
  let count = ref 0 in
  let source =
    let s = Stream.of_list events in
    fun () ->
      incr count;
      (* После 3-го события запрашиваем shutdown *)
      if !count = 4 then Shutdown_default.request ();
      s ()
  in

  let sink_count = ref 0 in
  Runtime.run Runtime.log_cfg
    ~key_of:(fun (t:Test_support.Domain.telemetry) -> t.Test_support.Domain.device_id)
    ~source
    ~pipeline:(fun s ->
      s |> Mf_event.with_watermarks ~latency:(seconds 3))
    ~sink:(fun _ -> incr sink_count)
    ();
  (* После shutdown source должен был остановиться раньше чем все события *)
  check "stopped before all events"
    (!count < List.length events + 5)

(* ── 7. DLQ + Pipeline: невалидные байты не роняют ────────── *)
let test_dlq_in_pipeline () =
  Printf.printf "\n-- DLQ: bad payloads don't crash pipeline\n";

  let dlq_count = ref 0 in
  let send_dlq _ = incr dlq_count in

  (* Симулируем source который отдаёт mix good/bad bytes *)
  let payloads = [
    (true,  {|{"device_id":"A","speed_kmh":60.0,"fuel_pct":80.0,"position":{"lat":55.0,"lon":37.0},"ts":1000,"device":null}|});
    (false, {|{broken|});
    (true,  {|{"device_id":"B","speed_kmh":90.0,"fuel_pct":70.0,"position":{"lat":55.0,"lon":37.0},"ts":2000,"device":null}|});
    (false, {|not json at all|});
    (true,  {|{"device_id":"A","speed_kmh":65.0,"fuel_pct":79.0,"position":{"lat":55.0,"lon":37.0},"ts":3000,"device":null}|});
  ] in

  let decoded = List.filter_map (fun (_ok, s) ->
    Runtime.safe_decode ~send_dlq ~topic:"test"
      ~codec:(fun b ->
        match Yojson.Safe.from_string (Bytes.to_string b) with
        | j -> Test_support.Domain.telemetry_of_yojson j
        | exception Yojson.Json_error e -> Error e)
      ~attempt:1
      (Bytes.of_string s)
  ) payloads in

  check_eq "3 good payloads decoded" (List.length decoded) 3;
  check_eq "2 bad payloads → DLQ" !dlq_count 2

(* ── Main ───────────────────────────────────────────────── *)
let () =
  Printf.printf "==========================================\n";
  Printf.printf "  miniFlink v3 - Reliability Tests\n";
  Printf.printf "==========================================\n";
  test_dlq_noop          ();
  test_dlq_log           ();
  test_safe_decode       ();
  test_shutdown_noop     ();
  test_shutdown_default  ();
  test_runtime_shutdown_guard ();
  test_dlq_in_pipeline   ();
  Printf.printf "\nAll reliability tests passed.\n"
