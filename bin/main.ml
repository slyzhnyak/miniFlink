(* ============================================================
   Main.ml — декларативный pipeline

   Демонстрирует runtime с DLQ и Shutdown:
   - run_test   : noop режим, тестовые данные
   - run_log    : log режим, метрики в stderr + shutdown handler
   ============================================================ *)

open Time
open Domain

(* ── Справочник устройств ─────────────────────────────────── *)

let devices = Table.of_list [
  "truck_A", { owner = "ООО Яблоки"; max_speed = 90.; zone = "north" };
  "truck_B", { owner = "ООО Металл"; max_speed = 85.; zone = "south" };
  "truck_C", { owner = "ООО Молоко"; max_speed = 80.; zone = "north" };
]

(* ── Pipeline ─────────────────────────────────────────────── *)

let pipeline source =
  source
  |> Mf_event.with_watermarks   ~latency:(seconds 3)
  |> Pipe.enrich (module Telemetry)
       ~from:devices
       ~merge:(fun t dev -> { t with device = dev })
  |> Pipe.window (module Telemetry) (Pipe.tumbling (seconds 30))
  |> Pipe.aggregate              Rules.compute
  |> Pipe.flat_map               (Rules.check Rules.fleet)
  |> Pipe.dedup (module Alert)
       ~rule:(fun a -> a.rule)
       ~cooldown:(minutes 5)

(* ── Sink ─────────────────────────────────────────────────── *)

(* Без "%!" в hot path: флеш на каждое событие = системный вызов
   на каждый алерт. Буферизуем, флешим один раз в конце (at_exit). *)
let print_alert a =
  let sev = match a.severity with
    | Critical -> "CRITICAL" | Warning -> "WARNING" | Info -> "INFO"
  in
  Printf.printf "[%s] %s (%s): %s\n" sev a.device_id a.rule a.message

let () = at_exit (fun () -> flush stdout)

(* ── Run: noop (тесты) ────────────────────────────────────── *)

let run_test () =
  Printf.printf "=== Noop mode (no DLQ, no shutdown handler) ===\n";
  let source = Stream.of_list Fixtures.scenario_alerts in
  Runtime.run Runtime.noop
    ~key_of:(fun (t:telemetry) -> t.device_id)
    ~source
    ~pipeline
    ~sink:print_alert
    ()

(* ── Run: log (метрики + shutdown) ───────────────────────── *)

let run_log () =
  Printf.printf "=== Log mode (metrics to stderr + shutdown handler) ===\n";
  Printf.printf "(Send SIGTERM or SIGINT to test graceful shutdown)\n";
  let source = Stream.of_list Fixtures.scenario_alerts in
  Runtime.run Runtime.log_cfg
    ~key_of:(fun (t:telemetry) -> t.device_id)
    ~source
    ~pipeline
    ~sink:print_alert
    ()

(* ── Entry point ──────────────────────────────────────────── *)

let () =
  match Sys.argv with
  | [| _; "log" |] -> run_log ()
  | _              -> run_test ()
