(* ============================================================
   Main.ml

   Весь pipeline — 10 строк.
   Нет аннотаций типов внутри |> цепочки.
   Нет повторения ключа.
   Нет низкоуровневого кодирования.
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

(* ── Sink: напечатать алерт ───────────────────────────────── *)

let print_alert a =
  let sev = match a.severity with
    | Critical -> "CRITICAL" | Warning -> "WARNING" | Info -> "INFO"
  in
  Printf.printf "[%s] %s (%s): %s\n%!" sev a.device_id a.rule a.message

(* ── Run ──────────────────────────────────────────────────── *)

let run_test () =
  Fixtures.scenario_alerts
  |> Stream.of_list
  |> pipeline
  |> Pipe.sink print_alert

let () = run_test ()
