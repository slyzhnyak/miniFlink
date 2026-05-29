(* ============================================================
   Rules.ml — бизнес-логика: агрегация и правила

   Чистые функции. Никакой зависимости от фреймворка.
   ============================================================ *)

open Domain

(* ── Агрегация ────────────────────────────────────────────── *)

let compute key (events : telemetry list) : stats =
  match events with
  | [] ->
    { device_id = key; max_speed = 0.; avg_speed = 0.;
      min_fuel = 100.; count = 0; device = None }
  | _  ->
    let n   = float_of_int (List.length events) in
    let dev = List.find_map (fun (t:telemetry) -> t.device) events in
    { device_id = key;
      max_speed = List.fold_left (fun m t -> max m t.speed_kmh) 0.   events;
      avg_speed = List.fold_left (fun s t -> s +. t.speed_kmh)  0.   events /. n;
      min_fuel  = List.fold_left (fun m t -> min m t.fuel_pct)  100. events;
      count     = List.length events;
      device    = dev }

(* ── Правила ──────────────────────────────────────────────── *)

let alert ~rule ~severity ~msg (s : stats) =
  { id        = Printf.sprintf "%s_%s" s.device_id rule;
    device_id = s.device_id;
    rule;
    severity;
    message   = msg s;
    ts        = 0 }

let speed_limit (s : stats) : alert list =
  let limit = match s.device with Some d -> d.max_speed | None -> 90. in
  if s.max_speed > limit *. 1.3 then
    [alert ~rule:"speed_critical" ~severity:Critical
       ~msg:(fun s -> Printf.sprintf "%.0f km/h, limit %.0f" s.max_speed limit) s]
  else if s.max_speed > limit then
    [alert ~rule:"speed_warning" ~severity:Warning
       ~msg:(fun s -> Printf.sprintf "%.0f km/h" s.max_speed) s]
  else []

let fuel_level (s : stats) : alert list =
  if s.min_fuel < 10. then
    [alert ~rule:"fuel_critical" ~severity:Critical
       ~msg:(fun s -> Printf.sprintf "fuel %.0f%%" s.min_fuel) s]
  else if s.min_fuel < 20. then
    [alert ~rule:"fuel_warning" ~severity:Warning
       ~msg:(fun s -> Printf.sprintf "fuel %.0f%%" s.min_fuel) s]
  else []

(* Список правил — просто список функций stats -> alert list *)
let fleet = [speed_limit; fuel_level]

let check rules (s : stats) : alert list =
  List.concat_map (fun rule -> rule s) rules
