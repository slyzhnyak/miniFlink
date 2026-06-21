(** Тест: Trigger.of_config — record-based alternative API.

    Покрывает:
    1. default_config + of_config дают идентичный результат с create
    2. Template pattern: базовый config + with-override
    3. serdes bundle работает (persistence не падает) *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

type alert =
  | A_problem of string * float * Time.t
  | A_recovery of string * Time.t

let () =
  Printf.printf "Test: Trigger.of_config\n%!";

  (* ── 1. of_config equivalent to create ─────────────── *)
  Printf.printf "\n-- 1. of_config equivalent to create\n";

  let spec_via_create = Trigger.create
    ~name:"low_voltage"
    ~condition:(Trigger.less_than 3.5)
    ~problem_for:0
    ~severity:Trigger.Warning
    ~produce_alert:(fun ~key ~value ~ts -> A_problem (key, value, ts))
    ~produce_recovery:(fun ~key ~ts -> A_recovery (key, ts))
    () in

  let spec_via_config = Trigger.of_config {
    (Trigger.default_config
      ~name:"low_voltage"
      ~condition:(Trigger.less_than 3.5)
      ~produce_alert:(fun ~key ~value ~ts -> A_problem (key, value, ts))
      ~produce_recovery:(fun ~key ~ts -> A_recovery (key, ts)))
    with severity = Trigger.Warning
  } in

  check "names match"
    (Trigger.name spec_via_create = Trigger.name spec_via_config);
  check "severity match"
    (Trigger.severity spec_via_create = Trigger.severity spec_via_config);

  (* Тест поведения: тот же поток событий → те же alerts *)
  let events = [
    Mf_event.data ("A", 4.0) 0;
    Mf_event.data ("A", 3.0) 1000;  (* low voltage — fire *)
    Mf_event.data ("A", 5.0) 2000;  (* recovery *)
    Mf_event.wm 3000;
  ] in

  let alerts_create = events |> Stream.of_list
    |> Trigger.of_stream spec_via_create
    |> Pipe.collect in
  let alerts_config = events |> Stream.of_list
    |> Trigger.of_stream spec_via_config
    |> Pipe.collect in
  check (Printf.sprintf "behavior identical: create=%d config=%d alerts"
           (List.length alerts_create) (List.length alerts_config))
    (alerts_create = alerts_config);

  (* ── 2. Template pattern — base + override ─────────── *)
  Printf.printf "\n-- 2. Template pattern\n";

  let base = Trigger.default_config
    ~name:"voltage_template"
    ~condition:(Trigger.less_than 0.0)  (* placeholder *)
    ~produce_alert:(fun ~key ~value ~ts -> A_problem (key, value, ts))
    ~produce_recovery:(fun ~key ~ts -> A_recovery (key, ts)) in

  let low = Trigger.of_config {
    base with
      name = "low_voltage";
      condition = Trigger.less_than 3.5
  } in
  let critical = Trigger.of_config {
    base with
      name = "critical_voltage";
      condition = Trigger.less_than 3.0;
      severity = Trigger.High
  } in

  check "low spec name" (Trigger.name low = "low_voltage");
  check "critical spec name" (Trigger.name critical = "critical_voltage");
  check "low severity = Warning (default)"
    (Trigger.severity low = Trigger.Warning);
  check "critical severity = High (overridden)"
    (Trigger.severity critical = Trigger.High);

  (* Behaviour: разные thresholds *)
  let events = [
    Mf_event.data ("M", 3.2) 0;     (* low fires, critical doesn't *)
    Mf_event.wm 1000;
  ] in
  let low_alerts = events |> Stream.of_list
    |> Trigger.of_stream low |> Pipe.collect in
  let crit_alerts = events |> Stream.of_list
    |> Trigger.of_stream critical |> Pipe.collect in
  check "low fires on 3.2 < 3.5"   (List.length low_alerts >= 1);
  check "critical doesn't fire on 3.2 ≥ 3.0"
    (List.length crit_alerts = 0);

  (* ── 3. persistence через Runtime_context (ортогонально) ─── *)
  Printf.printf "\n-- 3. orthogonal persistence via context\n";
  let tbl = Hashtbl.create 16 in
  let backend = Persistence_backend.of_memory tbl in
  let ctx = Runtime_context.durable backend in

  let with_persist = Trigger.of_config {
    base with
      name = "persisted_low";
      condition = Trigger.less_than 3.5;
  } in

  Runtime_context.with_context ctx (fun () ->
    let _ = events |> Stream.of_list
      |> Trigger.of_stream with_persist
      |> Pipe.collect in
    check "backend has records after run" (Hashtbl.length tbl > 0));

  Printf.printf "\nTest passed.\n"
