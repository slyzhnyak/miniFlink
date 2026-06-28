(** Тесты пайплайна [Pipelines.gas_alerts].

    Семантика retract-based газовых алертов сложная — нужны явные
    регрессионные тесты для каждого случая. Подход: построить три
    стрима (rssi, locations, gas) из списков, прогнать через gas_alerts,
    проверить что эмитированный поток Data/Retract событий соответствует
    ожидаемому.

    Tested scenarios:
    - thresholds: ppm < warn → silent; ppm ≥ warn → Gas_alert(Warning);
      ppm ≥ crit → Gas_alert(Critical)
    - sticky: повторный gas в пределах 20% не эмитит ничего
    - level transition: Warning→Critical → атомарный Update
    - ppm refresh: изменение >20% при том же уровне → атомарный Update
    - resolved: возврат в норму → Retract + Gas_resolved (исчезновение)
    - position arrives later (ГЛАВНЫЙ КЕЙС из ТЗ): gas первый, RSSI
      позже → Update с новой position *)

open Miniflink
open Ex07_location_lib
open Domain

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

(** Запустить gas_alerts на трёх списках и собрать ВСЕ Mf_event'ы. *)
let run_pipeline
    ?(rssi_packets = []) ?(locations = []) ?(gas_packets = [])
    () : gas_alert Mf_event.t list =
  let s_rssi = Mf_event.of_list ~ts:(fun (p : packet) -> p.ts) rssi_packets in
  let s_loc  = Mf_event.of_list ~ts:(fun (l : location) -> l.loc_wend) locations in
  let s_gas  = Mf_event.of_list ~ts:(fun (g : gas_packet) -> g.g_ts) gas_packets in
  let stream = Pipelines.gas_alerts ~rssi:s_rssi ~locations:s_loc ~gas:s_gas () in
  let acc = ref [] in
  let rec loop () =
    match stream () with
    | None -> ()
    | Some ev -> acc := ev :: !acc; loop ()
  in loop ();
  List.rev !acc

(** Отфильтровать только Data + Retract (отбросить Watermark). *)
let data_retract_only = List.filter (function
  | Mf_event.Data _ | Mf_event.Retract _ | Mf_event.Update _ -> true
  | Mf_event.Watermark _ -> false)

(* Хелперы для краткости *)
let mk_gas lamp ts ?(co2 = None) ?(co = None) ?(h2 = None) ?(ch4 = None) () =
  { g_lamp = lamp; g_ts = ts;
    g_co2 = co2; g_co = co; g_h2 = h2; g_ch4 = ch4 }

let mk_packet lamp ts readings =
  { lamp; readings; voltage = 4.0; moving = true; sos = false; ts }

let mk_location lamp wend position =
  { loc_lamp = lamp; loc_wend = wend; loc_top2 = []; loc_position = position }

(* Получить из события Gas_alert поля для проверки. *)
let get_alert_info = function
  | Mf_event.Data (Gas_alert a, _) ->
    Some (a.ga_lamp, a.ga_gas, a.ga_level, a.ga_ppm, a.ga_position)
  | _ -> None

let get_retract_info = function
  | Mf_event.Retract (Gas_alert a, _) ->
    Some (a.ga_lamp, a.ga_gas, a.ga_level, a.ga_ppm, a.ga_position)
  | _ -> None

(* Update несёт old+new атомарно: возвращаем (old_info, new_info). *)
let get_update_info = function
  | Mf_event.Update { old = Gas_alert o; new_value = Gas_alert n; _ } ->
    Some ((o.ga_lamp, o.ga_gas, o.ga_level, o.ga_ppm, o.ga_position),
          (n.ga_lamp, n.ga_gas, n.ga_level, n.ga_ppm, n.ga_position))
  | _ -> None

let is_resolved = function
  | Mf_event.Data (Gas_resolved _, _) -> true
  | _ -> false

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Gas alerts pipeline\n";
  Printf.printf "==========================================\n";

  (* ─── 1. Пороги ───────────────────────────────────────────── *)
  Printf.printf "\n-- 1. Thresholds\n";

  (* Безопасное значение CO=10ppm → НИЧЕГО *)
  let events = run_pipeline
    ~gas_packets:[mk_gas "M1" 1000 ~co:(Some 10.) ()] () in
  check "CO=10ppm: no alerts emitted"
    (data_retract_only events = []);

  (* CO=60ppm (warn=50, crit=100) → Warning *)
  let events = run_pipeline
    ~gas_packets:[mk_gas "M1" 1000 ~co:(Some 60.) ()] () in
  let alerts = data_retract_only events in
  check "CO=60ppm: 1 emission"
    (List.length alerts = 1);
  (match get_alert_info (List.hd alerts) with
   | Some ("M1", Gas_CO, Warning, ppm, None) when ppm = 60. ->
     pass "  → Gas_alert(M1, CO, Warning, 60ppm, no position)"
   | _ -> fail "  expected Gas_alert(M1, CO, Warning, 60ppm, None)");

  (* CO=120ppm → Critical *)
  let events = run_pipeline
    ~gas_packets:[mk_gas "M1" 1000 ~co:(Some 120.) ()] () in
  (match data_retract_only events with
   | [ev] when (match get_alert_info ev with
                | Some (_, Gas_CO, Critical, _, _) -> true | _ -> false) ->
     pass "CO=120ppm → Critical"
   | _ -> fail "CO=120ppm: expected single Critical alert");

  (* ─── 2. Sticky (повтор без изменений = тишина) ─────────────── *)
  Printf.printf "\n-- 2. Sticky alerts (no spam on repeat)\n";

  let events = run_pipeline
    ~gas_packets:[
      mk_gas "M1" 1000 ~co:(Some 120.) ();
      mk_gas "M1" 11000 ~co:(Some 121.) ();  (* почти то же *)
      mk_gas "M1" 21000 ~co:(Some 119.) ();  (* почти то же *)
    ] () in
  let n = List.length (data_retract_only events) in
  check (Printf.sprintf "3 gas packets with stable CO: 1 emission (was %d)" n)
    (n = 1);

  (* ─── 3. Level transition Warning → Critical ────────────────── *)
  Printf.printf "\n-- 3. Level transition\n";

  let events = run_pipeline
    ~gas_packets:[
      mk_gas "M1" 1000 ~co:(Some 60.) ();   (* Warning *)
      mk_gas "M1" 11000 ~co:(Some 120.) (); (* Critical *)
    ] () in
  (match data_retract_only events with
   | [d1; u2] ->
     (* Теперь level transition — атомарный Update (Warning→Critical),
        а не пара Retract+Data: downstream видит коррекцию за один шаг. *)
     let ok1 = (match get_alert_info d1 with
                | Some (_, Gas_CO, Warning, _, _) -> true | _ -> false) in
     let ok2 = (match get_update_info u2 with
                | Some ((_, Gas_CO, Warning, _, _), (_, Gas_CO, Critical, _, _)) -> true
                | _ -> false) in
     check "W→C: Data(W), Update(W→C)" (ok1 && ok2)
   | other ->
     Printf.printf "  unexpected events: %d\n" (List.length other);
     fail "W→C: expected 2 events (Data, Update)");

  (* ─── 4. ppm refresh при том же уровне ──────────────────────── *)
  Printf.printf "\n-- 4. ppm refresh on >20%% change\n";

  let events = run_pipeline
    ~gas_packets:[
      mk_gas "M1" 1000 ~co:(Some 60.) ();
      mk_gas "M1" 11000 ~co:(Some 90.) ();   (* +50%, тот же Warning *)
    ] () in
  let n = List.length (data_retract_only events) in
  check (Printf.sprintf "ppm change 60→90: 2 events (Data, Update) (got %d)" n)
    (n = 2);

  (* ─── 5. Gas_resolved при возврате в норму ──────────────────── *)
  Printf.printf "\n-- 5. Gas_resolved\n";

  let events = run_pipeline
    ~gas_packets:[
      mk_gas "M1" 1000 ~co:(Some 60.) ();   (* Warning *)
      mk_gas "M1" 11000 ~co:(Some 10.) ();  (* норма *)
    ] () in
  (match data_retract_only events with
   | [d1; r2; res] ->
     let ok1 = (match get_alert_info d1 with
                | Some (_, Gas_CO, Warning, _, _) -> true | _ -> false) in
     let ok2 = (match get_retract_info r2 with
                | Some (_, Gas_CO, Warning, _, _) -> true | _ -> false) in
     let ok3 = is_resolved res in
     check "alert→safe: Data(W), Retract(W), Gas_resolved" (ok1 && ok2 && ok3)
   | other ->
     Printf.printf "  unexpected: %d events\n" (List.length other);
     fail "expected D, R, Resolved");

  (* ─── 6. Position приходит позже газа (ГЛАВНЫЙ КЕЙС) ────────── *)
  Printf.printf "\n-- 6. Position arrives later → retract+emit with new position\n";

  let events = run_pipeline
    ~gas_packets:[
      mk_gas "M1" 1000 ~co:(Some 120.) ();  (* Critical, нет позиции *)
    ]
    ~rssi_packets:[
      (* Raw RSSI пакет — даёт грубую позицию через single_beacon *)
      mk_packet "M1" 5000 [("B1", -50.)];
    ] () in
  let alerts = data_retract_only events in
  (* Ожидаем: Data(no pos) → Update(no pos → with pos). Позиция
     приехала позже газа — алерт обогащается координатами атомарно,
     без промежуточного «алерт исчез». *)
  (match alerts with
   | [d1; u2] ->
     let pos1 = (match get_alert_info d1 with
                 | Some (_, _, _, _, p) -> p | _ -> None) in
     let (pos_old, pos_new) = (match get_update_info u2 with
       | Some ((_,_,_,_,po), (_,_,_,_,pn)) -> (po, pn)
       | None -> (None, None)) in
     check "first alert has no position" (pos1 = None);
     check "Update.old matches first (no position)" (pos_old = None);
     check "Update.new_value has position from raw RSSI"
       (pos_new <> None);
     (match pos_new with
      | Some (x, y, _) ->
        Printf.printf "    enriched position: (x=%.0f, y=%.0f)\n" x y
      | None -> ())
   | other ->
     Printf.printf "  unexpected: %d events\n" (List.length other);
     fail "expected 2 events (Data-no-pos, Update-with-pos)");

  (* ─── 7. Position update от точной локации ──────────────────── *)
  Printf.printf "\n-- 7. Window location updates position (precise > rough)\n";

  let events = run_pipeline
    ~gas_packets:[
      mk_gas "M1" 1000 ~co:(Some 120.) ();
    ]
    ~rssi_packets:[
      mk_packet "M1" 5000 [("B1", -50.)];   (* грубая *)
    ]
    ~locations:[
      mk_location "M1" 10000 (Some (200.0, 300.0, -160));  (* точная *)
    ] () in
  let alerts = data_retract_only events in
  (* Ожидаем: Data(no pos) → Update(→ rough from B1) → Update(→ precise).
     Каждый refresh — атомарный Update, а не пара Retract+Data, поэтому
     3 события вместо 5. *)
  check (Printf.sprintf "3 events: Data, Update(rough), Update(precise) (got %d)"
           (List.length alerts))
    (List.length alerts = 3);

  (* Последнее событие — Update; его new_value должен нести точную
     позицию из location. *)
  let last_pos = match List.rev alerts with
    | last :: _ ->
      (match get_update_info last with
       | Some (_, (_, _, _, _, p)) -> p
       | None -> (match get_alert_info last with
                  | Some (_, _, _, _, p) -> p | None -> None))
    | [] -> None in
  (match last_pos with
   | Some (200.0, 300.0, -160) ->
     pass "last alert (Update.new_value) has precise position from window"
   | _ ->
     fail "last alert should have precise position from location");

  Printf.printf "\nAll gas_alerts tests passed.\n"
