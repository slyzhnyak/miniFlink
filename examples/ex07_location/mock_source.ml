(** Мок-источник пакетов фонарей шахтёров.

    Заменяемая часть пайплайна — это всё, что превращает «нашу симуляцию
    шахтёрской смены» в поток событий, готовый к обработке. В проде на
    его место станет {!Kafka_source.read} — тот же {!S}, отдающий
    {!Domain.packet}. Топология примера ([ex07_location.ml]) ничего не
    знает о происхождении пакетов.

    Источник РЕАЛИСТИЧНЫЙ по умолчанию: содержит опоздавшие пакеты и
    дубли — то, что at-least-once Kafka и UDP-радио в шахте создают
    неизбежно. Бенчмарки и regression-тесты используют именно такой
    источник: бенчмарк, прогоняющий «идеальный» поток, не отразит цены
    ретракций и dedup, которые в проде составляют существенную часть
    работы — он давал бы ложно-оптимистичные цифры и пропускал бы
    регрессии в коде ретрактов.

    Сценарий разложен на ДВА слоя в API, чтобы можно было исследовать
    вклад каждого аспекта явно:
    - [base_packets ()]: чистый поток (6 шахтёров каждые 15с);
    - [with_late_and_dups]: добавляет 4 опоздавших + 8 дублей.
    {!Default} применяет оба — это то, что видит ex07 и тесты. *)

open Miniflink
open Time
open Domain

(** Сигнатура источника пакетов: блок данных + статистика. *)
module type S = sig
  val read : unit -> packet Mf_event.t Stream.t

  (** Поток газовых пакетов от того же сценария. {b Независимый} от
      {!read} — у газа свой период (~10 секунд), свои сценарии
      превышений (только у части шахтёров), свои опоздавшие/дубли. *)
  val read_gas : unit -> gas_packet Mf_event.t Stream.t

  val stats : unit -> string list
end

(** {2 Внутренности сценария} *)

let noise i k = float_of_int ((i * 7 + k * 13) mod 9) -. 4.0
(** Шум RSSI -4..+4 dB — детерминированный по [i] и [k]. *)

let steps = 25                          (* t = 0, 15, ..., 360с *)
let dt = seconds 15

let m1_voltage i = 4.0 -. 0.8 *. float_of_int i /. float_of_int (steps - 1)
(** Батарея M1 садится: 4.0В → 3.2В. К i=15 (t=225с) V≈3.5В,
    debounce 2 мин ⇒ алерт [Low_voltage] на t≈345с. *)

(** Базовая траектория шахтёра по индексу шага [i]. *)
let packets_for (lamp : string) (i : int) : packet option =
  let t = i * dt in
  let default = { lamp; ts = t; readings = [];
                  voltage = 4.0; moving = true; sos = false } in
  match lamp with
  | "M1" -> Some { default with
      readings = [ "B1", -45. +. noise i 1; "B2", -52. +. noise i 2 ];
      voltage  = m1_voltage i }
  | "M2" -> Some { default with
      readings = [ "B3", -48. +. noise i 6; "B4", -50. +. noise i 7;
                   "B5", -72. +. noise i 9 ];
      moving   = (t < seconds 120) }            (* стоит после t=120с → No_motion *)
  | "M3" ->
    Some { default with
      readings = if i * 15 <= 20
                 then [ "B1", -60. +. noise i 11 ]
                 else [] }                       (* пустые → No_readings *)
  | "M4" -> Some { default with
      readings = [ "B5", -55. +. noise i 12 ];
      sos      = (t = seconds 120) }            (* SOS-импульс *)
  | "M5" ->
    if i * 15 <= 100
    then Some { default with readings = [ "B2", -50. +. noise i 13 ] }
    else None                                   (* фонарь замолк → No_packets *)
  | "M6" ->
    (* Все четыре проблемы одновременно. Пакеты идут до конца, чтобы
       contact-FSM не подавил motion/voltage. *)
    Some { lamp; ts = t;
           readings = if t <= seconds 30
                      then [ "B3", -65. +. noise i 14 ]
                      else [];
           voltage  = 3.3;                       (* всегда ниже порога *)
           moving   = (t < seconds 120);        (* стоит после 120с *)
           sos      = (t = seconds 45) }        (* SOS на t=45с *)
  | _ -> None

let miners = ["M1"; "M2"; "M3"; "M4"; "M5"; "M6"]

(** {2 Публичный API} *)

(** Чистый поток пакетов: каждые 15с от 6 шахтёров. Используется
    бенчмарками (без аномалий канала). *)
let base_packets () : packet list =
  List.init steps (fun i ->
    List.filter_map (fun l -> packets_for l i) miners)
  |> List.concat

(** Добавить «дрожание канала»: 4 опоздавших пакета (опоздание ≥
    размера окна — гарантия retract) + 8 дублей (повторы существующих).
    Эти аномалии симулируют at-least-once Kafka + UDP-радио в шахте. *)
let with_late_and_dups (base : packet list) : packet list =
  let late_packets = [
    (* (вставить_после_ts, пакет) — четыре «дрожащих» опоздавших *)
    seconds 210, { lamp = "M1"; ts = seconds 118;
                   readings = [ "B1", -44.; "B2", -53. ];
                   voltage = m1_voltage 8; moving = true; sos = false };
    seconds 180, { lamp = "M2"; ts = seconds 88;
                   readings = [ "B3", -49.; "B4", -51.; "B5", -73. ];
                   voltage = 4.0; moving = true; sos = false };
    seconds 165, { lamp = "M3"; ts = seconds 73;
                   readings = [ "B1", -62. ]; voltage = 4.0;
                   moving = true; sos = false };
    seconds 120, { lamp = "M6"; ts = seconds 33;
                   readings = [ "B3", -64. ];
                   voltage = 3.3; moving = true; sos = false };
  ] in
  let dup_of lamp i = match packets_for lamp i with
    | Some p -> [p; p]    (* пакет + его дубль *)
    | None -> [] in
  let duplicates =
    dup_of "M1" 5 @ dup_of "M2" 10 @ dup_of "M4" 12 @ dup_of "M6" 6 in
  let with_late =
    List.fold_left (fun acc (insert_after, late) ->
      let before, after = List.partition (fun p -> p.ts <= insert_after) acc in
      before @ [late] @ after) base late_packets in
  with_late @ duplicates

(** {2 Газовые пакеты — базовый сценарий}

    Газ шлётся ОТДЕЛЬНО от RSSI: период ~10с (для базового сценария
    {!gas_steps} шагов), у части шахтёров есть превышения для
    демонстрации алертов и retract'ов при обновлении position. *)

let gas_steps = 37          (* t = 0, 10, 20, ..., 360с (10-секундный шаг) *)
let gas_dt = seconds 10

(** Газовый профиль шахтёра по индексу шага. Возвращает [None] если в
    этом шаге пакет газа не шлётся (например, конкретный шахтёр без
    газового сенсора). Сценарии подобраны чтобы покрыть:
    - Critical → диспетчер видит сразу;
    - Warning → Critical → demo смены уровня (retract+emit);
    - Critical → норма → demo Gas_resolved;
    - постоянное Critical → demo обновления position (retract+emit
      когда у шахтёра уточняется позиция от первого RSSI окна). *)
let gas_for (lamp : string) (i : int) : gas_packet option =
  let t = i * gas_dt in
  let mk ~co2 ~co ~h2 ~ch4 =
    Some { g_lamp = lamp; g_ts = t;
           g_co2 = co2; g_co = co; g_h2 = h2; g_ch4 = ch4 } in
  match lamp with
  | "M1" ->
    (* M1: критический CO быстро (t≈30с) и держится — главный demo retract'а
       по обновлению position. *)
    mk ~co2:(Some 400.) ~co:(Some (if t >= seconds 30 then 130. else 20.))
       ~h2:None ~ch4:None
  | "M2" ->
    (* M2: CH4 warning → critical (демо смены уровня → retract+emit). *)
    let ch4 =
      if t < seconds 60 then 2000.       (* норма *)
      else if t < seconds 180 then 6000. (* warning *)
      else 12000.                          (* critical *)
    in
    mk ~co2:None ~co:None ~h2:None ~ch4:(Some ch4)
  | "M3" ->
    (* M3: critical CO2 → норма (демо Gas_resolved). *)
    let co2 =
      if t < seconds 60 then 18000.      (* critical *)
      else if t < seconds 120 then 7000. (* warning *)
      else 600.                            (* норма *)
    in
    mk ~co2:(Some co2) ~co:None ~h2:None ~ch4:None
  | "M4" ->
    (* M4: всё в норме, газовые пакеты идут для покрытия «без алертов». *)
    mk ~co2:(Some 500.) ~co:(Some 5.) ~h2:None ~ch4:(Some 100.)
  | "M5" ->
    (* M5: H2 critical, постоянно, но шлёт через раз — пропуски. *)
    if i mod 2 = 0 then None
    else mk ~co2:None ~co:None ~h2:(Some 11000.) ~ch4:None
  | "M6" ->
    (* M6: нет газового сенсора (как у некоторых старых моделей). *)
    None
  | _ -> None

let base_gas_packets () : gas_packet list =
  let acc = ref [] in
  for i = 0 to gas_steps - 1 do
    List.iter (fun m -> match gas_for m i with
      | Some g -> acc := g :: !acc
      | None -> ()) miners
  done;
  List.rev !acc

(** {2 Реализация модуля {!S}} *)

(** Реалистичный источник: базовый поток + опоздавшие + дубли. ЭТО
    дефолт, потому что в проде канал ВСЕГДА даёт late-пакеты (UDP-радио)
    и дубли (Kafka at-least-once retry). Бенчмарки и тесты используют
    именно его — иначе мы измеряли бы нерепрезентативный «happy path».

    Если когда-нибудь понадобится изолировать вклад dedup или retracts —
    делать это явной параметризацией с дефолтом «как в проде», а не
    отдельным «чистым» источником: иначе кто-то наступит и опубликует
    приукрашенный бенчмарк. *)
module Default : S = struct
  let packets () = with_late_and_dups (base_packets ())
  let read () =
    Mf_event.of_list ~ts:(fun (p : packet) -> p.ts) (packets ())
  let read_gas () =
    Mf_event.of_list ~ts:(fun (g : gas_packet) -> g.g_ts) (base_gas_packets ())
  let stats () =
    let all = packets () in
    let unique = List.sort_uniq
      (fun a b -> compare (a.lamp, a.ts) (b.lamp, b.ts)) all in
    let gas = base_gas_packets () in
    [
      Printf.sprintf "пакетов всего: %d" (List.length all);
      Printf.sprintf "из них дублей (тот же lamp+ts): %d"
        (List.length all - List.length unique);
      Printf.sprintf "газовых пакетов: %d" (List.length gas);
    ]
end

(** {1 Large_mine — конфигурируемый источник для бенчмарков}

    Полноразмерная симуляция шахты: N горизонтов с M биконами и K
    шахтёрами на каждом. Шахтёр слышит 5 ближайших биконов на СВОЁМ
    горизонте (биконы других горизонтов не слышны). Сценарии алертов
    распределяются по шахтёрам процентами. Late+dup аномалии
    сохраняются (3% дублей, ~3% опоздавших) — бенчмарк должен видеть
    цену ретракций и dedup. *)
module Large_mine = struct

  type config = {
    horizons             : int;    (** число горизонтов шахты *)
    beacons_per_horizon  : int;
    miners_per_horizon   : int;
    simulation_minutes   : int;    (** длительность симуляции (event-time) *)
    step_seconds         : int;    (** период пакета фонаря (~15с) *)
    gas_period_seconds   : int;    (** период газового пакета (~10с) *)
    (* проценты шахтёров с разными алертами; пересечения возможны *)
    pct_no_packets       : float;
    pct_no_readings      : float;
    pct_no_motion        : float;
    pct_low_voltage      : float;
    pct_sos              : float;
    pct_gas_alerts       : float;  (** доля шахтёров с превышением газа *)
    (* частота аномалий канала *)
    pct_duplicates       : float;
    pct_late             : float;
  }

  let default_config = {
    horizons             = 16;
    beacons_per_horizon  = 64;
    miners_per_horizon   = 256;
    simulation_minutes   = 60;
    step_seconds         = 15;
    gas_period_seconds   = 10;
    pct_no_packets       = 0.10;
    pct_no_readings      = 0.05;
    pct_no_motion        = 0.20;
    pct_low_voltage      = 0.30;
    pct_sos              = 0.01;
    pct_gas_alerts       = 0.05;  (* 5% шахтёров с газовыми событиями *)
    pct_duplicates       = 0.03;
    pct_late             = 0.03;
  }

  let beacon_id ~horizon ~idx = Printf.sprintf "B%d-%d" horizon idx
  let miner_id  ~horizon ~idx = Printf.sprintf "M%d-%d" horizon idx
  let horizon_depth_m h = -160 - h * 20

  let beacon_position ~beacons_per_horizon ~idx =
    let side = int_of_float (ceil (sqrt (float beacons_per_horizon))) in
    let x = (idx mod side) * 50 in
    let y = (idx / side) * 50 in
    float_of_int x, float_of_int y

  let miner_position ~miners_per_horizon ~idx =
    let side = int_of_float (ceil (sqrt (float miners_per_horizon))) in
    let x = (idx mod side) * 25 in
    let y = (idx / side) * 25 in
    float_of_int x, float_of_int y

  let beacons_table (cfg : config) : (string, float * float * int) Hashtbl.t =
    let tbl = Hashtbl.create (cfg.horizons * cfg.beacons_per_horizon) in
    for h = 0 to cfg.horizons - 1 do
      for b = 0 to cfg.beacons_per_horizon - 1 do
        let x, y = beacon_position ~beacons_per_horizon:cfg.beacons_per_horizon ~idx:b in
        Hashtbl.add tbl (beacon_id ~horizon:h ~idx:b)
          (x, y, horizon_depth_m h)
      done
    done;
    tbl

  let all_miners (cfg : config) : string list =
    let acc = ref [] in
    for h = 0 to cfg.horizons - 1 do
      for m = 0 to cfg.miners_per_horizon - 1 do
        acc := miner_id ~horizon:h ~idx:m :: !acc
      done
    done;
    List.rev !acc

  let neighbors_5 ~cfg ~horizon ~miner_idx : (string * float) list =
    let mx, my = miner_position ~miners_per_horizon:cfg.miners_per_horizon
                   ~idx:miner_idx in
    let dists =
      List.init cfg.beacons_per_horizon (fun b ->
        let bx, by = beacon_position ~beacons_per_horizon:cfg.beacons_per_horizon ~idx:b in
        let dx = mx -. bx and dy = my -. by in
        let d  = sqrt (dx *. dx +. dy *. dy) in
        (beacon_id ~horizon ~idx:b, d)) in
    List.sort (fun (_, a) (_, b) -> compare a b) dists
    |> List.filteri (fun i _ -> i < 5)

  let rssi_at_distance d =
    -. 40.0 -. 20.0 *. log10 (max 1.0 d)

  type scenario = {
    drops_packets  : bool;
    drops_readings : bool;
    stops_moving   : bool;
    low_battery    : bool;
    presses_sos    : bool;
    has_gas_alert  : bool;
  }

  let scenario_for ~cfg ~horizon ~miner_idx : scenario =
    let h = Hashtbl.hash (horizon, miner_idx) in
    let bucket n = float_of_int (n mod 10000) /. 10000.0 in
    {
      drops_packets  = bucket h                < cfg.pct_no_packets;
      drops_readings = bucket (h / 11)         < cfg.pct_no_readings;
      stops_moving   = bucket (h / 131)        < cfg.pct_no_motion;
      low_battery    = bucket (h / 1009)       < cfg.pct_low_voltage;
      presses_sos    = bucket (h / 10007)      < cfg.pct_sos;
      has_gas_alert  = bucket (h / 100003)     < cfg.pct_gas_alerts;
    }

  let packets_for_miner ~cfg ~horizon ~miner_idx =
    let nbrs = neighbors_5 ~cfg ~horizon ~miner_idx in
    let sc = scenario_for ~cfg ~horizon ~miner_idx in
    let lamp = miner_id ~horizon ~idx:miner_idx in
    let n_steps = cfg.simulation_minutes * 60 / cfg.step_seconds in
    let drop_after = n_steps / 3 in
    let stop_after = n_steps / 2 in
    let sos_at     = n_steps / 4 in
    let acc = ref [] in
    for i = 0 to n_steps - 1 do
      if sc.drops_packets && i > drop_after then ()
      else begin
        let t = i * cfg.step_seconds * 1000 in
        let readings =
          if sc.drops_readings && i > drop_after / 2 then []
          else List.map (fun (b, d) -> (b, rssi_at_distance d)) nbrs in
        let voltage =
          if sc.low_battery then 3.3 -. 0.05 *. float_of_int (i mod 3)
          else 4.0 in
        let moving = not (sc.stops_moving && i > stop_after) in
        let sos = sc.presses_sos && i = sos_at in
        acc := { lamp; readings; voltage; moving; sos; ts = t } :: !acc
      end
    done;
    List.rev !acc

  let base_packets (cfg : config) : packet list =
    (* Накапливаем хвост-рекурсивно через List.rev_append; обычный
       List.concat на 4096 списках × ~240 элементов взрывает stack. *)
    let all = ref [] in
    for h = 0 to cfg.horizons - 1 do
      for m = 0 to cfg.miners_per_horizon - 1 do
        let pkts = packets_for_miner ~cfg ~horizon:h ~miner_idx:m in
        all := List.rev_append pkts !all
      done
    done;
    (* стандартный List.sort работает на 1М (мерж-сорт, дёргает стек
       логарифмически) — оставляем *)
    List.sort (fun a b -> compare a.ts b.ts) !all

  let with_channel_jitter (cfg : config) (base : packet list) : packet list =
    let n_total = List.length base in
    let n_dups = int_of_float (float_of_int n_total *. cfg.pct_duplicates) in
    let n_late = int_of_float (float_of_int n_total *. cfg.pct_late) in
    let arr = Array.of_list base in
    let len = Array.length arr in
    let dup_step = max 1 (n_total / max 1 n_dups) in
    let dup_list = ref [] in
    Array.iteri (fun i p ->
      if i mod dup_step = 0 && List.length !dup_list < n_dups then
        dup_list := p :: !dup_list) arr;
    let late_step = max 1 (n_total / max 1 n_late) in
    let late_swaps = ref 0 in
    let i = ref 0 in
    while !i < len - 100 && !late_swaps < n_late do
      if !i mod late_step = 0 then begin
        let j = !i + 90 + (!i mod 20) in
        if j < len then begin
          let tmp = arr.(!i) in
          arr.(!i) <- arr.(j);
          arr.(j) <- tmp;
          incr late_swaps
        end
      end;
      incr i
    done;
    (* List.rev_append вместо @ — последний tail-rec, безопасен на 1М *)
    List.rev_append (List.rev (Array.to_list arr)) !dup_list

  (** Газовый профиль шахтёра. Только для тех у кого has_gas_alert=true;
      остальные не шлют газ (имитация шахтёров без сенсора). Для тех у
      кого есть — половина симуляции в норме, потом фиксированное
      превышение по CO (самый частый кейс в шахте). Это даёт явные
      пары retract'ов на каждом таком шахтёре, не флудя весь поток. *)
  let gas_packets_for_miner ~cfg ~horizon ~miner_idx =
    let sc = scenario_for ~cfg ~horizon ~miner_idx in
    if not sc.has_gas_alert then []
    else
      let lamp = miner_id ~horizon ~idx:miner_idx in
      let n_steps = cfg.simulation_minutes * 60 / cfg.gas_period_seconds in
      let alert_after = n_steps / 2 in
      let acc = ref [] in
      for i = 0 to n_steps - 1 do
        let t = i * cfg.gas_period_seconds * 1000 in
        let co_ppm = if i >= alert_after then 120. else 8. in
        acc := { g_lamp = lamp; g_ts = t;
                 g_co2 = Some 500.; g_co = Some co_ppm;
                 g_h2 = None; g_ch4 = None } :: !acc
      done;
      List.rev !acc

  let base_gas_packets (cfg : config) : gas_packet list =
    let all = ref [] in
    for h = 0 to cfg.horizons - 1 do
      for m = 0 to cfg.miners_per_horizon - 1 do
        let pkts = gas_packets_for_miner ~cfg ~horizon:h ~miner_idx:m in
        all := List.rev_append pkts !all
      done
    done;
    List.sort (fun a b -> compare a.g_ts b.g_ts) !all

  module Make (Cfg : sig val config : config end) : S = struct
    let cached_packets = lazy (with_channel_jitter Cfg.config
                                 (base_packets Cfg.config))
    let cached_gas = lazy (base_gas_packets Cfg.config)
    let read () =
      Mf_event.of_list ~ts:(fun (p : packet) -> p.ts)
        (Lazy.force cached_packets)
    let read_gas () =
      Mf_event.of_list ~ts:(fun (g : gas_packet) -> g.g_ts)
        (Lazy.force cached_gas)
    let stats () =
      let all = Lazy.force cached_packets in
      let unique = List.sort_uniq
        (fun a b -> compare (a.lamp, a.ts) (b.lamp, b.ts)) all in
      let gas = Lazy.force cached_gas in
      [
        Printf.sprintf "горизонтов: %d, биконов/гор: %d, шахтёров/гор: %d"
          Cfg.config.horizons Cfg.config.beacons_per_horizon
          Cfg.config.miners_per_horizon;
        Printf.sprintf "симуляция: %d минут, шаг %dс RSSI / %dс газа"
          Cfg.config.simulation_minutes Cfg.config.step_seconds
          Cfg.config.gas_period_seconds;
        Printf.sprintf "RSSI пакетов всего: %d" (List.length all);
        Printf.sprintf "из них дублей (тот же lamp+ts): %d"
          (List.length all - List.length unique);
        Printf.sprintf "газовых пакетов: %d (от %.0f%% шахтёров с превышениями)"
          (List.length gas) (Cfg.config.pct_gas_alerts *. 100.);
      ]
  end
end
