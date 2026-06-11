(* ============================================================
   Пример 7 — локация шахтёра по RSSI маяков связи.

   Каждые 15 секунд фонарь шахтёра шлёт пакет: ID фонаря + до пяти
   самых слышимых маяков с их RSSI. Маяки слышны только в пределах
   своего горизонта (один уровень шахты). Сервис делает две вещи:

   ЛОКАЦИЯ (скользящее окно минута / шаг 5 секунд):
     1. flat_map: пакет → показания (шахтёр, маяк, RSSI);
     2. window_agg_keyed по «шахтёр|маяк» → median RSSI по каждой паре
        в окне минуты (median гасит шум радиоканала);
        allowed_lateness 30с — опоздавший пакет вызывает РЕТРАКТ и
        пересчёт уже закрытых окон;
     3. для КАЖДОГО окна каждого шахтёра — два маяка с сильнейшим
        median RSSI (вход для трилатерации);
     4. обогащение координатами и горизонтом из справочника узлов.

     Особенность скользящего окна 60s/5s: одно событие попадает в 12
     окон, поэтому локация обновляется каждые 5 секунд, хотя пакеты
     приходят раз в 15.

   ДВА АЛЕРТА (process_keyed + event-time таймеры):
     A. «нет пакетов от фонаря >2 мин»  — heartbeat по факту приёма
        пакета (сбрасывается ЛЮБЫМ пакетом, включая пустой);
     B. «не слышит ни одного маяка >5 мин» — heartbeat по показаниям
        (сбрасывается только пакетом с показаниями).

   Граничные случаи в данных:
     - M3 слышит маяки только до t=20с, дальше шлёт ПУСТЫЕ пакеты:
       алерт B сработает (нет показаний 5 мин), алерт A — нет;
     - M4 слышит только ОДИН маяк → честное предупреждение, для
       трилатерации этого мало;
     - M5 шлёт пакеты до t=100с, потом ЗАМОЛКАЕТ: сработает алерт A
       (нет пакетов >2 мин);
     - один пакет M1 приходит С ОПОЗДАНИЕМ ~25с → ретракции
       пересчитывают закрытые окна (счётчик в выводе).

   Замечание о ключе: пара шахтёр×маяк — склейка "lamp|beacon";
   известная ловушка коллизии (см. роадмап «Композитные ключи»)
   здесь безопасна: ID без разделителя.
   ============================================================ *)

open Miniflink
open Time

(* ── Модель ───────────────────────────────────────────────── *)

type packet = {
  lamp     : string;                  (* ID фонаря шахтёра *)
  readings : (string * float) list;   (* до 5: (ID маяка, RSSI dBm) *)
  ts       : Time.t;
}

type reading = { r_lamp : string; r_beacon : string; r_rssi : float; r_ts : Time.t }

(* справочник маяков: ID → (x, y, горизонт) *)
let beacons = [
  "B1", (120.0,  40.0, -160);
  "B2", (180.0,  40.0, -160);
  "B3", (240.0,  95.0, -240);
  "B4", (300.0,  95.0, -240);
  "B5", (360.0, 150.0, -240);
] |> List.to_seq |> Hashtbl.of_seq

let beacon_horizon b =
  match Hashtbl.find_opt beacons b with Some (_,_,h) -> Some h | None -> None

let no_readings_threshold = minutes 2   (* нет пакетов вообще *)
let silence_threshold     = minutes 5   (* пакеты есть, маяков не слышит *)

(* ── Входные пакеты (~6 минут, шаг 15с) ────────────────────── *)
(* M1 (горизонт -160м): слышит свой уровень B1/B2.
   M2 (-240м): слышит B3/B4/B5 (свой уровень).
   M3 (-160м): слышит B1 до t=20с, дальше пустые пакеты (фонарь жив,
               эфир пуст — алерт B сработает).
   M4 (-240м): всегда слышит только B5 (один маяк).
   M5 (-160м): шлёт пакеты до t=100с, потом замолкает (алерт A). *)
let noise i k = float_of_int ((i * 7 + k * 13) mod 9) -. 4.0   (* −4..+4 dB *)

let steps = 25                              (* t = 0, 15, ..., 360с *)
let dt    = seconds 15

let packets_for lamp i =
  let t = i * dt in
  match lamp with
  | "M1" -> Some { lamp; ts = t; readings = [
      "B1", -45. +. noise i 1;
      "B2", -52. +. noise i 2 ] }
  | "M2" -> Some { lamp; ts = t; readings = [
      "B3", -48. +. noise i 6;
      "B4", -50. +. noise i 7;
      "B5", -72. +. noise i 9 ] }
  | "M3" ->
    if i * 15 <= 20
    then Some { lamp; ts = t; readings = [ "B1", -60. +. noise i 11 ] }
    else Some { lamp; ts = t; readings = [] }            (* пустые пакеты *)
  | "M4" -> Some { lamp; ts = t; readings = [
      "B5", -55. +. noise i 12 ] }
  | "M5" ->
    if i * 15 <= 100
    then Some { lamp; ts = t; readings = [ "B2", -50. +. noise i 13 ] }
    else None                                            (* фонарь замолк *)
  | _ -> None

let packets =
  let base =
    List.init steps (fun i ->
      List.filter_map (fun l -> packets_for l i) ["M1"; "M2"; "M3"; "M4"; "M5"])
    |> List.concat in
  (* опоздавший пакет M1 c ts=180с, вставленный после ts≈225с *)
  let late = { lamp = "M1"; ts = seconds 180; readings = [
      "B1", -44.;
      "B2", -53. ] } in
  let before, after = List.partition (fun p -> p.ts <= seconds 225) base in
  before @ [late] @ after

(* ── median RSSI по шахтёр×маяк, скользящее окно 60s/5s ───── *)

let median_rssi source =
  source
  |> Pipe.flat_map (fun p ->
       List.map (fun (b, rssi) ->
         { r_lamp = p.lamp; r_beacon = b; r_rssi = rssi; r_ts = p.ts })
         p.readings)
  |> Pipe.event_time ~lateness:(seconds 1)
  |> Pipe.window_agg_keyed ~by:(fun r -> r.r_lamp ^ "|" ^ r.r_beacon)
       ~allowed_lateness:(seconds 30)
       (Pipe.sliding (seconds 60) (seconds 5))
       (Agg.median (fun r -> r.r_rssi))

(* ── Два алерта на process_keyed ──────────────────────────── *)

module ByLamp = Keyed.Make (struct type t = packet let key p = p.lamp end)

type alert = No_packets of string * Time.t option   (* нет пакетов >2 мин *)
           | No_readings of string * Time.t option  (* пакеты есть, маяков нет *)

let connectivity_alerts source =
  source
  |> Pipe.event_time ~lateness:(seconds 1)
  |> Pipe.process_keyed (module ByLamp)
       ~init:(fun () -> (ref None, ref None, ref None, ref `Ok))
         (* (last_packet_ts, last_reading_ts, current_timer_ts, last_alert) *)
       ~on_event:(fun ctx _key (last_pkt, last_rd, cur_timer, last_alert) p ->
         last_pkt := Some p.ts;
         if p.readings <> [] then begin
           last_rd := Some p.ts;
           last_alert := `Ok       (* показания вернулись — сбросить алерт *)
         end;
         let t_pkt = p.ts + no_readings_threshold in
         let t_rd  = (match !last_rd with Some t -> t | None -> p.ts) + silence_threshold in
         let next  = min t_pkt t_rd in
         (match !cur_timer with
          | Some t when t <> next -> ctx.Pipe.cancel_event_timer t
          | _ -> ());
         cur_timer := Some next;
         ctx.Pipe.set_event_timer next)
       ~on_timer:(fun ctx key (last_pkt, last_rd, cur_timer, last_alert) t _kind ->
         cur_timer := None;
         let exhausted_pkt = match !last_pkt with
           | Some lp -> t >= lp + no_readings_threshold | None -> true in
         let exhausted_rd = match !last_rd with
           | Some lr -> t >= lr + silence_threshold | None -> true in
         let new_alert : [ `Ok | `No_packets | `No_readings ] =
           if exhausted_pkt then `No_packets
           else if exhausted_rd then `No_readings
           else `Ok in
         if new_alert <> !last_alert then begin
           last_alert := new_alert;
           (match new_alert with
            | `No_packets -> ctx.Pipe.emit (No_packets (key, !last_pkt))
            | `No_readings -> ctx.Pipe.emit (No_readings (key, !last_rd))
            | `Ok -> ())
         end;
         (* перепланировать следующий горизонт, иначе таймер больше не
            проверится — мы потеряем эскалацию «нет показаний → нет пакетов» *)
         let next_rd  = (match !last_rd with Some t -> t | None -> 0) + silence_threshold in
         let next_pkt = (match !last_pkt with Some t -> t | None -> 0) + no_readings_threshold in
         let next = if t < next_rd then min next_rd next_pkt
                    else next_pkt in
         if next > t then begin
           cur_timer := Some next;
           ctx.Pipe.set_event_timer next
         end)

(* ── Сборка сервиса ───────────────────────────────────────── *)

let () =
  Printf.printf "=== Локация шахтёров по маякам ===\n\n";

  (* алерты связи *)
  Printf.printf "Контроль связи:\n";
  let alerts =
    Mf_event.of_list ~ts:(fun p -> p.ts) packets
    |> connectivity_alerts
    |> Pipe.collect in
  (match alerts with
   | [] -> Printf.printf "  все фонари в эфире\n"
   | _ -> List.iter (function
       | No_packets (lamp, Some t) ->
         Printf.printf "  ⚠ %s: НЕТ ПАКЕТОВ >%d мин (последний t=%dс)\n"
           lamp (no_readings_threshold/60000) (t/1000)
       | No_packets (lamp, None) ->
         Printf.printf "  ⚠ %s: пакетов не было ни разу\n" lamp
       | No_readings (lamp, Some t) ->
         Printf.printf "  ⚠ %s: пакеты идут, но не слышит маяки >%d мин (последние показания t=%dс)\n"
           lamp (silence_threshold/60000) (t/1000)
       | No_readings (lamp, None) ->
         Printf.printf "  ⚠ %s: не слышал маяки ни разу\n" lamp) alerts);

  (* окна median + подсчёт ретракций *)
  let win_events =
    Mf_event.of_list ~ts:(fun p -> p.ts) packets
    |> median_rssi
    |> Stream.to_list in
  let retracts = List.length (List.filter
    (function Mf_event.Retract _ -> true | _ -> false) win_events) in
  if retracts > 0 then
    Printf.printf "\nОпоздавшие пакеты: ретракций (пересчётов окон): %d\n" retracts;

  (* финальные значения окон (после применения ретрактов) *)
  let medians =
    Pipe.materialize ~by:(fun (k, _) wend -> (k, wend))
      (Stream.of_list win_events) in

  (* группировка: (шахтёр, конец_окна) → [(маяк, median)] *)
  let by_lamp_window : (string * Time.t, (string * float) list) Hashtbl.t =
    Hashtbl.create 64 in
  List.iter (fun ((key, wend), (_, med)) ->
    match med, String.split_on_char '|' key with
    | Some m, [lamp; beacon] ->
      let cur = try Hashtbl.find by_lamp_window (lamp, wend) with Not_found -> [] in
      Hashtbl.replace by_lamp_window (lamp, wend) ((beacon, m) :: cur)
    | _ -> ()) medians;

  (* top-2 по median В КАЖДОМ окне *)
  let top2_per_window =
    Hashtbl.fold (fun (lamp, wend) bs acc ->
      let top2 =
        List.sort (fun (_, a) (_, b) -> compare b a) bs
        |> List.filteri (fun i _ -> i < 2) in
      (lamp, wend, top2, List.length bs) :: acc) by_lamp_window []
    |> List.sort compare in

  (* показ: для краткости — последние 3 окна каждого шахтёра *)
  Printf.printf "\nЛокация (top-2 маяков с сильнейшим median RSSI, последние 3 окна каждого фонаря):\n\n";
  let by_lamp = Hashtbl.create 8 in
  List.iter (fun (lamp, wend, top2, n) ->
    let xs = try Hashtbl.find by_lamp lamp with Not_found -> [] in
    Hashtbl.replace by_lamp lamp ((wend, top2, n) :: xs)) top2_per_window;

  ["M1"; "M2"; "M3"; "M4"; "M5"] |> List.iter (fun lamp ->
    match Hashtbl.find_opt by_lamp lamp with
    | None | Some [] ->
      Printf.printf "%s: нет окон с показаниями\n\n" lamp
    | Some wins ->
      let last3 = List.sort (fun (a,_,_) (b,_,_) -> compare b a) wins
                  |> List.filteri (fun i _ -> i < 3)
                  |> List.rev in
      Printf.printf "%s:\n" lamp;
      List.iter (fun (wend, top2, n_heard) ->
        Printf.printf "  окно→%3dс: " (wend / 1000);
        (match top2 with
         | [] -> Printf.printf "—\n"
         | xs ->
           List.iteri (fun i (beacon, med) ->
             match Hashtbl.find_opt beacons beacon with
             | Some (x, y, h) ->
               Printf.printf "%s%s %.1f dBm (x=%.0f y=%.0f h=%dм)"
                 (if i > 0 then "; " else "") beacon med x y h
             | None -> Printf.printf "%s%s %.1f dBm"
                 (if i > 0 then "; " else "") beacon med) xs;
           if n_heard < 2 then
             Printf.printf "  ⚠ слышен 1 маяк — для трилатерации мало";
           print_newline ())) last3;
      print_newline ())
