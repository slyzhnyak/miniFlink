(* ============================================================
   Пример 7 — локация шахтёра по RSSI маяков (ядро minePASS).

   Каждые 5 секунд фонарь шахтёра шлёт пакет: ID фонаря + до пяти самых
   слышимых маяков с их RSSI. Сервис делает две вещи:

   ЛОКАЦИЯ:
     1. разворачивает пакеты в показания (шахтёр, маяк, RSSI);
     2. median RSSI по паре шахтёр×маяк в СКОЛЬЗЯЩЕМ окне за минуту
        (median гасит выбросы радиоканала; sliding 60s/5s обновляет
        картину каждые 5 секунд); allowed_lateness 30с — опоздавший
        пакет вызывает РЕТРАКТ и пересчёт уже закрытых окон;
     3. на шахтёра — два маяка с сильнейшим median + координаты узлов.

   СВЯЗЬ (process_keyed + event-time таймер):
     алерт, если шахтёр НЕ СЛЫШИТ узлы дольше 5 минут. Таймер
     сбрасывается только пакетами С ПОКАЗАНИЯМИ: пустой пакет означает
     «фонарь жив, но маяков не слышно» — алерт всё равно нужен.

   Граничные случаи в данных:
     - M3 слышит узлы только до t=20с, дальше шлёт ПУСТЫЕ пакеты
       → алерт о пропадании связи (20с + 5мин < финальный watermark);
     - M4 всё время слышит ОДИН узел → локация честно предупреждает,
       что для трилатерации этого мало;
     - один пакет M1 (t=200с) приходит С ОПОЗДАНИЕМ ~25с → ретракции
       пересчитывают закрытые окна (счётчик в выводе).

   Замечание о ключе: пара шахтёр×маяк — склейка "lamp|beacon";
   ловушка коллизии склейки (см. роадмап «Композитные ключи») здесь
   безопасна: ID без разделителя.
   ============================================================ *)

open Miniflink
open Time

(* ── Модель ───────────────────────────────────────────────── *)

type packet = {
  lamp     : string;                     (* ID фонаря шахтёра *)
  readings : (string * float) list;      (* до 5: (ID маяка, RSSI dBm) *)
  ts       : Time.t;
}

type reading = { r_lamp : string; r_beacon : string; r_rssi : float; r_ts : Time.t }

(* справочник координат маяков (узлов): ID → (x, y, горизонт) *)
let beacons = [
  "B1", (120.0,  40.0, -160);
  "B2", (180.0,  40.0, -160);
  "B3", (240.0,  95.0, -240);
  "B4", (300.0,  95.0, -240);
  "B5", (360.0, 150.0, -240);
] |> List.to_seq |> Hashtbl.of_seq

let silence_threshold = minutes 5    (* не слышит узлы дольше → алерт *)

(* ── Входные пакеты (симуляция ~6 минут, шаг 5с) ──────────── *)
let noise i k = float_of_int ((i * 7 + k * 13) mod 9) -. 4.0   (* −4..+4 dB *)

let steps = 73                                  (* t = 0, 5, ..., 360с *)

let packets_for lamp i =
  let t = seconds (i * 5) in
  match lamp with
  | "M1" -> Some { lamp; ts = t; readings = [
      "B1", -45. +. noise i 1;  "B2", -52. +. noise i 2;
      "B3", -78. +. noise i 3;  "B4", -85. +. noise i 4;
      "B5", -90. +. noise i 5 ] }
  | "M2" -> Some { lamp; ts = t; readings = [
      "B3", -48. +. noise i 6;  "B4", -50. +. noise i 7;
      "B2", -75. +. noise i 8;  "B5", -72. +. noise i 9;
      "B1", -88. +. noise i 10 ] }
  | "M3" ->   (* слышит узлы до 20с; дальше фонарь жив, но эфир пуст *)
    if i * 5 <= 20
    then Some { lamp; ts = t; readings = [ "B5", -60. +. noise i 11 ] }
    else Some { lamp; ts = t; readings = [] }            (* пустой пакет *)
  | "M4" ->   (* всегда слышит только один узел *)
    Some { lamp; ts = t; readings = [ "B5", -55. +. noise i 12 ] }
  | _ -> None

let packets =
  let base =
    List.init steps (fun i ->
      List.filter_map (fun l -> packets_for l i) ["M1"; "M2"; "M3"; "M4"])
    |> List.concat in
  (* опоздавший пакет M1 с ts=200с: вставляем ПОСЛЕ пакетов с ts≈225с —
     к его прибытию watermark уже ~224с, окна с концом ≤224с закрыты,
     allowed_lateness 30с заставит их ретрактнуть и пересчитать *)
  let late = { lamp = "M1"; ts = seconds 200; readings = [
      "B1", -44.; "B2", -53.; "B3", -77.; "B4", -86.; "B5", -91. ] } in
  let before, after = List.partition (fun p -> p.ts <= seconds 225) base in
  before @ [late] @ after

(* ── Пайплайн 1: median RSSI по шахтёр×маяк ───────────────── *)

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

(* ── Пайплайн 2: контроль связи (heartbeat по узлам) ──────── *)

module ByLamp = Keyed.Make (struct type t = packet let key p = p.lamp end)

let silence_alerts source =
  source
  |> Pipe.event_time ~lateness:(seconds 1)   (* таймеры срабатывают по watermark *)
  |> Pipe.process_keyed (module ByLamp)
       ~init:(fun () -> ref None)                  (* last heard (ts) *)
       ~on_event:(fun ctx _key last p ->
         if p.readings <> [] then begin            (* пустой пакет НЕ сбрасывает *)
           last := Some p.ts;
           ctx.Pipe.cancel_event_timers ();
           ctx.Pipe.set_event_timer (p.ts + silence_threshold)
         end)
       ~on_timer:(fun ctx key last _t _kind ->
         ctx.Pipe.emit (key, !last))

(* ── Сборка сервиса ───────────────────────────────────────── *)

let () =
  Printf.printf "=== Локация шахтёров по маякам ===\n\n";

  (* связь: кто не слышит узлы дольше порога *)
  Printf.printf "Контроль связи (порог %d мин):\n" (silence_threshold / 60000);
  let alerts =
    Mf_event.of_list ~ts:(fun p -> p.ts) packets
    |> silence_alerts
    |> Pipe.collect in
  (match alerts with
   | [] -> Printf.printf "  все шахтёры слышат узлы\n"
   | xs -> List.iter (fun (lamp, last) ->
       match last with
       | Some t -> Printf.printf
           "  ⚠ %s НЕ СЛЫШИТ УЗЛЫ — последний контакт t=%dс (связь потеряна)\n"
           lamp (t / 1000)
       | None -> Printf.printf "  ⚠ %s не слышал узлы ни разу\n" lamp) xs);

  (* локация: median по окнам, с подсчётом ретракций от опоздавших *)
  let events =
    Mf_event.of_list ~ts:(fun p -> p.ts) packets
    |> median_rssi
    |> Stream.to_list in
  let retracts =
    List.length (List.filter (function Mf_event.Retract _ -> true | _ -> false) events) in
  if retracts > 0 then
    Printf.printf "\nОпоздавшие пакеты: пересчитано окон (ретракций): %d\n" retracts;

  let medians =
    Pipe.materialize ~by:(fun (k, _) wend -> (k, wend))
      (Stream.of_list events) in

  let latest_wend =
    List.fold_left (fun m ((_, w), _) -> max m w) min_int medians in
  let by_lamp = Hashtbl.create 4 in
  List.iter (fun ((key, wend), (_, med)) ->
    if wend = latest_wend then
      match med, String.split_on_char '|' key with
      | Some m, [lamp; beacon] ->
        Hashtbl.replace by_lamp lamp
          ((beacon, m) :: (try Hashtbl.find by_lamp lamp with Not_found -> []))
      | _ -> ()) medians;

  Printf.printf "\nЛокация (median RSSI за минуту, два сильнейших узла):\n\n";
  ["M1"; "M2"; "M3"; "M4"] |> List.iter (fun lamp ->
    match Hashtbl.find_opt by_lamp lamp with
    | None ->
      Printf.printf "Шахтёр %s: нет свежих показаний — локация неизвестна\n\n" lamp
    | Some bs ->
      let top2 =
        List.sort (fun (_, a) (_, b) -> compare b a) bs
        |> List.filteri (fun i _ -> i < 2) in
      Printf.printf "Шахтёр %s:\n" lamp;
      List.iter (fun (beacon, med) ->
        match Hashtbl.find_opt beacons beacon with
        | Some (x, y, h) ->
          Printf.printf "  %s  %.1f dBm   → x=%.0f y=%.0f горизонт %dм\n"
            beacon med x y h
        | None ->
          Printf.printf "  %s  %.1f dBm   → координаты неизвестны\n" beacon med)
        top2;
      if List.length top2 < 2 then
        Printf.printf "  ⚠ слышен только один узел — для трилатерации недостаточно\n";
      print_newline ())
