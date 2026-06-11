(* ============================================================
   Пример 7 — локация шахтёра по RSSI маяков (ядро minePASS).

   Каждые 5 секунд фонарь шахтёра шлёт пакет: ID фонаря + пять самых
   слышимых маяков с их RSSI. Сервис:
     1. разворачивает пакет в показания (шахтёр, маяк, RSSI);
     2. считает median RSSI по каждой паре шахтёр×маяк в СКОЛЬЗЯЩЕМ
        окне за последнюю минуту (median гасит выбросы радиоканала,
        sliding 60s/5s обновляет картину каждые 5 секунд);
     3. для каждого шахтёра берёт два маяка с сильнейшим median RSSI;
     4. обогащает их координатами из справочника маяков.

   Выход — два ближайших узла с координатами на каждого шахтёра:
   готовая основа для трилатерации/привязки к горизонту.

   Замечание о ключе: пара шахтёр×маяк — композитный ключ через
   склейку "lamp|beacon". У склейки есть известная ловушка коллизии,
   если ID содержат разделитель (см. роадмап «Композитные ключи») —
   здесь ID формата M1/B2 безопасны.
   ============================================================ *)

open Miniflink
open Time

(* ── Модель ───────────────────────────────────────────────── *)

type packet = {
  lamp     : string;                     (* ID фонаря шахтёра *)
  readings : (string * float) list;      (* топ-5: (ID маяка, RSSI dBm) *)
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

(* ── Входные пакеты (симуляция ~70 секунд) ────────────────── *)
(* M1 стоит между B1 и B2 (они сильнейшие), слышит B3/B4/B5 слабо.
   M2 — между B3 и B4. RSSI шумит — median это сгладит. *)
let noise i k = float_of_int ((i * 7 + k * 13) mod 9) -. 4.0   (* −4..+4 dB *)

let packets =
  List.init 14 (fun i ->                       (* каждые 5с: t = 0,5,...,65 *)
    let t = seconds (i * 5) in
    [ { lamp = "M1"; ts = t; readings = [
          "B1", -45. +. noise i 1;  "B2", -52. +. noise i 2;
          "B3", -78. +. noise i 3;  "B4", -85. +. noise i 4;
          "B5", -90. +. noise i 5 ] };
      { lamp = "M2"; ts = t; readings = [
          "B3", -48. +. noise i 6;  "B4", -50. +. noise i 7;
          "B2", -75. +. noise i 8;  "B5", -72. +. noise i 9;
          "B1", -88. +. noise i 10 ] } ])
  |> List.concat

(* ── Пайплайн ─────────────────────────────────────────────── *)

let median_rssi source =
  source
  (* пакет → пять показаний (шахтёр, маяк, RSSI) *)
  |> Pipe.flat_map (fun p ->
       List.map (fun (b, rssi) ->
         { r_lamp = p.lamp; r_beacon = b; r_rssi = rssi; r_ts = p.ts })
         p.readings)
  |> Pipe.event_time ~lateness:(seconds 1)
  (* median RSSI по паре шахтёр×маяк, скользящее окно минута/5с *)
  |> Pipe.window_agg_keyed ~by:(fun r -> r.r_lamp ^ "|" ^ r.r_beacon)
       (Pipe.sliding (seconds 60) (seconds 5))
       (Agg.median (fun r -> r.r_rssi))

let () =
  Printf.printf "=== Локация шахтёров по маякам ===\n\n";

  (* финальное состояние окон: (ключ, конец окна) → median *)
  let medians =
    Mf_event.of_list ~ts:(fun p -> p.ts) packets
    |> median_rssi
    |> Pipe.materialize ~by:(fun (k, _) wend -> (k, wend)) in

  (* последняя полная картина: для каждого шахтёра — медианы маяков из
     самого свежего окна *)
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

  (* top-2 по median RSSI + координаты *)
  Hashtbl.fold (fun lamp bs acc -> (lamp, bs) :: acc) by_lamp []
  |> List.sort compare
  |> List.iter (fun (lamp, bs) ->
       let top2 =
         List.sort (fun (_, a) (_, b) -> compare b a) bs
         |> List.filteri (fun i _ -> i < 2) in
       Printf.printf "Шахтёр %s — два ближайших узла (median RSSI за минуту):\n" lamp;
       List.iter (fun (beacon, med) ->
         match Hashtbl.find_opt beacons beacon with
         | Some (x, y, h) ->
           Printf.printf "  %s  %.1f dBm   → x=%.0f y=%.0f горизонт %dм\n"
             beacon med x y h
         | None ->
           Printf.printf "  %s  %.1f dBm   → координаты неизвестны\n" beacon med)
         top2;
       print_newline ())
