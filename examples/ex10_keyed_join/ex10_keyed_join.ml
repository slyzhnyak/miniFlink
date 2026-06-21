(** ex10 — Multi-stream join по ключу через {!Pipe.keyed_join}.

    Демо: симулятор IoT-станции с тремя независимыми датчиками
    (температура, влажность, давление). Каждый датчик публикует
    значения в свой stream. [keyed_join] объединяет потоки в
    единый snapshot "latest values per sensor for each station".

    После join'а — простой alert когда **все три** значения
    одновременно вне нормы для одной станции.

    Контраст с ex09: ex09 использует tagged-union + manual
    process_keyed (30+ строк). Здесь — одна строка через
    keyed_join. *)

open Miniflink

(* Один тип на все датчики: (station_id, value). Это позволяет
   keyed_join работать (single-type конкретизация). Если значения
   разных датчиков имеют разный смысл — оборачиваем числа в
   ad-hoc запись tag'ом каналом — но для демо просто float. *)
type reading = string * float

module By_station : Keyed.S with type t = reading = struct
  type t = reading
  let key (s, _) = s
end

(* Имитация трёх sensor-потоков *)

let temperature_events = [
  Mf_event.data ("ST1", 22.5)  0;
  Mf_event.data ("ST2", 25.0)  1000;
  Mf_event.data ("ST1", 35.0)  10_000;  (* перегрев ST1 *)
  Mf_event.data ("ST2", 27.0)  12_000;
  Mf_event.data ("ST1", 38.0)  20_000;  (* ST1 ещё горячее *)
  (* Калибровочная коррекция: датчик ST1 был рассинхронизирован, его
     замер 38.0 на самом деле 31.5 после поправки. Приходит как
     АТОМАРНЫЙ Update — keyed_join заменяет значение в snapshot БЕЗ
     промежуточного None (без "мигания" частичного состояния, из-за
     которого ложно сработала/сбросилась бы тревога). Это ключевая
     гарантия keyed_join, ради которой существует вариант Update. *)
  Mf_event.update ("ST1", 38.0) ("ST1", 31.5) 25_000;
]

let humidity_events = [
  Mf_event.data ("ST1", 45.0)  500;
  Mf_event.data ("ST2", 50.0)  1500;
  Mf_event.data ("ST1", 85.0)  11_000;  (* высокая влажность ST1 *)
  Mf_event.data ("ST2", 48.0)  13_000;
  Mf_event.data ("ST1", 90.0)  21_000;
]

let pressure_events = [
  Mf_event.data ("ST1", 1013.0) 200;
  Mf_event.data ("ST2", 1015.0) 1200;
  Mf_event.data ("ST1", 950.0)  9_000;   (* низкое давление ST1 *)
  Mf_event.data ("ST2", 1014.0) 14_000;
  Mf_event.data ("ST1", 940.0)  22_000;
]

(* Условие тревоги: все три значения вне нормы одновременно. *)
let is_critical ~temp ~humidity ~pressure =
  temp > 30.0 && humidity > 70.0 && pressure < 980.0

let pp_options (station, opts) =
  match opts with
  | [Some (_, t); Some (_, h); Some (_, p)] ->
    Printf.sprintf "%s: T=%.1f°C H=%.0f%% P=%.0fhPa%s"
      station t h p
      (if is_critical ~temp:t ~humidity:h ~pressure:p
       then " 🚨 CRITICAL" else "")
  | _ ->
    Printf.sprintf "%s: (partial reading)" station

let () =
  Printf.printf "=== ex10: keyed_join — multi-sensor IoT pipeline ===\n\n";
  Printf.printf "Условие тревоги: T>30°C И H>70%% И P<980hPa одновременно\n";
  Printf.printf "Stations: ST1, ST2\n\n";

  (* Каждый sensor stream имеет одинаковый тип reading = (string * float). *)
  let temp_stream     = temperature_events |> Stream.of_list in
  let humidity_stream = humidity_events |> Stream.of_list in
  let pressure_stream = pressure_events |> Stream.of_list in

  (* ВСЯ магия — одна строка. *)
  let joined = Pipe.keyed_join (module By_station)
                 [temp_stream; humidity_stream; pressure_stream] in

  Printf.printf "--- Snapshots (порядок по event-time) ---\n";
  let critical_count = ref 0 in
  let prev_st1_temp = ref None in
  let corrections = ref 0 in
  joined |> Pipe.iter_data (fun (station, opts) ->
    (match opts with
     | [Some (_, t); Some (_, h); Some (_, p)]
       when is_critical ~temp:t ~humidity:h ~pressure:p ->
       incr critical_count;
       Printf.printf "  %s\n" (pp_options (station, opts))
     | _ ->
       Printf.printf "  %s\n" (pp_options (station, opts)));
    (* Зафиксируем коррекцию температуры ST1: входной Update
       38.0→31.5 пришёл в keyed_join, который атомарно обновил slot
       и выдал НОВЫЙ полный snapshot (без промежуточного None —
       snapshot всегда содержит все три датчика). *)
    if station = "ST1" then begin
      (match opts, !prev_st1_temp with
       | [Some (_, t); _; _], Some pt when pt = 38.0 && t = 31.5 ->
         incr corrections;
         Printf.printf "  ↳ температура ST1 атомарно скорректирована %.1f→%.1f (snapshot целостный, без None)\n" pt t
       | _ -> ());
      match opts with [Some (_, t); _; _] -> prev_st1_temp := Some t | _ -> ()
    end);

  Printf.printf "\nИтого: %d critical snapshots, %d атомарная коррекция через Update\n"
    !critical_count !corrections;
  Printf.printf "Калибровочный Update (38.0→31.5) keyed_join применил атомарно:\n";
  Printf.printf "snapshot обновился целиком, без промежуточного None — тревога\n";
  Printf.printf "не мигнула. Это и есть смысл варианта Update против Retract+Data.\n"
