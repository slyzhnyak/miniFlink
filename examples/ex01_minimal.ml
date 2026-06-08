(* ============================================================
   Пример 1 — минимальный пайплайн со своим типом.

   Показывает с нуля: свой тип события, KEYED-модуль для ключа,
   простейший пайплайн (фильтр → окно → агрегация). Никаких
   доменных типов библиотеки — всё своё, как у пользователя.

   Запуск: dune exec examples/01_minimal.exe
   ============================================================ *)

open Time

(* ── 1. Свой тип события ──────────────────────────────────── *)
(* Показания температурных датчиков. *)
type reading = {
  sensor_id : string;
  celsius   : float;
  ts        : int;       (* event-time, мс *)
}

(* ── 2. KEYED: как извлечь ключ группировки ───────────────── *)
(* Один раз описываем что группируем по sensor_id — дальше
   операторы (window, dedup) берут ключ отсюда, не повторяя. *)
module Sensor = Keyed.Make (struct
  type t = reading
  let key r = r.sensor_id
end)

(* ── 3. Пайплайн: средняя температура по датчику за окно ──── *)
let pipeline source =
  source
  (* отбрасываем явно битые показания *)
  |> Pipe.filter (fun r -> r.celsius > -100. && r.celsius < 200.)
  (* watermark: данные могут опоздать на 2с *)
  |> Mf_event.with_watermarks ~latency:(seconds 2)
  (* окна по 10 секунд + агрегаторы: среднее и счёт за один проход.
     Никакой ручной свёртки — готовые комбинируемые агрегаты. *)
  |> Pipe.window_agg (module Sensor) (Pipe.tumbling (seconds 10))
       Agg.(both (mean (fun r -> r.celsius)) count)

(* ── 4. Данные и запуск ───────────────────────────────────── *)
let sample = [
  { sensor_id = "A"; celsius = 20.0; ts = seconds 1 };
  { sensor_id = "B"; celsius = 18.5; ts = seconds 2 };
  { sensor_id = "A"; celsius = 21.0; ts = seconds 3 };
  { sensor_id = "A"; celsius = 22.0; ts = seconds 5 };
  { sensor_id = "B"; celsius = 19.0; ts = seconds 6 };
  (* битое показание — отфильтруется *)
  { sensor_id = "A"; celsius = 999.0; ts = seconds 7 };
  (* следующее окно *)
  { sensor_id = "A"; celsius = 25.0; ts = seconds 12 };
  { sensor_id = "B"; celsius = 20.0; ts = seconds 13 };
]

let () =
  Printf.printf "=== Пример 1: средняя температура по датчику за окно 10с ===\n\n";
  Mf_event.of_list ~ts:(fun (r:reading) -> r.ts) sample
  |> pipeline
  |> Stream.to_list
  |> List.iter (function
       | Mf_event.Data ((sensor, (avg, n)), window_end) ->
         let avg_s = match avg with Some a -> Printf.sprintf "%.1f°C" a | None -> "—" in
         Printf.printf "окно[..%ds]  датчик %s:  средняя %s  (%d показаний)\n"
           (window_end / 1000) sensor avg_s n
       | _ -> ())
