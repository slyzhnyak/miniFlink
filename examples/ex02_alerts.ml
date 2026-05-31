(* ============================================================
   Пример 2 — обогащение, правила, подавление дублей.

   Показывает: enrich из справочной таблицы (left join), генерацию
   алертов через flat_map, dedup с cooldown (не спамить одинаковыми
   алертами). Два разных типа: вход (reading) и выход (alert).

   Запуск: dune exec examples/02_alerts.exe
   ============================================================ *)

open Time

(* ── Входной тип: показание датчика ───────────────────────── *)
type reading = {
  sensor_id : string;
  celsius   : float;
  ts        : int;
  (* заполняется через enrich: *)
  limit     : float option;   (* порог для этого датчика *)
}

module Sensor = Keyed.Make (struct
  type t = reading
  let key r = r.sensor_id
end)

(* ── Выходной тип: алерт ──────────────────────────────────── *)
type alert = {
  a_sensor : string;
  a_kind   : string;
  a_ts     : int;
}

module Alert = Keyed.Make (struct
  type t = alert
  let key a = a.a_sensor
end)

(* ── Справочник: пороги по датчикам ───────────────────────── *)
let limits = Table.of_list [
  ("A", 25.0);
  ("B", 30.0);
  (* для датчика C порога нет — enrich оставит None *)
]

(* ── Правило: превышение порога → алерт ───────────────────── *)
let check (r : reading) : alert list =
  match r.limit with
  | Some lim when r.celsius > lim ->
    [{ a_sensor = r.sensor_id; a_kind = "overheat"; a_ts = r.ts }]
  | _ -> []

(* ── Пайплайн ─────────────────────────────────────────────── *)
let pipeline source =
  source
  (* обогащаем порогом из справочника *)
  |> Pipe.enrich (module Sensor)
       ~from:limits
       ~merge:(fun r lim -> { r with limit = lim })
  (* превышение → алерт (flat_map: 0 или 1 алерт на показание) *)
  |> Pipe.flat_map check
  (* не спамить: один и тот же алерт по датчику не чаще раза в 5 минут *)
  |> Pipe.dedup (module Alert)
       ~rule:(fun a -> a.a_kind)
       ~cooldown:(minutes 5)

let sample = [
  { sensor_id = "A"; celsius = 20.; ts = seconds 1; limit = None };
  { sensor_id = "A"; celsius = 26.; ts = seconds 2; limit = None };  (* > 25 → алерт *)
  { sensor_id = "A"; celsius = 27.; ts = seconds 3; limit = None };  (* дубль в cooldown → подавлен *)
  { sensor_id = "B"; celsius = 31.; ts = seconds 4; limit = None };  (* > 30 → алерт *)
  { sensor_id = "C"; celsius = 99.; ts = seconds 5; limit = None };  (* нет порога → нет алерта *)
  (* спустя 6 минут — cooldown истёк, снова алертим по A *)
  { sensor_id = "A"; celsius = 28.; ts = minutes 6 + seconds 3; limit = None };
]

let () =
  Printf.printf "=== Пример 2: алерты с обогащением и подавлением дублей ===\n\n";
  Stream.of_list (List.map (fun r -> Mf_event.data r r.ts) sample)
  |> pipeline
  |> Stream.to_list
  |> List.iter (function
       | Mf_event.Data (a, _) ->
         Printf.printf "АЛЕРТ  датчик %s:  %s  (t=%dс)\n"
           a.a_sensor a.a_kind (a.a_ts / 1000)
       | _ -> ());
  Printf.printf "\n(датчик A: 2 алерта вместо 3 — дубль в cooldown подавлен;\n";
  Printf.printf " датчик C: без алерта — нет порога в справочнике)\n"
