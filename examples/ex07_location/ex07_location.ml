(** Пример 7 — топология сервиса локации шахтёров.

    Тонкий файл. Вся работа — в модулях:
    {ul
    {- {!Domain}        — типы [packet], [alert], [location], пороги, справочник маяков}
    {- {!Mock_source}   — мок-источник «как Kafka»: {!Mock_source.With_channel_jitter}}
    {- {!Pipelines}     — ядро: [median_rssi] (локация) и [connectivity_alerts] (FSM)}
    {- {!Mock_sink}     — мок-sink: рендеринг алертов и локаций в stdout}}

    В прода-сценарии заменяется только {!Mock_source} (на [Kafka_source])
    и {!Mock_sink} (на [Kafka_sink] / web-push). Пайплайны и доменная
    модель — без изменений. *)

open Ex07_location_lib

let section title = Printf.printf "\n%s\n" title

let () =
  Printf.printf "=== Локация шахтёров по маякам ===\n";

  (* ── Источник: «kafka topic» с лёгким дрожанием канала ─────── *)
  let module Src = Mock_source.With_channel_jitter in

  section "Контроль состояния шахтёра:";
  Src.read ()
  |> Pipelines.connectivity_alerts
  |> Mock_sink.publish_alerts;

  section "Статистика входа:";
  List.iter (fun s -> Printf.printf "  %s\n" s) (Src.stats ());

  (* Локации: материализуем поток в список, печатаем счётчик ретрактов,
     потом отдаём в sink. *)
  let location_events =
    Src.read ()
    |> Pipelines.median_rssi
    |> Miniflink.Stream.to_list in
  Printf.printf "  ретракций окон (от опоздавших пакетов): %d\n"
    (Mock_sink.count_retracts location_events);

  section "Локация (top-2 маяков по median RSSI, последние 3 окна каждого фонаря):";
  print_newline ();
  Mock_sink.publish_locations (Miniflink.Stream.of_list location_events)
