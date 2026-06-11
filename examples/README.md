# Примеры использования miniFlink

Самодостаточные примеры — каждый со своими типами событий, как у
пользователя библиотеки (не на внутренних доменных типах). От простого
к сложному.

| Пример | Что показывает |
|--------|----------------|
| [`ex01_minimal.ml`](ex01_minimal.ml) | Минимум: свой тип, `KEYED`-модуль для ключа, `filter → window_agg`. Среднее и счёт по датчикам через комбинируемые агрегаторы (`Agg.both`). |
| [`ex02_alerts.ml`](ex02_alerts.ml) | `enrich` из справочной таблицы, генерация алертов через `flat_map`, `dedup` с cooldown (подавление дублей). Вход и выход — разные типы. |
| [`ex03_windows.ml`](ex03_windows.ml) | Четыре типа окон на одном потоке: tumbling (время), count (количество), session (паузы активности), global + trigger (по событию). |
| [`ex04_mine.ml`](ex04_mine.ml) | **Комплексная топология** (мониторинг шахты): три источника, `union` (слияние по event-time), `update_table` (таблица порогов из потока конфигурации), `enrich` из таблиц, tumbling + session окна, агрегация, правила, `dedup`. Показывает весь функционал вместе. |
| [`ex05_fleet.ml`](ex05_fleet.ml) | **Сервис целиком** (мониторинг флота электробусов): как операторы собираются в работающий сервис — `safe_map` (битые показания не роняют сервис), `window_agg` + `Agg` (метрики по депо с алертами о низком заряде), `allowed_lateness` (терпит опоздавшую телеметрию), **exactly-once** (накопительная статистика с durable checkpoint-ами), `retry` (устойчивая публикация сводки), `health`. Не демонстрация операторов, а код сервиса. |
| [`ex06_topology.ml`](ex06_topology.ml) | **Модель исполнения B+C** (мониторинг шахты): `merge_partitioned` (две партиции газа с разным event-time → один поток, idle-стратегия `Wall_clock_timeout`), `fan_out` (один поток → две обработки, политика на выход: аварии `Block` не теряют, дашборд `Drop_oldest`), `supervisor` (два пайплайна, политика на пайплайн: аварии `Crash_all`, дашборд `Restart`). Обе аварии из разных партиций пойманы через всю цепочку. Фундамент под Kafka-топологию (см. `connectors/kafka/`). |
| [`ex07_location.ml`](ex07_location.ml) | **Локация шахтёра по маякам** (ядро minePASS): пакеты фонарей (топ-5 слышимых маяков с RSSI) → `flat_map` разворот в показания → **median RSSI** (`Agg.median` гасит выбросы радиоканала) по композитному ключу шахтёр×маяк в **скользящем окне** минута/5с → top-2 сильнейших узла на шахтёра → обогащение координатами из справочника. Готовая основа для трилатерации. |

## Запуск

```bash
dune exec examples/ex01_minimal.exe
dune exec examples/ex02_alerts.exe
dune exec examples/ex03_windows.exe
dune exec examples/ex04_mine.exe
dune exec examples/ex05_fleet.exe
dune exec examples/ex06_topology.exe
dune exec examples/ex07_location.exe
```

## С чего начать своё

Минимальный скелет (из `ex01`):

```ocaml
(* 1. свой тип события *)
type reading = { sensor_id : string; celsius : float; ts : int }

(* 2. как извлечь ключ группировки — один раз *)
module Sensor = Keyed.Make (struct
  type t = reading
  let key r = r.sensor_id
end)

(* 3. пайплайн как композиция операторов *)
let pipeline source =
  source
  |> Pipe.filter (fun r -> r.celsius < 200.)
  |> Mf_event.with_watermarks ~latency:(Time.seconds 2)
  (* окно + готовые агрегаторы (среднее и максимум за один проход) *)
  |> Pipe.window_agg (module Sensor) (Pipe.tumbling (Time.seconds 10))
       Agg.(both (mean (fun r -> r.celsius)) (max_by (fun r -> r.celsius)))

(* 4. подать данные — of_list берёт event-time из ~ts *)
Mf_event.of_list ~ts:(fun r -> r.ts) data
|> pipeline |> Stream.to_list
```

Полный справочник операторов — в [API-документации](../docs/api/index.html).
