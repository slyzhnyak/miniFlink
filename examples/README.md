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
| [`ex05_fleet.ml`](ex05_fleet.ml) | **Сервис целиком** (мониторинг флота электробусов): как операторы собираются в работающий сервис — `safe_map` (битые показания не роняют сервис), `window_agg` + `Agg` (метрики по депо с алертами о низком заряде; все три агрегата retractable — `mean`, retractable-`min` через `to_list`+fold, `count_if` — поэтому опоздавшая телеметрия атомарно корректирует уже закрытые окна, эмитя `Update`, а не теряется), `allowed_lateness` (терпит опоздавшую телеметрию), **exactly-once** (накопительная статистика с durable checkpoint-ами), `retry` (устойчивая публикация сводки), `health`. Не демонстрация операторов, а код сервиса. |
| [`ex06_topology.ml`](ex06_topology.ml) | **Модель исполнения B+C** (мониторинг шахты): `merge_partitioned` (две партиции газа с разным event-time → один поток, idle-стратегия `Wall_clock_timeout`), `fan_out` (один поток → две обработки, политика на выход: аварии `Block` не теряют, дашборд `Drop_oldest`), `supervisor` (два пайплайна, политика на пайплайн: аварии `Crash_all`, дашборд `Restart`). Обе аварии из разных партиций пойманы через всю цепочку. Фундамент под Kafka-топологию (см. `connectors/kafka/`). |
| [`ex07_location/`](ex07_location/) | **Локация шахтёра по маякам связи + газовые алерты с обогащением координатами** — структурирован как реальный сервис. `Domain` (типы [packet], [gas_packet], [alert], [gas_alert] с Gas_alert/Gas_resolved, пороги газов), `Mock_source` (модуль `Default` для базового сценария — 6 шахтёров с late+dups + газовые сценарии M1-M5, и `Large_mine` для бенчмарка), `Pipelines` (`median_rssi` с расчётом позиции через линейную интерполяцию двух сильнейших маяков, `connectivity_alerts` FSM, `gas_alerts` с retract-семантикой), `Mock_sink`. Ядро в библиотеке `ex07_location_lib`. **Газовый пайплайн**: temporal-left-join трёх потоков (raw RSSI, RSSI окна, gas) через keyed state — gas driver, position enrichment; sticky алерты по `(lamp, gas)` с retract+emit при смене уровня, изменении ppm >20%, или обновлении позиции; Gas_resolved при возврате в норму. Главный кейс: газ пришёл раньше позиции → алерт без координат, потом приходит RSSI → автоматический retract+emit с координатами. Покрывает: 4 алерта диагностики + SOS + газовые алерты на 4 газа (CO/CO2/H2/CH4), скользящие окна 60с/5с, dedup, `allowed_lateness` для опоздавших, линейная интерполяция позиции вдоль линии биконов в штреке (корректный метод для геометрии тоннеля, трилатерация на 3+ маяков снаружи здесь не применима — биконы лежат на одной прямой). |
| [`ex08_triggers/`](ex08_triggers/) | **Триггерная система в стиле Zabbix** — декларативные триггеры как ортогональное дополнение к ex07. Использует библиотечные модули `Trigger` (lib/trigger.ml) и `Item` (lib/item.ml). `Domain` — собственный `type alert` с парами Problem+Recovery (Low_voltage/Voltage_ok, Voltage_critical/Voltage_recovered, No_packets/Packets_resumed, Gas_co_warning/Gas_co_safe). `Items` — извлечение item-потоков из `Mock_source` (тот же что в ex07) простыми `Pipe.map`: voltage_item, gas_co_item; `no_packets_item` через `Item.silence_age` (periodic-эмиссия «возраста тишины» через self-timer'ы). `Triggers` — декларации с гистерезисом (`less_than_with_hysteresis ~problem:3.5 ~recovery:3.7`) и debounce (`problem_for:2 минуты`); каждый триггер несёт `produce_alert` / `produce_recovery` callback'и → compile-time проверка маппинга в `Domain.alert`. Топология собирает несколько триггеров через `Trigger.combine` на одном item-потоке + `Mf_event.union` для слияния разных источников. **Ключевая идея**: добавить новый триггер = одна декларация в `triggers.ml`, ничего больше не править — ни источника, ни sink, ни существующие триггеры. Можно подключать/отключать триггеры по необходимости, можно использовать вместе с FSM-подходом из ex07. ex07 не тронут, все 7 предыдущих примеров работают как раньше. |
| [`ex09_complex_trigger/`](ex09_complex_trigger/) | **Multi-condition trigger через композицию items** — живой пример сложного триггерного выражения. Сценарий «критическая эвакуация» с условием `CO>50ppm И voltage<3.5В И avg_rssi_5m<-75dBm, дольше 1мин`. В Zabbix писалось бы inline-DSL, у нас — композиция операторов: `voltage_item` + `co_item` + `avg_rssi_item` (через `window_agg_keyed` sliding 5min/30s, `allowed_lateness` 1мин — опоздавшие RSSI-замеры атомарно уточняют среднее закрытого окна) объединяются в `combined_item` через `Mf_event.union` + `Pipe.process_keyed`. Триггер использует `Trigger.custom ~problem ~recovery` с произвольным OCaml-predicate (multi-dimensional hysteresis: CO 50→45, voltage 3.5→3.7, rssi -75→-70). Has-флаги защищают от ложных срабатываний на init-значениях до получения первых данных. 4 unit-теста проверяют семантику. Контраст с ex08 (простые threshold-триггеры): здесь сложная корреляция нескольких метрик без расширения библиотечного API. См. также `docs/triggers-cookbook.md` для разбора подхода. |
| [`ex10_keyed_join/`](ex10_keyed_join/) | **Multi-stream join по ключу + атомарный Update** — `Pipe.keyed_join` объединяет потоки трёх датчиков (температура/влажность/давление) в единый snapshot «последние значения на станцию». Условие тревоги — все три вне нормы одновременно. **Ключевой кейс**: калибровочная коррекция датчика приходит как атомарный `Update` (38.0→31.5) — `keyed_join` обновляет slot и эмитит целостный snapshot **без промежуточного `None`**, так что тревога не «мигает» между old и new. Это и есть смысл варианта `Update` против пары `Retract`+`Data`: вся магия — одна строка `keyed_join` против ~30 строк ручного управления состоянием в ex09. |

## Запуск

```bash
dune exec examples/ex01_minimal.exe
dune exec examples/ex02_alerts.exe
dune exec examples/ex03_windows.exe
dune exec examples/ex04_mine.exe
dune exec examples/ex05_fleet.exe
dune exec examples/ex06_topology.exe
dune exec examples/ex07_location/ex07_location.exe
dune exec examples/ex08_triggers/ex08_triggers.exe
dune exec examples/ex09_complex_trigger/ex09_complex_trigger.exe
dune exec examples/ex10_keyed_join/ex10_keyed_join.exe
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
