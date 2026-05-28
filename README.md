# miniFlink

Декларативная потоковая обработка данных на OCaml.

Реализация ключевых концепций Apache Flink: event time, watermarks, windowing,
stateful operators, exactly-once, table/join, retractions — плюс production слои
(DLQ, graceful shutdown, Prometheus metrics). ~1200 строк, 42 файла.

## Почему декларативно

```ocaml
(* Весь pipeline — 10 строк, нет аннотаций типов, нет key_by *)
let pipeline source =
  source
  |> Event.with_watermarks   ~latency:(seconds 3)
  |> Pipe.enrich (module Telemetry)
       ~from:devices
       ~merge:(fun t dev -> { t with device = dev })
  |> Pipe.window (module Telemetry) (Pipe.tumbling (seconds 30))
  |> Pipe.aggregate              Rules.compute
  |> Pipe.flat_map               (Rules.check Rules.fleet)
  |> Pipe.dedup (module Alert)
       ~rule:(fun a -> a.rule)
       ~cooldown:(minutes 5)
```

`key_by` спрятан в `window` через type class `KEYED` — ключ описывается один раз в типе.
Смена формата (JSON → Protobuf) — одна строка в codec. Pipeline не меняется.

## Структура

```
Ядро (не меняется):
  stream.ml          pull-based поток: unit -> 'a option
  mf_event.ml        Data | Watermark | Retract
  pipe.ml            enrich, window, aggregate, dedup, flat_map
  keyed.ml           type class KEYED — убирает ~key: из операторов
  table.ml           Static | Snapshot таблицы
  time.ml            seconds, minutes, hours
  codec.ml           JSON / Protobuf абстракция
  domain.ml          типы с [@@deriving yojson, show]
  rules.ml           чистая бизнес-логика

Параллелизм (OCaml 4 / 5):
  channel.mli        интерфейс канала
  channel_v4.ml      Mutex + Condition (OCaml 4 Thread)
  channel_v5.ml      Atomic lock-free SPSC (OCaml 5 Domain)
  parallel.mli       интерфейс parallel runner
  parallel_v4.ml     Thread.create / Thread.join
  parallel_v5.ml     Domain.spawn / Domain.join

Production слои (все опциональны, noop по умолчанию):
  dlq.mli            dead letter queue
  dlq_noop.ml        молча считает
  dlq_log.ml         пишет в stderr с контекстом
  shutdown.mli       graceful shutdown
  shutdown_noop.ml   игнорирует SIGTERM
  shutdown_default.ml SIGTERM → drain → checkpoint → exit
  metrics.mli        Prometheus-совместимые метрики
  metrics_noop.ml    нулевой overhead
  metrics_log.ml     counter/gauge/histogram + HTTP /metrics endpoint
  barrier.mli        exactly-once parallel (Chandy-Lamport)
  state_backend.mli  персистентный стейт
  schema.mli         schema evolution + migration
  harness.mli        тестовый фреймворк
  runtime.ml         сборка всех слоёв

Makefile:
  make check_version  OCaml 4/5 → выбирает channel/parallel реализацию
  make all            собрать всё
  make bench          single-thread throughput
  make bench_parallel parallel vs sequential
```

## Запуск

```bash
# Зависимости (Ubuntu/Debian)
apt install libmosquitto-dev librdkafka-dev ocaml-findlib
apt install libppx-deriving-yojson-ocaml-dev libyojson-ocaml-dev libqcheck-ocaml-dev

# Сборка
make check_version   # → OCaml 4: Thread+Mutex | OCaml 5: Domain+Atomic
make all

# Запуск
./miniflink          # тестовые данные (noop режим)
./miniflink log      # log режим: метрики в stderr

# Тесты
./test_core_bin      # 22 unit теста
./test_props_bin     # 7 QCheck property тестов (1400 случаев)
./test_reliability_bin  # DLQ + Shutdown тесты
./test_metrics_bin   # Prometheus metrics тесты
```

## Выбор режима

```ocaml
Runtime.run Runtime.noop     (* тесты: нулевой overhead            *)
Runtime.run Runtime.log_cfg  (* разработка: метрики в stderr       *)
Runtime.run Runtime.parallel (* многопоточный, 4 workers           *)
Runtime.run Runtime.prod     (* всё: метрики HTTP :9090 + shutdown *)
```

Одна строка. Pipeline не меняется.

## Производительность

OCaml 4.14, 1 CPU, Xeon 2.80GHz, 1M events, 1000 devices, tumbling 30s:

| режим          | ev/s  | speedup |
|----------------|-------|---------|
| sequential     | 210K  | 1.0x    |
| 1 worker       | 152K  | 0.72x (channel overhead) |
| 2 workers      | 406K  | 1.94x   |
| 4 workers      | 519K  | 2.48x   |
| 8 workers      | 600K  | 2.87x   |

Channel overhead: ~106 нс/msg (Mutex+Condition).
На OCaml 5 с Domain: ожидается ~3.5x на 4 ядрах (без GIL).

## Production-readiness

| Компонент                   | Статус                        |
|-----------------------------|-------------------------------|
| Event time + watermarks     | ✓ реализовано                 |
| Windowing tumbling/sliding  | ✓ реализовано                 |
| KEYED type class            | ✓ реализовано                 |
| Stateful operators          | ✓ реализовано                 |
| Exactly-once (single)       | ✓ реализовано                 |
| Table + Join                | ✓ реализовано                 |
| Retractions                 | ✓ реализовано                 |
| Параллелизм OCaml 4/5       | ✓ реализовано                 |
| Dead Letter Queue           | ✓ реализовано (noop + log)    |
| Graceful Shutdown           | ✓ реализовано (SIGTERM/INT)   |
| Prometheus Metrics          | ✓ реализовано (HTTP :9090)    |
| Unit + Property тесты       | ✓ 29 тестов, 1400 QCheck      |
| Schema evolution            | stub → schema_default.ml      |
| Exactly-once parallel       | stub → barrier_default.ml     |
| Persistent state (RocksDB)  | stub → файловый backend        |
| MQTT adapter                | ✓ в miniflink/ (C FFI)        |
| Kafka adapter               | ✓ в miniflink/ (C FFI)        |

## Связанные материалы

- [Техническая статья (PDF)](docs/miniflink_article.pdf)
- [Популярная статья (PDF)](docs/miniflink_popular.pdf)
- [Сравнение OCaml vs Go vs TypeScript (PDF)](docs/ocaml_vs_go_ts.pdf)
