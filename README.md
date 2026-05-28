# miniFlink

Функциональная семантика потоковой обработки данных на OCaml.

Учебная реализация ключевых концепций Apache Flink: event time, watermarks, windowing, stateful operators, exactly-once checkpointing, table/join, retractions — в ~600 строк без внешних зависимостей.

## Структура

```
src/
  core/          — ядро (не меняется никогда)
    stream.ml      pull-based поток: unit -> 'a option
    event.ml       Data | Watermark | Retract
    pipe.ml        операторы: enrich, window, aggregate, dedup
    keyed.ml       type class KEYED — убирает ~key: из операторов
    table.ml       Static | Snapshot таблицы
    time.ml        единицы времени: seconds, minutes
    codec.ml       абстракция над форматом (JSON / Protobuf)
    domain.ml      доменные типы с [@@deriving yojson, show]
    rules.ml       чистая бизнес-логика

  parallel/      — параллелизм (опционально)
    channel.mli    интерфейс канала
    channel_v4.ml  Mutex + Condition (OCaml 4)
    channel_v5.ml  Atomic lock-free SPSC (OCaml 5)
    parallel.mli   интерфейс parallel runner
    parallel_v4.ml Thread.create / Thread.join
    parallel_v5.ml Domain.spawn / Domain.join

  layers/        — production слои (все опциональны)
    barrier.*      exactly-once parallel (Chandy-Lamport)
    state_backend.* персистентный стейт (Memory | RocksDB)
    metrics.*      observability (noop | log | OTel)
    dlq.*          dead letter queue (noop | log | Kafka)
    shutdown.*     graceful shutdown (noop | default)
    schema.*       schema evolution (noop | versioned)
    harness.*      тестовый фреймворк
    runtime.ml     сборка всех слоёв

  adapters/      — источники и стоки
    mqtt_stubs.c   C FFI для libmosquitto
    mqtt.ml        MQTT source / sink
    kafka_stubs.c  C FFI для librdkafka
    kafka.ml       Kafka source / sink

  app/           — приложение
    main.ml        декларативный pipeline (10 строк)
    fixtures.ml    тестовые данные

bench/           — бенчмарки
  bench.ml           single-thread throughput
  bench_parallel.ml  parallel vs sequential
```

## Ключевые концепции

### Декларативный pipeline

```ocaml
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

Нет `key_by` — он спрятан в `window` через type class `KEYED`.
Нет аннотаций типов внутри `|>` цепочки.

### Смена формата — одна строка

```ocaml
(* JSON *)
let telemetry_codec = Codec.json
  ~encode:Domain.telemetry_to_yojson
  ~decode:Domain.telemetry_of_yojson

(* Protobuf — pipeline не меняется *)
let telemetry_codec = Codec.protobuf
  ~encode:Domain_pb.encode_telemetry
  ~decode:Domain_pb.decode_telemetry
```

### Opтогональные слои

```ocaml
(* Разработка *)
Runtime.run Runtime.noop ~key_of ~source ~pipeline ~sink

(* Production: метрики + shutdown + DLQ + persistent state *)
Runtime.run Runtime.prod ~key_of ~source ~pipeline ~sink
```

Каждый слой: `.mli` (контракт) + `_noop.ml` (заглушка) + `_default.ml` (реализация).

## Сборка

```bash
# Зависимости
apt install libmosquitto-dev librdkafka-dev
opam install ppx_deriving_yojson yojson ppx_deriving

# OCaml 4 или 5 — Makefile определяет автоматически
make check_version   # показать выбранные реализации
make all             # собрать всё
make bench           # single-thread benchmark
make bench_parallel  # parallel benchmark

# Запуск
./miniflink          # тестовые данные
./miniflink kafka    # реальный Kafka broker
```

## Производительность

OCaml 4.14, 1 CPU, Xeon 2.80GHz:

| режим | ev/s | speedup |
|---|---|---|
| sequential (full pipeline) | 210K | 1.0x |
| 2 workers | 406K | 1.94x |
| 4 workers | 519K | 2.48x |
| 8 workers | 600K | 2.87x |

На OCaml 5 с Domain ожидается ~3.5-3.8x на 4 ядрах.

## Production-readiness

| Компонент | Статус |
|---|---|
| Event time + watermarks | ✓ |
| Windowing (tumbling/sliding) | ✓ |
| Stateful operators + key_by | ✓ |
| Exactly-once (single-thread) | ✓ |
| Table + Join (lookup/interval/union) | ✓ |
| Retractions (invertible + recompute) | ✓ |
| MQTT / Kafka adapters | ✓ |
| Параллелизм (OCaml 4 + 5) | ✓ |
| Persistent state backend | stub (файловый) |
| Exactly-once parallel | stub (barrier интерфейс) |
| Мониторинг / метрики | stub → metrics_log |
| Graceful shutdown | ✓ |
| Dead letter queue | ✓ |
| Schema evolution | stub |
| Тесты | harness готов |

## Требования

- OCaml 4.14+ или OCaml 5.x
- `ppx_deriving_yojson`, `yojson` для доменных типов
- `libmosquitto-dev` для MQTT (опционально)
- `librdkafka-dev` для Kafka (опционально)
