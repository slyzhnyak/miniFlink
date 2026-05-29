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
lib/                 — библиотека miniflink
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
  Параллелизм:
    channel.mli        интерфейс канала
    parallel.mli       интерфейс parallel runner
    (channel.ml и parallel.ml генерируются dune из variants/)
  Production слои (все опциональны, noop по умолчанию):
    dlq.*              dead letter queue (noop | log)
    shutdown.*         graceful shutdown (noop | SIGTERM drain)
    metrics.*          Prometheus метрики (noop | log + HTTP endpoint)
    barrier.*          exactly-once parallel (Chandy-Lamport)
    state_backend.*    персистентный стейт (noop | memory | rocksdb)
    schema.*           schema evolution + migration
    harness.*          тестовый фреймворк
    runtime.ml         сборка всех слоёв
    fixtures.ml        тестовые данные

variants/            — реализации channel/parallel по версии OCaml
  channel_v4.ml        Mutex + Condition (OCaml 4 Thread)
  channel_v5.ml        Atomic lock-free SPSC (OCaml 5 Domain)
  parallel_v4.ml       Thread.create / Thread.join
  parallel_v5.ml       Domain.spawn / Domain.join

bin/    main.ml        демо pipeline
bench/  bench.ml bench_parallel.ml
test/   13 тест-сюит (core, props, reliability, metrics,
                     retract, sliding, dedup_evict, parallel_crash,
                     determinism, watermark, table_ttl,
                     checkpoint_parallel, rocksdb)
```

Сборка через **dune**. `channel.ml`/`parallel.ml` выбираются автоматически
по версии OCaml через `(rule (enabled_if (>= %{ocaml_version} 5.0.0)))` —
никакого ручного `cp`.

## Запуск

```bash
# Зависимости (Ubuntu/Debian)
apt install ocaml-dune ocaml-findlib
apt install libppx-deriving-yojson-ocaml-dev libyojson-ocaml-dev libqcheck-ocaml-dev
apt install librocksdb-dev   # для персистентного state backend (C FFI)

# Сборка — dune сам выберет v4 (OCaml 4) или v5 (OCaml 5)
dune build

# Запуск демо
dune exec bin/main.exe          # тестовые данные (noop режим)
dune exec bin/main.exe log      # log режим: метрики в stderr

# Бенчмарки
dune exec bench/bench.exe
dune exec bench/bench_parallel.exe

# Все тесты (быстрые, входят в CI)
dune test

# Soak-тест (медленный, вручную): проверка отсутствия утечек памяти
dune exec bench/soak.exe              # 2M событий
dune exec bench/soak.exe -- 10000000  # 10M событий
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
| Exactly-once parallel       | ✓ реализовано (barrier + snapshot) |
| Persistent state (RocksDB)  | ✓ реализовано (C FFI, librocksdb) |
| MQTT adapter                | ✓ в miniflink/ (C FFI)        |
| Kafka adapter               | ✓ в miniflink/ (C FFI)        |

## Non-goals (осознанно не делаем)

«mini» в названии честное. Следующее намеренно вне области проекта —
это распределённая инфраструктура, а не семантика потоковой обработки:

- **Распределённое выполнение.** Один узел. Нет execution graph
  поверх нескольких машин, нет распределённого планировщика.
- **Remote shuffle / network transport.** Параллелизм — через
  shared-memory каналы между потоками/доменами, не по сети.
- **End-to-end exactly-once через внешние source/sink.** Реализован
  consistent snapshot операторного стейта в параллельном режиме
  (barrier + recovery). Координация offset-ов источника и
  транзакционный commit в sink (полный Kafka EOS) — отдельная
  большая задача, не сделана.
- **Incremental checkpointing.** Снапшот целиком, не дельтами.

Это сознательный выбор: проект демонстрирует *семантику* Flink
(event time, watermarks, windowing, exactly-once стейта, retractions)
максимально прозрачно, а не воспроизводит его распределённую
инфраструктуру. Для single-node до 1-2 ядер он полнофункционален.

## Связанные материалы

- [Техническая статья (PDF)](docs/miniflink_article.pdf)
- [Популярная статья (PDF)](docs/miniflink_popular.pdf)
- [Сравнение OCaml vs Go vs TypeScript (PDF)](docs/ocaml_vs_go_ts.pdf)
