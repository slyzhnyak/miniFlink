# miniFlink

Декларативная потоковая обработка данных на OCaml.

По сути это **декларативный streaming DSL для OCaml**, реализующий семантику
Apache Flink **на одном узле**. Акцент — на чистоте и выразительности модели
обработки (event time, окна, состояние, exactly-once), а не на распределённом
выполнении. Распределённость, remote shuffle и кластерная отказоустойчивость —
сознательно вне области проекта (см. Non-goals); это позволяет держать ядро
маленьким и понятным. Если нужен распределённый стоимостью промышленный движок —
это Flink; если нужна понятная, типобезопасная, тестируемая модель потоковой
обработки на одной машине — это miniFlink.

Реализация ключевых концепций Apache Flink: event time, watermarks, windowing
(tumbling, sliding, count, session со слиянием, global+triggers) с
инкрементальной агрегацией и комбинируемыми агрегаторами, stateful
operators, exactly-once (end-to-end: offset, 2PC sink, recovery, durable),
table/join с TTL (+ temporal as-of join), union потоков, retractions,
co-обработка разнотипных потоков (`co_process`), детекция отсутствия
(`on_silence`) и подавление (`suppress_while`) — плюс production-слои
(DLQ + retry/backoff, graceful shutdown, Prometheus metrics, структурированные
логи, health, config, RocksDB state). ~6100 строк OCaml в ядре, 133 тест-сюиты,
76 property-инвариантов.

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

Тот же конвейер как поток данных:

```mermaid
flowchart TD
    SRC(["source: телеметрия"]) --> WM["with_watermarks<br/><i>latency 3s — граница опоздавших</i>"]
    DEV[("devices<br/>таблица")] -.-> ENR
    WM --> ENR["enrich<br/><i>обогащение из справочника</i>"]
    ENR --> WIN["window · tumbling 30s<br/><i>группировка по ключу и времени</i>"]
    WIN --> AGG["aggregate<br/><i>Rules.compute</i>"]
    AGG --> RUL["flat_map<br/><i>Rules.check — правила → алерты</i>"]
    RUL --> DED["dedup<br/><i>cooldown 5 мин — подавление повторов</i>"]
    DED --> SINK(["sink: алерты"])

    classDef op fill:#1e293b,stroke:#475569,color:#e2e8f0;
    classDef io fill:#0f766e,stroke:#134e4a,color:#fff;
    classDef tbl fill:#7c2d12,stroke:#431407,color:#fff;
    class WM,ENR,WIN,AGG,RUL,DED op;
    class SRC,SINK io;
    class DEV tbl;
```
Смена формата (JSON → Protobuf) — одна строка в codec. Pipeline не меняется.

## Структура

```
lib/                 — библиотека miniflink
  Ядро (не меняется):
    stream.ml          pull-based поток: unit -> 'a option
    mf_event.ml        Data | Watermark | Retract | Update (атомарная коррекция)
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
examples/              самодостаточные примеры (см. examples/README.md)
bench/  bench.ml bench_parallel.ml bench_ex07.ml soak.ml
test/   133 тест-сюиты  core, props/invariants, reliability, metrics,
                       retract/update, все типы окон, window_fold, agg,
                       union, safe/on_error, parallel + crash + recovery,
                       exactly-once (property: recover=replay), temporal,
                       dedup_evict, determinism, watermark/idle, table_ttl,
                       rocksdb, codec, channel, schema, backpressure,
                       co_process, single_timer, map_ts, silence_suppress,
                       input_validation, no_watermark_warn, ex07/ex11
```

### Практика: каждый найденный баг → тест

При нахождении бага (своими руками, через ревью, по жалобе пользователя)
**сначала пишем падающий тест**, который его воспроизводит, и только
потом фикс. Это закрепляет инвариант: репликация регресса не уйдёт
тихо, и тест остаётся живой документацией «это уже было сломано, не
ломайте снова». Примеры исторически принятых уроков:

- `test_fan_out.ml :: test_concurrent_block_drain` — гонка данных при
  конкурентном чтении Block-выходов (ловит регресс синхронизации).
- `test_timers.ml :: test_emit_time_composes_with_window` —
  `process_keyed` не должен эмитить с `ts=0` (иначе выход не
  композируется с окнами).
- `test_timers.ml :: test_timer_idempotent_set` +
  `test_timer_cancel_removes_both_logical` — Flink-семантика
  идентичности таймера по `(key, t)`: два set сливаются, cancel
  снимает обе логические записи. На этом легко наступить.
- `test_timers.ml :: test_timer_fires_at_scheduled_time_not_wm` —
  `on_timer` получает `now` = время по расписанию, не текущий
  watermark, и оно может быть **в прошлом** относительно уже
  обработанных данных.
- `test_dedup_evict.ml :: test_dedup_passes_late_drops_duplicate` —
  `dedup` срубает дубль, но пропускает честный опоздавший пакет,
  который вызывает retract уже закрытого окна.
- `test_ex07_smoke.ml` — end-to-end smoke ex07: вывод содержит все
  типы алертов, M6 имеет одновременно все четыре, дубли и ретракты
  посчитаны, координаты прорисованы. Защищает от молчаливого
  регресса семантики окон/таймеров через example.
- `test_cp_offset.ml` — exactly-once при восстановлении: `cp_offset`
  должен указывать на позицию в потоке, согласованную со снапшотом, а
  не на число обработанных Data. Был реальный баг (§4.1): при
  watermark-ах между Data offset промахивался → потеря/повтор при
  recovery. Тест переписан из xfail-заглушки в строгую регрессию;
  свойство дополнительно покрыто `test_prop_exactly_once` (recover+replay
  = бескрашевый прогон на случайных потоках).

Сборка через **dune**. `channel.ml`/`parallel.ml` выбираются автоматически
по версии OCaml через `(rule (enabled_if (>= %{ocaml_version} 5.0.0)))` —
никакого ручного `cp`.

## Использование как зависимость

В своём проекте добавь зависимость и подтяни miniFlink через opam pin
из git (публикации в opam-репозиторий нет — это своя библиотека):

```bash
opam pin add miniflink https://github.com/slyzhnyak/miniFlink.git
```

В `dune-project` приложения: `(depends miniflink ...)`, в `dune`
библиотеки/бинаря приложения — `(libraries miniflink ...)`. Модули под
namespace `Miniflink.*` (`Miniflink.Pipe`, `Miniflink.Stream`, ...), либо
`open Miniflink` в начале файла.

Системное требование: **`librocksdb-dev`** должна быть установлена (C FFI
для персистентного state backend) — opam её не ставит сам, поставь
вручную (`apt install librocksdb-dev`) перед сборкой.

## Запуск

> Подробная инструкция для Debian Stable + OCaml 5 (с настоящей
> параллельностью через Domain): см. [INSTALL.md](INSTALL.md).
> Ниже — краткий рецепт для OCaml 4.14 на Ubuntu/Debian.

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

# примеры использования (со своими типами, от простого к сложному)
dune exec examples/ex01_minimal.exe   # окна + агрегация
dune exec examples/ex02_alerts.exe    # enrich + правила + dedup
dune exec examples/ex03_windows.exe   # четыре типа окон
dune exec examples/ex04_mine.exe      # комплексная топология (шахта)
dune exec examples/ex05_fleet.exe     # production-путь: EO+recovery, Agg, safe_map, schema
dune exec examples/ex06_topology.exe  # модель исполнения B+C: merge+fan_out+supervisor
dune exec examples/ex07_location/ex07_location.exe  # локация шахтёра: median RSSI, sliding, top-2 + координаты
dune exec examples/ex08_triggers/ex08_triggers.exe  # триггеры в стиле Zabbix (гистерезис, recovery)
dune exec examples/ex09_complex_trigger/ex09_complex_trigger.exe  # составной триггер (комбинация условий)
dune exec examples/ex10_keyed_join/ex10_keyed_join.exe  # multi-stream join по ключу
dune exec examples/ex11_presence.exe  # учёт присутствия: когда нужен Retract (исчезновение без замены)

# Бенчмарки
dune exec bench/bench.exe              # базовый: throughput single + parallel
dune exec bench/bench_parallel.exe     # параллелизм по ядрам
dune exec bench/bench_ex07.exe         # большая шахта (1M пакетов, ~90 сек)
dune exec bench/bench_changed_paths.exe -- 1000000  # filter + group_by под объёмом

# Все тесты (быстрые, входят в CI)
dune test

# Soak-тесты: проверка отсутствия утечек памяти.
# В наборе (быстрый, в CI): test_soak_memory — window_agg/dedup, плато памяти.
# Длинные ручные прогоны:
dune exec bench/soak.exe     -- 5000000  # window/dedup/enrich
dune exec bench/soak_agg.exe -- 5000000  # group_by/window_agg/filter + late data
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

Числа — с реальных прогонов; команды для воспроизведения в скобках.
Подробные A/B-замеры и интерпретация — в `docs/load-test-2026-06.md` и
статье `docs/ex07_article/`.

**OCaml 4.14 vs 5** — главная выгода Domain.spawn. На синтетической
нагрузке (tumbling 30s, 1000 устройств) параллелизм упирается в
runtime-lock на v4 (~2.9x потолок) и масштабируется по-настоящему на v5:

| воркеров | OCaml 4.14 (speedup) | OCaml 5.1 (speedup) |
|----------|----------------------|---------------------|
| 2        | 1.94x                | 2.22x               |
| 4        | 2.48x                | 4.58x               |
| 8        | 2.87x                | 5.00x               |
| 12       | —                    | **5.71x** (с HT)    |

(`dune exec bench/bench_scaling.exe`; sequential-числа между машинами
несопоставимы — разные CPU и нагрузка.)

**Production-сценарий ex07** — полная Large_mine (16×64 бикона, 4096
шахтёров, ~950K пакетов, ~3% дублей + ~3% опоздавших с гарантией
retract). Опоздавшие дают атомарные `Update` (old→new одним событием):

| Пайплайн              | Throughput | Параллельно (OCaml 5) |
|-----------------------|------------|-----------------------|
| `connectivity_alerts` | 397K ev/s  | до 2.68x на 4 воркерах (дальше упирается в single-thread dispatcher) |
| `median_rssi`         | 19K ev/s   | до 4.17x на 8 воркерах |

Миграция `window.ml` с immutable `Map.Make` на `Hashtbl` дала 1.63x и
−61% аллокаций (найдено направленным профилированием). Аккумулятор
`group_by`, наоборот, оставлен на immutable `Map` сознательно: он
участвует в коррекциях (`window_fold` эмитит `Update{old, new}`, где old
и new обязаны быть независимыми снимками — mutable Hashtbl молча терял бы
late data). (`dune exec bench/bench_ex07.exe`,
`bench/bench_ex07_parallel.exe`.)

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
| Атомарный Update            | ✓ коррекция old→new одним событием (без flicker); корректен во всех операторах (filter, window_agg, keyed_join, trigger) |
| Параллелизм OCaml 4/5       | ✓ реализовано                 |
| Dead Letter Queue           | ✓ реализовано (noop + log)    |
| Graceful Shutdown           | ✓ реализовано (SIGTERM/INT)   |
| Prometheus Metrics          | ✓ реализовано (HTTP :9090)    |
| Изоляция исключений         | ✓ safe_map / safe_filter + `?on_error` в process_keyed / window_fold (ядовитое событие → обработчик/DLQ, не падение) |
| Unit + Property тесты       | ✓ 133 сюиты, 76 QCheck-инвариантов |
| CI                          | ✓ активен (GitHub Actions): сборка + тесты + покрытие на OCaml 4.14 и 5.2, зелёный на каждом пуше |
| Exactly-once parallel       | ✓ реализовано (barrier + snapshot) |
| Exactly-once end-to-end     | ✓ offset + 2PC sink + recovery + durable (E2E recovery harness: kill→recover→output совпадает) |
| Структурированные логи (JSON) | ✓ Log (событие+sink, назначение — за приложением) |
| Health/readiness            | ✓ Health.check (структура, не сервер) |
| Queue depth                 | ✓ хук on_queue_depth в parallel |
| Конфигурация                | ✓ Config (типизированная запись + validate) |
| Сквозной backpressure       | ✓ через цепочку bounded-каналов (доказано тестом) |
| Retry + backoff             | ✓ Retry (экспоненциальный, jitter) |
| Idle watermark              | ✓ with_idle_watermarks (wall-clock в тишине) |
| Schema evolution            | ✓ версия + миграция, явная ошибка на неизвестной версии |
| Типы окон                   | ✓ tumbling, sliding, count, session (слияние), global+triggers |
| Агрегация окон              | ✓ инкрементальная (window_fold) + комбинируемые агрегаторы (Agg) |
| Stream-table join           | ✓ enrich (snapshot) + temporal_join (as-of, корректен при опоздавших апдейтах) |
| Persistent state (RocksDB)  | ✓ реализовано (C FFI, librocksdb) |
| Источники/sink              | in-memory (seekable_of_list) + функции pull/push; Kafka-адаптер реализован (`connectors/kafka`: seekable source + транзакционный sink через librdkafka, базовый EOS проверен на живом брокере в CI) |

## Дорожная карта

Реализованное ядро (event time, окна, состояние, exactly-once,
table/join, retractions, параллелизм, production-слои) отражено в
таблице **Production-readiness** выше. Ниже — то, чего ещё нет,
сгруппированное по темам. Это направления, а не обязательства; ничто из
списка не блокирует текущее применение на одном узле.

**Производительность** (по замерам, не спекулятивно): батчинг событий
для амортизации per-event издержек; дальнейшее снижение аллокаций в
горячем пути (массивы фикс. длины для sliding-окон); инкрементальные
чекпойнты (дельты вместо полного снапшота); multi-dispatcher fan_out для
пайплайнов с очень быстрым sequential-путём (>1M ev/s). Базлайны для
регрессий — в `bench/` (`bench_ex07` = production-сценарий 1М пакетов).

**Ближе к Flink** (на одном узле): богатое состояние (ListState /
MapState / ReducingState); broadcast state; CEP (паттерны «A затем B в
течение T»); connected streams (два входа в один оператор); безопасная
склейка композитных ключей.

**Kafka end-to-end**: сделано (`connectors/kafka`). Вход — seekable
source с offset-маппингом и recovery-seek. Выход — транзакционный sink
через librdkafka (`init/begin/commit/abort_transaction`), одна
транзакция = одна epoch чекпойнта. Базовый EOS проверен на живом брокере
в CI (видимость по commit, отбрасывание по abort, переобработка без
дублей). Строжайший read-process-write (`send_offsets_to_transaction`) —
EXPERIMENTAL: падает в C-слое против живого брокера, ждёт отладки в среде
с брокером.

**Триггерная система** (стиль Zabbix): опциональный refresh внутри
Problem-состояния; триггеры на агрегированные items; конфиг триггеров из
файла; trigger dependencies.

**Прикладной слой (ex07)**: реальные Kafka source/sink вместо mock;
checkpoint/recovery в сами ex07-пайплайны; калибровка RSSI на месте;
end-to-end интеграция операционной готовности.

Подробные отчёты о профилировании и обоснования — в статье
`docs/ex07_article/` и в истории коммитов. Распределённость целиком —
вне области (см. **Non-goals**).

## Non-goals (осознанно не делаем)

«mini» в названии честное. Следующее намеренно вне области проекта —
это распределённая инфраструктура, а не семантика потоковой обработки:

- **Распределённое выполнение.** Один узел. Нет execution graph
  поверх нескольких машин, нет распределённого планировщика.
- **Remote shuffle / network transport.** Параллелизм — через
  shared-memory каналы между потоками/доменами, не по сети.
- **End-to-end exactly-once через внешние source/sink.** Реализован
  consistent snapshot операторного стейта в параллельном режиме
  (barrier + recovery), и Kafka-коннектор сделан
  (connectors/kafka: seekable source с offset-маппингом и recovery-seek
  на входе, транзакционный sink через librdkafka на выходе, базовый EOS
  проверён на живом брокере в CI). Строжайший read-process-write
  (send_offsets_to_transaction) пока EXPERIMENTAL.
- **Incremental checkpointing.** Снапшот целиком, не дельтами.

Это сознательный выбор: проект демонстрирует *семантику* Flink
(event time, watermarks, windowing, exactly-once стейта, retractions)
максимально прозрачно, а не воспроизводит его распределённую
инфраструктуру. Для single-node до 1-2 ядер он полнофункционален.

## Документация

API-документация генерируется через odoc:

```bash
./gen_docs.sh        # → docs/api/index.html + docs/miniflink_api.pdf
```

Открыть [docs/api/index.html](docs/api/index.html) — обзор архитектуры,
ссылки на все модули, и [туториал](docs/api/tutorial.html) с пошаговым
построением конвейера. Тот же справочник единым файлом —
[docs/miniflink_api.pdf](docs/miniflink_api.pdf) (41 стр., все модули).

Тематические документы:
- [docs/tutorial.tex](docs/tutorial.tex) — **сквозной getting-started
  туториал**: от потока-функции до durable-пайплайна на одном примере
  (телеметрия датчиков), 10 шагов. Лучшая точка входа для новых
  пользователей; связывает примеры ex01–ex10 единой нитью.
- [docs/literate-pipelines/](docs/literate-pipelines/) — эксперимент по
  грамотному описанию пайплайнов (literate programming в духе Кнута):
  сравнение двух подходов (код-первичен vs noweb) на примере газовых
  тревог, с обоснованием выбора для minePASS
- [docs/expressiveness.md](docs/expressiveness.md) — улучшения выразительности
  API: `Pipe.iter_data` family, `Pipe.keyed_join` (multi-stream join по ключу)
- [docs/orthogonal-persistence.md](docs/orthogonal-persistence.md) —
  **ортогональная persistence**: один и тот же пайплайн работает с
  persistence и без, режим и сериализация задаются снаружи через
  `Runtime_context` (Marshal по умолчанию). Все четыре stateful-оператора
  (window_fold, process_keyed, silence_age, trigger) на этой модели.
  Старые operator-specific доки оставлены как redirect-заметки.
- [docs/atomic-update-event.md](docs/atomic-update-event.md) — вариант
  `Update` (атомарная коррекция old→new без промежуточного состояния)

### Аудит и нагрузочное тестирование

Кодовая база прошла многораундовый аудит (июнь 2026). Каждый раунд
закрывал свой класс проблем; все находки — с регрессионными тестами:

- [docs/inspection-2026-06.md](docs/inspection-2026-06.md) — раунд 1:
  функциональные пробелы (Update на input окон, assert→invalid_arg,
  единый persistence API)
- [docs/inspection-2026-06-round2.md](docs/inspection-2026-06-round2.md) —
  раунд 2: concurrency/IO ядра (mutex-deadlock'и при исключении в
  callback, fd-leak, div-by-zero)
- [docs/inspection-2026-06-round3.md](docs/inspection-2026-06-round3.md) —
  раунд 3: variants/connectors/FFI (GC-safety в C-стабе, два
  unbounded-memory leak, Obj.t→abstract type)
- [docs/inspection-2026-06-round4-coverage.md](docs/inspection-2026-06-round4-coverage.md) —
  раунд 4: семантика Update/Retract по всем операторам + матрица
  покрытия (filter и window_agg давали молча неверный результат на
  не-Data событиях)
- [docs/inspection-2026-06-round5-examples.md](docs/inspection-2026-06-round5-examples.md) —
  раунд 5: реалистичность примеров вскрыла silent-data-loss в
  `group_by` (mutable accumulator терял late data)
- [docs/load-test-2026-06.md](docs/load-test-2026-06.md) — нагрузочный
  тест на 4096 ламп: устранена перф-регрессия `group_by`
  (Hashtbl.copy → immutable Map), soak на 5M событий без утечек,
  корректность параллельного режима

## Связанные материалы

- [Техническая статья (PDF)](docs/miniflink_article.pdf)
- [Популярная статья (Markdown)](docs/miniflink_popular.md)
- [**Exactly-once: глубокий разбор** (Markdown)](docs/exactly_once.md) — почему трудно, как ломается, как сделано
- [Сравнение OCaml vs Go vs TypeScript (PDF)](docs/ocaml_vs_go_ts.pdf) / [Markdown](docs/ocaml_vs_go_ts.md)
