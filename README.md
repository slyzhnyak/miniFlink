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
table/join с TTL (+ temporal as-of join), union потоков, retractions — плюс production-слои
(DLQ + retry/backoff, graceful shutdown, Prometheus metrics, структурированные
логи, health, config, RocksDB state). ~4100 строк OCaml, 36 тест-сюит.

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
examples/              самодостаточные примеры (см. examples/README.md)
bench/  bench.ml bench_parallel.ml
test/   36 тест-сюит (core, props, invariants, reliability, metrics,
                     retract, sliding, count_window, session_window,
                     global_window, window_fold, agg, union, safe,
                     recovery, differential, cardinality, temporal,
                     dedup_evict, parallel_crash, crash_checkpoint,
                     determinism, watermark, idle_watermark, table_ttl,
                     checkpoint_parallel, rocksdb, codec, channel,
                     window_validation, log, health_config, retry,
                     schema, backpressure, queue_depth)
```

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
| Изоляция исключений         | ✓ safe_map / safe_filter (битое событие → on_error, не падение) |
| Unit + Property тесты       | ✓ 36 сюит, QCheck-инварианты  |
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
| Источники/sink              | in-memory (seekable_of_list) + функции pull/push; MQTT/Kafka-адаптеров нет (источник подключает приложение) |

## TODO — чего ещё нет

Честный список того что {b пока не реализовано}, в порядке приоритета.
Если чего-то нет в коде — оно здесь, не нужно искать. Двигаемся сверху вниз.

### Приоритет 1 — замкнуть end-to-end exactly-once ✓ ГОТОВО

- [x] **Source offset в checkpoint.** Checkpoint хранит `cp_offset` —
  число обработанных событий, согласованное со снапшотами стейта
  (offset = сумма processed по воркерам на момент barrier, не позиция
  dispatcher — иначе рассинхрон со стейтом).
- [x] **Транзакционный / идемпотентный sink.** `idempotent_sink`
  (upsert по детерминированному ключу) и `buffered_sink` (2PC: pre-commit
  буфер → publish на commit epoch → flush хвоста при завершении).
  Commit привязан к фиксации checkpoint через barrier.
- [x] **Протокол холодного старта.** `recover`: прочитать последний
  checkpoint → восстановить стейт воркеров → перемотать источник на
  `cp_offset` → продолжить. Переобработка хвоста не создаёт дублей.
- [x] **Checkpoint на durable-хранилище.** `durable_store ~dir` пишет
  каждый checkpoint на диск + атомарный указатель `LATEST` через rename;
  `load_durable ~dir` поднимает последний после рестарта процесса.

### Приоритет 2 — операционная зрелость ✓ ГОТОВО

Реализовано с соблюдением границы библиотека/приложение: библиотека
даёт {b механизмы} (структуры, хуки, значения), а решения про I/O
(HTTP-порт, файлы конфига) остаются приложению.

- [x] **Структурированное логирование (JSON).** Модуль `Log`: события
  с уровнем, сообщением, полями. Куда писать — задаёт приложение через
  `Log.set_sink` (по умолчанию JSON в stderr). Библиотека решает {e что}
  залогировать, приложение — {e куда}.
- [x] **Health как структура.** `Health.check` собирает статус
  (readiness, размер стейта, watermark lag, queue depth) из замыканий
  приложения и отдаёт запись + `to_json`. Поднять `/health` endpoint —
  дело приложения (не тянем HTTP-стек в библиотеку).
- [x] **Глубина очереди каналов.** Хук `?on_queue_depth` в
  `run_parallel_simple`: dispatcher сообщает глубины входных каналов
  по воркерам (раннее предупреждение о backpressure). Что делать с
  числом — решает вызывающий.
- [x] **Конфигурация как типизированная запись.** Модуль `Config`:
  тип настроек + дефолты + `validate`. Откуда читать (YAML/env/CLI) —
  дело приложения; библиотека не парсит файлы, принимает значение.

### Приоритет 3 — надёжность под нагрузкой ✓ ГОТОВО

- [x] **Сквозной backpressure.** Уже работал через цепочку bounded-каналов
  (медленный sink → воркер реже читает → канал полон → dispatcher
  блокируется → source перестаёт вычитываться). Теперь это {b доказано
  тестом}: при медленном sink source не убегает вперёд, разрыв ограничен
  ёмкостью каналов, не объёмом потока.
- [x] **Retry с экспоненциальной задержкой.** Модуль `Retry`: политика
  (max_attempts, base, factor, max, jitter), `with_retry` повторяет
  транзиентные сбои и при исчерпании зовёт обработчик (обычно DLQ).
  `~sleep` вынесен параметром (тесты без реального ожидания).
- [x] **Idle watermark по wall-clock.** `Mf_event.with_idle_watermarks`:
  продвигает watermark в тишине, чтобы окна не висели. Таймер живёт в
  слое источника (неблокирующий poll), чистое event-time ядро не тронуто.
  `~now_ms`/`~sleep_ms` — параметры для тестируемости.
- [x] **Schema evolution.** Каркас (версия в заголовке + миграция) уже был;
  усилено: неизвестная версия без миграции теперь даёт {b явную ошибку},
  а не тихий проброс payload (был риск порчи данных). Покрыто тестами
  (round-trip, миграция v1→v2, граничные случаи).

### Приоритет 4 — производительность (только по замерам)

- [x] **Инкрементальная агрегация окон.** `window_fold ~init ~add` —
  окно сворачивает событие в аккумулятор сразу, а не копит весь список и
  агрегирует в конце. O(1) памяти на окно по числу событий вместо O(n),
  меньше GC-давления на больших окнах. (`window |> aggregate` оставлен
  где нужен весь список окна.) Сверху — модуль `Agg`: комбинируемые
  агрегаторы (count, sum, mean, min/max, arg_min/max, first/last) и
  `window_agg`; через `Agg.both` / `let+`/`and+` несколько агрегатов
  считаются за один проход.
- [ ] **Батчинг событий.** Обработка по одному несёт overhead на событие;
  микро-батчи амортизируют издержки каналов/сериализации.
- [ ] **Снижение аллокаций в hot path.** Профилирование + переиспользование
  буферов там где GC станет бутылочным горлышком.
- [ ] **Инкрементальные чекпойнты.** Дельты вместо полного снапшота —
  только когда полный реально станет дорогим.

### Приоритет 5 — типы окон ✓ ГОТОВО

Сейчас есть tumbling, sliding, и три новых типа:

- [x] **Count windows.** `count_window` с `count_tumbling n` /
  `count_sliding n step` — фаерят по числу событий, без watermarks
  (в этом и смысл: результат без ожидания по event-time). Per-key,
  валидация параметров.
- [x] **Session windows.** `session_window ~gap` — динамические границы
  по паузам активности, со {b слиянием} сессий когда событие перекрывает
  разрыв между ними. Закрытие по watermark (last+gap) или на конце потока.
  Это ломает «окно = чистая функция от timestamp» — отдельный оператор
  с состоянием сессий и логикой слияния.
- [x] **Global window + custom triggers.** `global_window ~trigger` — одно
  окно на ключ, политика «когда фаерить» отделена в триггер
  (`trigger_count`, `trigger_on_value` для ранней эмиссии). Действия
  Continue / Fire (накопительно) / FireAndPurge (со сбросом). Основа для
  построения других политик через разделение assigner/trigger.

### Приоритет 6 — ближе к Flink (на одном узле)

Семантика настоящего Flink, реально посильная без распределённости.
В порядке ценности; сложность и конфликты с non-goals отмечены честно.

Фундамент (открывает многое, но крупная работа):

- [ ] **ProcessFunction с таймерами.** Низкоуровневый оператор: ручное
  управление состоянием по ключу + регистрация event-time и
  processing-time таймеров, срабатывающих позже. Самое «флинковое» из
  недостающего — на таймерах строятся кастомные окна, таймауты сессий,
  отложенные действия, CEP. Требует timer service в event-time
  (срабатывание при продвижении watermark). Высокая сложность,
  наибольшая отдача.

Быстрые и ценные (чистая семантика, ложатся на существующее):

- [ ] **Side output для опоздавших.** Данные позже `allowed_lateness`
  сейчас теряются; Flink отправляет их в отдельный «late data» поток.
  `allowed_lateness` в окне уже есть — не хватает отдельного выхода.
  Низкая-средняя сложность.
- [ ] **Богатое состояние: ListState / MapState / ReducingState /
  AggregatingState.** Сейчас стейт — `bytes` через key-value backend.
  Типизированные структуры с эффективными операциями (добавить в
  список, обновить ключ). Удобство, не новая семантика. Средняя сложность.
- [ ] **Общий State TTL.** TTL есть для Table; обобщить на любое
  состояние оператора (автоистечение по времени). Механизм уже есть —
  низкая сложность.
- [ ] **Broadcast state.** Состояние, реплицируемое на всех воркеров
  (правила/конфиг, видимые всем). На single-node проще чем в
  распределённом. Низкая-средняя сложность.

Надстройки над фундаментом (требуют таймеров сначала):

- [ ] **CEP (complex event patterns).** Паттерны «A затем B в течение
  5 минут». Надстройка над ProcessFunction/таймерами — без них не
  сделать.

Под вопросом / конфликтуют с non-goals:

- [ ] **Connected streams (два входа в один оператор).** Поток данных +
  поток правил/конфига с общим состоянием. Ценно (для шахты: телеметрия
  + изменения конфигурации датчиков), но требует multi-input — а
  alignment нескольких входов мы сознательно не делаем (см. non-goals).
  Конфликт надо разрешить прежде чем браться.

Сознательно НЕ берём (отдельный проект по объёму): **SQL / Table API**
(парсер + планировщик + оптимизатор — это целый язык, не семантика),
**savepoints** (в основном операционка; durable checkpoint и schema
evolution уже частично покрывают).

### Рассмотрено и отклонено

- **Async I/O (Lwt/Eio).** Переделка pull-ядра с недоказанным выигрышем:
  при шардинге по ядрам блокирующий воркер занимает своё ядро, не тормозя
  остальных. Профиль CPU-bound, не I/O-bound. Делать только если замеры
  докажут что I/O — узкое место.
- **Distributed tracing (W3C TraceContext).** Осмыслен когда событие
  пересекает сетевые границы сервисов. На single-node достаточно
  correlation-id в структурированном логе.
- См. также {b Non-goals} ниже — распределённость целиком вне области.

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

## Документация

API-документация генерируется через odoc:

```bash
./gen_docs.sh        # → docs/api/index.html + docs/miniflink_api.pdf
```

Открыть [docs/api/index.html](docs/api/index.html) — обзор архитектуры,
ссылки на все модули, и [туториал](docs/api/tutorial.html) с пошаговым
построением конвейера. Тот же справочник единым файлом —
[docs/miniflink_api.pdf](docs/miniflink_api.pdf) (41 стр., все модули).

## Связанные материалы

- [Техническая статья (PDF)](docs/miniflink_article.pdf)
- [Популярная статья (Markdown)](docs/miniflink_popular.md)
- [**Exactly-once: глубокий разбор** (Markdown)](docs/exactly_once.md) — почему трудно, как ломается, как сделано
- [Сравнение OCaml vs Go vs TypeScript (PDF)](docs/ocaml_vs_go_ts.pdf) / [Markdown](docs/ocaml_vs_go_ts.md)
