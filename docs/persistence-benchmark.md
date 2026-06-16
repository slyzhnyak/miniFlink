# Persistence overhead benchmark

Замер накладных расходов persistence для всех четырёх stateful
операторов в реалистичном пайплайне с **fusion** voltage/CO/RSSI
каналов через `Pipe.keyed_join` — production-like minePASS workload.

## Структура pipeline

```
                                              ┌→ voltage_alerts
packets ──→ voltage_stream ──→ Trigger ───────┤
                                              └→ window_fold (sum)

gas    ──→ co_stream ──→ Trigger ─→ co_alerts

packets ──→ silence_age ──→ silence stream

packets ──→ voltage ─┐
gas     ──→ co ──────┼─→ keyed_join ─→ process_keyed (combined FSM)
packets ──→ rssi ────┘                      ↓
                                       evacuation alerts
```

Combined FSM — **главный stateful operator** в этом workload. Он
объединяет три канала через `keyed_join`, держит per-key state с
4 полями (voltage, co, rssi, critical_since), эмитит alert на
**критическом сочетании** (voltage<3.5 И co>50 И rssi<-75).

Это и есть реалистичный minePASS use case: evacuation alert не
по одному параметру, а по комбинации трёх независимых каналов
от разных датчиков.

## Конфигурации

| Scale | Horizons × Miners | Sim time | Events | N runs |
|---|---|---|---|---|
| Small | 4 × 32 (128 miners) | 15 min | ~8K | 5 |
| Medium | 8 × 64 (512 miners) | 30 min | ~67K | 3 |
| Large | 16 × 256 (4096 miners) | 60 min | ~950K | 2 |

Запуск:
```bash
SCALE=small  ./_build/default/bench/bench_persistence.exe  # ~5s
SCALE=medium ./_build/default/bench/bench_persistence.exe  # ~20s
SCALE=large  ./_build/default/bench/bench_persistence.exe  # ~5min
```

## Результаты — Medium scale (рекомендуемый для отчётов)

**66,727 events (8 horizons × 64 miners × 30 min simulation):**

| Scenario | Median (s) | Overhead vs A | Throughput (ev/s) | Backend records | Backend bytes |
|---|---|---|---|---|---|
| **A.** Baseline (no persistence) | 0.47 | — | **127K** | — | — |
| **B.** Trigger only persisted | 0.49 | **+3%** | 123K | 204 | 234 KB |
| **C.** silence_age only persisted | 0.54 | **+15%** | 111K | 512 | 3,275 KB |
| **D.** process_keyed only persisted | 0.64 | **+36%** | 94K | 512 | 6,218 KB |
| **E.** window_fold only persisted | 0.48 | **+2%** | 125K | 0* | 0 KB* |
| **F.** **All operators persisted** | 0.68 | **+45%** | **88K** | 1,228 | 9,728 KB |

*window_fold snapshot пишется только на Watermark; в этом workload
Mock_source эмитит мало watermarks (только на границах фаз)*

### Recovery (Scenario G)

Phase 1 (33,363 events) фильтрует state в backend. Phase 2 (33,364
events) с тем же backend начинает с **restore_all** на старте каждого
оператора.

| Phase | Wall-time | State после |
|---|---|---|
| Phase 1 | 0.34 s | 1,184 records / 4.8 MB |
| Phase 2 (with restore) | 0.35 s | — |

**Restore overhead: ~3%.** Backend читается один раз на старте каждого
оператора, парсится JSON, восстанавливается hashtable.

## Ключевые insights

### 1. Trigger persistence почти бесплатна (+3%)

Snapshot пишется **только на state-transitions**. За 30-минутный
прогон с 5% gas alerts + debounce 1-2 минуты — это **1,921 sets**
на 67K events = **0.03 sets per event**.

### 2. window_fold persistence бесплатна (+2%) в этом workload

Snapshot на каждый Watermark. В Mock_source watermarks редки —
zero фактических записей. В production с regular watermarks
overhead будет выше (оценочно 5-10%).

### 3. silence_age — +15% при высоком write rate

**61,464 writes** (one per event + tick) добавляют +70 ms. JSON-
сериализация маленьких записей (last_seen + fire_at) быстрая.

### 4. process_keyed — главный contributor (+36%)

**58,807 writes** — snapshot после каждого on_event с **combined
FSM state** (4 поля: voltage, co, rssi, critical_since). State
крупнее значит JSON сериализация дороже.

Это **не оптимизированный** API: пользователь не может пометить
«state не изменился». Watermark-based batched snapshot был бы
заметно быстрее, но требует API изменений.

### 5. Full persistence overhead (+45%)

F (все 4 оператора с shared backend) показал **+45%**, что чуть
меньше арифметической суммы individual costs (3+15+36+2 = 56%).
Это потому что backend writes amortize fixed costs (hash lookup
в memory backend).

### 6. Combined pipeline vs параллельные потоки

Сравнение с первой версией benchmark (где silence/FSM/window
работали на **отдельных** копиях packet stream):

| Метрика | Parallel pipelines | Combined (этот бенч) |
|---|---|---|
| A baseline | 0.54s | 0.47s |
| F all persisted | 0.82s (+51%) | 0.68s (+45%) |
| process_keyed backend | 4.9 MB | 6.2 MB |

Combined pipeline на **15% быстрее baseline** (один проход вместо
дублирования) и **на 6% меньше overhead** при full persistence.
Это **правильнее** соответствует production minePASS deployment.

## Throughput vs production load

Production minePASS: **4,096 шахтёров × event каждые 15с = 273 events/sec**.

Worst-case throughput (F. All persisted, medium scale): **88K events/sec**.

**Margin = 320x.** Запас производительности на 2 порядка от типичной
production нагрузки. Даже с disk-backed RocksDB при 100× slowdown
margin остаётся **3×**.

## Известные ограничения

1. **In-memory backend** — реальный disk-backed backend будет медленнее
   из-за fsync/IO. Этот benchmark показывает **верхнюю границу**.

2. **Watermark frequency** — Mock_source эмитит мало watermarks. На
   реальном Kafka source window_fold overhead будет 5-10%.

3. **Single-threaded** — `parallel` API не задействован. С multi-core
   throughput будет другим.

4. **Memory pressure** — medium scale аллоцирует ~3 GB / run. На large
   scale ожидается ~40 GB allocation что может доминировать over
   persistence cost.

## Воспроизводимость

Все цифры в этом документе — `medium`, MacBook Pro M3 (12-core, 36GB).
На других машинах absolute numbers будут отличаться; относительные
overhead'ы (vs A column) стабильны.
