# Persistence overhead benchmark

Замер накладных расходов persistence для всех четырёх stateful
операторов в реалистичном пайплайне (события + газы) на масштабе
типичной шахты.

## Что меряли

`bench/bench_persistence.ml` — полный пайплайн minePASS-style:

- **packet stream** (15-секундный период от каждого шахтёра)
  - `event_time` watermarks
  - `Trigger.of_stream` для **low_voltage** (порог 3.5В, debounce 2 мин)
  - `Item.silence_age` для трекинга молчания, tick 30с
  - `Pipe.process_keyed` — FSM connectivity-like с event_timer на 60с silence
  - `Pipe.window_fold` — sum voltage в tumbling(1 min)
- **gas stream** (10-секундный период)
  - `event_time` watermarks
  - `Trigger.of_stream` для **high_co** (порог 50 ppm, debounce 1 мин)

Семь сценариев A–G, каждый запускается N раз с warmup + measure.
Метрики: wall-time, allocated bytes, backend writes count, backend size.

## Конфигурации

| Scale | Horizons × Miners | Sim time | Events | N runs |
|---|---|---|---|---|
| Small | 4 × 32 (128 miners) | 15 min | ~8K | 5 |
| Medium | 8 × 64 (512 miners) | 30 min | ~67K | 3 |
| Large | 16 × 256 (4096 miners) | 60 min | ~950K | 2 |

Запуск: `SCALE=medium ./_build/default/bench/bench_persistence.exe`

## Результаты — Medium scale (рекомендуемый для отчётов)

**66,727 events (8 horizons × 64 miners × 30 min simulation):**

| Scenario | Median (s) | Overhead vs A | Throughput (events/s) | Backend records | Backend bytes |
|---|---|---|---|---|---|
| **A.** Baseline (no persistence) | 0.54 | — | **110K** | — | — |
| **B.** Trigger only persisted | 0.53 | ≈ 0% | 112K | 204 | 234 KB |
| **C.** silence_age only persisted | 0.57 | **+5%** | 104K | 512 | 3,275 KB |
| **D.** process_keyed only persisted | 0.78 | **+43%** | 77K | 512 | 4,932 KB |
| **E.** window_fold only persisted | 0.53 | ≈ 0% | 114K | 0* | 0 KB* |
| **F.** **All operators persisted** | 0.82 | **+51%** | **73K** | 1,228 | 8,442 KB |

*window_fold snapshot пишется только на Watermark; в этом workload watermarks
эмитятся через `Pipe.event_time` но границы окон редко пересекаются за
30-минутный прогон, поэтому фактических записей мало.*

### Recovery (Scenario G)

Phase 1 (33,363 events) фильтрует state в backend. Phase 2 (33,364 events)
с тем же backend'ом начинает с **restore_all** на старте каждого
оператора.

| Phase | Wall-time | State после |
|---|---|---|
| Phase 1 | 0.44 s | 1,184 records / 4.2 MB |
| Phase 2 (with restore) | 0.46 s | — |

**Restore overhead: ~5%.** Backend читается один раз на старте каждого
оператора, парсится JSON, восстанавливаются hashtable/timers. На 1,184
records это занимает ~20 ms из 460 ms общего времени Phase 2.

## Ключевые insights

### 1. Trigger persistence почти бесплатна (~0%)

Snapshot пишется **только на state-transitions** (Ok→Pending_problem,
Pending_problem→Problem, etc). За 30-минутный прогон с 5% gas alerts +
debounce 1-2 минуты — это **1,921 sets** на 67K events = **0.03 sets per event**.

### 2. window_fold persistence бесплатна (~0%) в этом workload

Snapshot на каждый Watermark. В benchmark данных watermarks приходят
после `Pipe.event_time` который генерирует их по lateness window, но
большинство данных in-order и watermarks **не двигают** state. **0 sets**
зафиксировано.

В реалистичном production деплое с regular watermark emission cost будет
выше — оценочно сопоставимо с silence_age (~5-10% overhead).

### 3. silence_age — низкая overhead (+5%) при высоком write rate

**61,464 writes** (one per event + tick) обработаны за +30 ms. JSON-сериализация
маленьких записей (last_seen + fire_at) очень быстрая. Backend in-memory
не требует сетевых вызовов.

### 4. process_keyed — основной contributor overhead (+43%)

**59,374 writes** — snapshot пишется после **каждого** on_event и on_timer,
даже если state не изменился. State включает per-key dict + два timer sets
которые фильтруются per key (O(N) total timers на каждый write).

Это **не оптимизированный** API: пользователь не может пометить «state
не изменился, не нужен snapshot». Watermark-based batched snapshot
(как window_fold) был бы значительно быстрее, но требует API изменений.

### 5. Full persistence overhead аддитивна (+51%)

F (все 4 оператора) показал ~ sum(B+C+D+E) = 0 + 5 + 43 + 0 ≈ 48%.
Фактически 51% — близко к ожидаемому. Backend shared между операторами
не создаёт contention.

## Throughput vs production load

Production minePASS: ~4,096 шахтёров × event каждые 15с = **273 events/sec**.

Наш worst-case throughput (F. All persisted, medium scale): **73K events/sec**.

**Margin = 270x.** Persistence на всех операторах одновременно даёт
запас производительности на 2 порядка от типичной production нагрузки.
В реальности backend будет не in-memory а RocksDB/Postgres с
дополнительной задержкой — даже с 100x slowdown остался бы margin 2-3x.

## Известные ограничения benchmark

1. **In-memory backend** — реальный disk-backed backend будет медленнее
   из-за fsync/IO. Это benchmark показывает **верхнюю границу throughput**.
   Для disk-backed замера нужна интеграция с RocksDB.

2. **Watermark frequency** — Mock_source эмитит мало watermarks (только
   на границах фаз). На реальном Kafka source с regular watermark
   emission window_fold overhead будет выше.

3. **Single-threaded** — `parallel` API не задействован. С `Parallel.run`
   measure throughput на multiple cores будет другим.

4. **Memory pressure** — медиум scale аллоцирует 3 GB / run. На large
   scale (16×256 = 4096 miners) ожидается ~40 GB allocation что может
   доминировать over persistence cost.

## Воспроизводимость

```bash
SCALE=small  ./_build/default/bench/bench_persistence.exe  # ~5s
SCALE=medium ./_build/default/bench/bench_persistence.exe  # ~20s
SCALE=large  ./_build/default/bench/bench_persistence.exe  # ~5min
```

Все цифры в этом документе — `medium`, MacBook Pro M3 (12-core, 36GB).
На других машинах absolute numbers будут отличаться; относительные
overhead'ы (vs A column) стабильны.
