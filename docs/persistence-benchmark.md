# Persistence overhead benchmark

Замер накладных расходов persistence в **полном** production-like
minePASS workload, включающий:
- voltage trigger (low_voltage)
- CO trigger (high_co)
- silence_age tracking
- **Триангуляция позиций** через RSSI window_agg_keyed
- **Enriched gas alerts** (газ + координаты из триангуляции)
- Combined FSM (voltage+CO+RSSI через `Pipe.keyed_join`)
- window_fold для voltage trends

Это полная картина production minePASS — газовые алерты обогащаются
**координатами** шахтёра из real-time триангуляции, evacuation
alerts срабатывают на критическом сочетании voltage/CO/RSSI.

## Структура pipeline

```
                    ┌→ Trigger ──→ voltage_alerts
voltage ─→─────────┤
                    └→ window_fold ──→ voltage_sum

co     ─→ Trigger ──→ co_alerts

silence ─→ silence_age ──→ silence_stream

packets ─→ median_rssi (window_agg_keyed) ─→ location_stream
                                                    ↓
packets,gas,locations ─→ gas_alerts ─→ enriched_gas_alerts (with coords)

voltage ─┐
co ──────┼─→ keyed_join ─→ process_keyed (combined FSM)
rssi ────┘                          ↓
                              evacuation_alerts
```

## Важно: что persisted и что нет

**С persistence:**
- ✓ Trigger (snapshot on transitions)
- ✓ silence_age (snapshot per event + tick)
- ✓ process_keyed (snapshot per on_event/on_timer)
- ✓ window_fold (snapshot on watermark)

**Без persistence в текущей реализации:**
- ✗ window_agg_keyed (в `median_rssi`) — использует `Agg.t` с
  existential acc, persistence требует refactor `Agg.t`
- ✗ gas_alerts enrichment — manual Hashtbl с custom retract семантикой

То есть **2 из 6 stateful компонентов** в production minePASS пока
без persistence. Это документировано как known limitation в expressiveness
branch. Refactor `Agg.t` — отдельный TODO.

## Конфигурации

| Scale | Horizons × Miners | Sim time | Events | N runs |
|---|---|---|---|---|
| Small | 4 × 32 (128 miners) | 15 min | ~8K | 5 |
| Medium | 8 × 64 (512 miners) | 30 min | ~67K | 3 |
| Large | 16 × 256 (4096 miners) | 60 min | ~950K | 2 |

Запуск:
```bash
SCALE=small  ./_build/default/bench/bench_persistence.exe  # ~5s
SCALE=medium ./_build/default/bench/bench_persistence.exe  # ~30s
SCALE=large  ./_build/default/bench/bench_persistence.exe  # ~10min
```

## Результаты — Medium scale (рекомендуемый для отчётов)

**66,727 events (8 horizons × 64 miners × 30 min simulation):**

| Scenario | Median (s) | Overhead vs A | Throughput (ev/s) | Backend records | Backend bytes |
|---|---|---|---|---|---|
| **A.** Baseline (no persistence) | 2.77 | — | **22K** | — | — |
| **B.** Trigger only persisted | 2.79 | **+1%** | 22K | 204 | 234 KB |
| **C.** silence_age only persisted | 2.75 | ~0% | 22K | 512 | 3,275 KB |
| **D.** process_keyed only persisted | 2.94 | **+6%** | 20K | 512 | 6,218 KB |
| **E.** window_fold only persisted | 2.73 | ~0% | 22K | 0* | 0 KB* |
| **F.** **All four persisted** | 2.93 | **+6%** | **20K** | 1,228 | 9,728 KB |

*window_fold snapshot пишется только на Watermark; в этом workload
Mock_source эмитит мало watermarks*

### Recovery (Scenario G)

Phase 1 (33,363 events) фильтрует state в backend. Phase 2 (33,364 events)
с тем же backend'ом начинает с **restore_all**.

| Phase | Wall-time | State после |
|---|---|---|
| Phase 1 | 1.64 s | 1,184 records / 4.8 MB |
| Phase 2 (with restore) | 1.61 s | — |

**Restore overhead: ~0%** (фактически phase 2 чуть быстрее — noise).

## Где время — triangulation доминирует

Baseline pipeline тратит **2.77s на 67K events**. Allocation
**5 GB**. Это **на порядок дороже** чем без triangulation
(там было 0.47s, 3 GB).

Причина — `Pipelines.median_rssi`:
- Каждый пакет имеет до 5 readings → 5 events после flat_map
- Sliding(60s, 5s) window → каждое reading попадает в **12 окон**
- 67K packets × 5 readings × 12 окон = **4M window slots**
- В каждом slot `Agg.group_by` + `median` + `top_k_by 2`

Это **реальный** production cost minePASS — большинство время в RSSI
aggregation и triangulation, persistence overhead становится **маленьким**
относительно.

## Ключевые insights

### 1. С triangulation persistence overhead падает до ~6%

Без triangulation было +45% overhead. С ней — +6%. Причина проста:
triangulation **дороже** чем все 4 persistent operators вместе.

Это **хорошая новость**: в реальном production где cycle dominated
by triangulation, persistence добавляет marginal cost.

### 2. process_keyed (+6%) — главный contributor persistence

Те же 58,807 snapshots с **combined FSM** state (4 поля). Но
по сравнению с triangulation cost — пренебрежимо мало.

### 3. Recovery практически бесплатна

restore_all — one-shot на старте. На 1,184 records занимает
~20 ms из 1.6 секунд. Это **~1.3% overhead** от phase time.

### 4. Triangulation pipeline (median_rssi) не persisted

`window_agg_keyed` с `Agg.t` — главный stateful operator с **наибольшим**
state size (окно содержит **все** readings за 60 sec для каждого
ключа). После restart этот state теряется.

Что это значит для production:
- Triggers (voltage, CO) — **переживают** restart ✓
- Silence detection — **переживает** restart ✓
- Combined FSM evacuation — **переживает** restart ✓
- Voltage trends (window_fold) — **переживают** restart ✓
- **Triangulation** — **теряется** на restart, требует ~60 sec
  warmup до первой полной позиции после restart ✗
- **Gas enrichment** — теряется, требует первой successful triangulation ✗

Workaround в production: первые 60 sec после restart считать
**warmup phase** где gas alerts не enriched координатами.

## Throughput vs production load

Production minePASS: **4,096 шахтёров × event/15s = 273 events/sec**.

Worst-case throughput (F. All persisted, medium scale): **20K events/sec**.

**Margin = 73×.** Меньше чем без triangulation (320×), но всё равно
достаточно для production deployment.

Memory pressure: 5 GB / run при 67K events. Это **главный** scaling
issue — для large scale (950K events × 16 horizons × 256 miners)
ожидается ~70 GB allocation, что требует careful GC tuning.

## Сравнение версий benchmark

| Workload | Baseline | F all persist | Memory | Где время |
|---|---|---|---|---|
| Без triangulation (v1) | 0.54s | 0.82s (+51%) | 3 GB | persistence |
| С triangulation (v2) | 2.77s | 2.93s (+6%) | 5 GB | **triangulation** |

V2 — **правильная** production-like модель. Persistence overhead
теперь **пренебрежимо мал** относительно core compute cost.

## Известные ограничения

1. **In-memory backend** — disk-backed RocksDB будет медленнее
2. **Watermark frequency** — Mock_source эмитит мало watermarks
3. **Single-threaded** — `parallel` API не задействован
4. **Memory pressure** — на large scale ~70 GB allocation
5. **Triangulation не persisted** — known limitation Agg.t

## Воспроизводимость

Все цифры в этом документе — `medium`, MacBook Pro M3 (12-core, 36GB).
