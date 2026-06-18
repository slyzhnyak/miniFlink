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
| **A.** Baseline (no persistence) | 2.30 | — | **29K** | — | — |
| **B.** Trigger only persisted | 2.36 | **+2.6%** | 28K | 204 | 234 KB |
| **C.** silence_age only persisted | 2.31 | **+0.5%** | 29K | 512 | 3,275 KB |
| **D.** process_keyed only persisted | 2.44 | **+6.4%** | 27K | 512 | 6,218 KB |
| **E.** window_fold only persisted | 2.32 | **+0.9%** | 29K | 0* | 0 KB* |
| **F.** **All four persisted** | 2.52 | **+9.7%** | **24K** | 1,228 | 9,728 KB |

*window_fold snapshot пишется только на Watermark; в этом workload
Mock_source эмитит мало watermarks*

### Recovery (Scenario G)

Phase 1 (33,363 events) фильтрует state в backend. Phase 2 (33,364 events)
с тем же backend'ом начинает с **restore_all**.

| Phase | Wall-time | State после |
|---|---|---|
| Phase 1 | 1.47 s | 1,184 records / 4.9 MB |
| Phase 2 (with restore) | 1.43 s | — |

**Restore overhead: ~0%** (фактически phase 2 чуть быстрее — noise).

## Где время — triangulation доминирует

Baseline pipeline тратит **2.30s на 67K events**. Это **на порядок дороже**
чем без triangulation (там было 0.47s).

Причина — `Pipelines.median_rssi`:
- Каждый пакет имеет до 5 readings → 5 events после flat_map
- Sliding(60s, 5s) window → каждое reading попадает в **12 окон**
- 67K packets × 5 readings × 12 окон = **4M window slots**
- В каждом slot `Agg.group_by` + `median` + `top_k_by 2`

Это **реальный** production cost minePASS — большинство время в RSSI
aggregation и triangulation, persistence overhead становится **маленьким**
относительно.

## История улучшений

| Версия | Baseline | F overhead | Throughput at F |
|---|---|---|---|
| v1 (parallel pipelines, не реалистично) | 0.54s | +51% | 88K ev/s |
| v2 (combined через keyed_join) | 0.47s | +45% | 95K ev/s |
| v3 (+ triangulation + gas enrichment) | 2.77s | +6% | 20K ev/s |
| **v4 (silence_age O(n)→O(1) optimization)** | **2.30s** | **+9.7%** | **24K ev/s** |

V4 (текущая) включает silence_age оптимизацию из feat/profiling branch:
Timers перешёл с pending_timer list (O(n) insert/remove) на Hashtbl
(O(1)). Это ускорило **baseline** на 17% (silence_age работает в
baseline без persistence тоже) и подняло throughput с 22K до 29K ev/s.

С persistence overhead **относительно** вырос (+9.7% vs +6%) потому что
быстрее baseline → relative percent больше. **Абсолютно** persistence
теперь стоит дешевле в абсолютных секундах:
- Старая F-A: 2.93 - 2.77 = 0.16s
- Новая F-A: 2.52 - 2.30 = 0.22s

(на самом деле абсолютно стоит чуть дороже из-за добавления silence_age
fix к baseline — persistence пишет state каждое событие, но baseline
тоже теперь дешевле)

## Ключевые insights

### 1. С triangulation persistence overhead ~+10% (vs +51% без)

Без triangulation было +51% overhead. С ней — +9.7%. Причина проста:
triangulation **дороже** чем все 4 persistent operators вместе.

В реальном production где cycle dominated by triangulation,
persistence добавляет marginal cost.

### 2. process_keyed (+6.4%) — главный contributor persistence

58,807 snapshots с **combined FSM** state (4 поля). Snapshot
пишется после каждого on_event/on_timer.

### 3. Trigger persistence почти бесплатна (+2.6%)

Snapshot только на state-transitions. За 30-минутный прогон с
debounce 1-2 минуты — это 204 sets на 67K events = **0.003 sets per event**.

### 4. silence_age persistence теперь практически бесплатна (+0.5%)

Было +5% **до** O(n)→O(1) оптимизации Timers. Теперь — практически
0% overhead. Snapshot всё ещё пишется на каждое event, но обработка
самого silence_age стала настолько дешёвой, что **persistence write**
доминирует и составляет малую часть.

### 5. Recovery практически бесплатна (+0%)

restore_all — one-shot на старте. На 1,184 records занимает
~10 ms из 1.5 секунд.

### 6. Triangulation pipeline (median_rssi) не persisted

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

Worst-case throughput (F. All persisted, medium scale): **24K events/sec**.

**Margin = 88×.** Адекватно для production deployment. Даже с
disk-backed RocksDB при 100× slowdown margin остаётся **>0.8×** —
впритык, нужны оптимизации batching.

Memory pressure: ~2.5 GB / run при 67K events. Это **главный** scaling
issue — для large scale (950K events × 16 horizons × 256 miners)
ожидается ~35 GB allocation что требует careful GC tuning.

## Известные ограничения

1. **In-memory backend** — disk-backed RocksDB будет медленнее
2. **Watermark frequency** — Mock_source эмитит мало watermarks
3. **Single-threaded** — `parallel` API не задействован
4. **Memory pressure** — на large scale ~35 GB allocation
5. **Triangulation не persisted** — known limitation Agg.t

## Воспроизводимость

Все цифры в этом документе — `medium`, MacBook Pro M3 (12-core, 36GB).
