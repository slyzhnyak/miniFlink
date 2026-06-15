# Trigger persistence: реализация и гарантии

Detailed reference для `Trigger.of_stream ~backend`. Формат записи,
recovery-семантика, ограничения. Practical guide — в
`docs/triggers-cookbook.md`.

## Архитектура

```
┌─────────────┐     state changes      ┌──────────────┐
│   Trigger   │ ──────────────────────▶│              │
│  of_stream  │                        │   backend    │
│             │ ◀──────────────────────│  (KV store)  │
└─────────────┘    restore on start    └──────────────┘
       │
       │ emit alerts (unchanged interface)
       ▼
   downstream
```

`Trigger` ничего не знает о конкретной реализации backend'а — он
работает через record-of-functions:

```ocaml
type backend = {
  get    : string -> bytes option;
  set    : string -> bytes -> unit;
  delete : string -> unit;
  keys   : unit -> string list;
}
```

Это позволяет подключить любой KV-store как backend:
- `Trigger.backend_of_memory` — обёртка над `Hashtbl.t` (для тестов)
- В production — обёртка над RocksDB через `lib/state_backend_rocksdb`
- Mock-backend для тестов с проверкой "что записалось"

## Формат записи в backend

### Ключи

Каждый per-key state хранится под ключом:

```
"trigger:{spec.name}:{json_serialized_user_key}"
```

Например для триггера `low_voltage` с ключом `"M_critical"`:

```
trigger:low_voltage:"M_critical"
```

Префикс `"trigger:{name}:"` нужен для двух целей:

1. **Изоляция между триггерами.** Несколько триггеров могут писать
   в один backend; они различаются по name.
2. **Восстановление при startup.** `restore_all` итерируется по
   `backend.keys ()` с фильтром по префиксу — не трогает чужие
   записи.

### Значения

Каждая запись — JSON-объект:

```json
{
  "key": <serialized 'k>,
  "state": <tagged state>,
  "last_event_ts": <int milliseconds>
}
```

`<serialized 'k>` — результат `spec.s_serialize_key` (например `"M_critical"`
→ `` `String "M_critical" ``).

`<tagged state>` — JSON с tagged variant'ом:

```json
{"tag": "ok"}
```

```json
{
  "tag": "pending_problem",
  "since": 225000,
  "last_value": <serialized 'v>,
  "fire_at": 345000
}
```

```json
{
  "tag": "problem",
  "since": 345000,
  "last_alert": <serialized 'a>,
  "last_alert_ts": 345000
}
```

```json
{
  "tag": "pending_ok",
  "problem_since": 345000,
  "last_alert": <serialized 'a>,
  "last_alert_ts": 345000,
  "recovery_since": 380000,
  "fire_at": 410000
}
```

### Пример полной записи

Для триггера `evacuation` с ключом `"M_critical"`, находящимся в
`Pending_problem` с `fire_at=345с` и значением `value=3.2В`:

```
key: trigger:evacuation:"M_critical"
value: {
  "key": "M_critical",
  "state": {
    "tag": "pending_problem",
    "since": 225000,
    "last_value": 3.2,
    "fire_at": 345000
  },
  "last_event_ts": 240000
}
```

## Когда происходит запись

Snapshot пишется **только на изменении state machine**:

- `Ok` → `Pending_problem` (condition впервые сработал)
- `Pending_problem` → `Problem` (debounce дозрел, alert emitted)
- `Pending_problem` → `Ok` (condition отпустился во время debounce)
- `Problem` → `Pending_ok` (recovery condition впервые сработал)
- `Pending_ok` → `Ok` (recovery debounce дозрел, Resolved emitted)
- `Pending_ok` → `Problem` (condition вернулся во время recovery debounce)

На каждом event **без** перехода — snapshot **не пишется**. Это
осознанный trade-off:

- Для 4096 шахтёров с пакетами каждые 15с это ~270 ev/s.
- Transitions редки (несколько в день на ключ).
- Write-per-transition даёт ~доли write/sec на весь сервис.
- Write-per-event дал бы 270 writes/sec — лишняя нагрузка.

**Стоимость:** между двумя event'ами без transition'а
`last_event_ts` в backend отстаёт. Если процесс упал в этом
интервале — out-of-order filter после restore примет события чуть
позже чем мог бы. Acceptable.

## Восстановление при startup

`Trigger.of_stream ~backend ...` вызывает `restore_all` **до**
обработки первого event. Процедура:

1. `backend.keys ()` — получить список всех ключей
2. Отфильтровать по префиксу `"trigger:{spec.name}:"`
3. Для каждого ключа:
   - `backend.get key` → JSON bytes
   - Parse JSON → record с `key`, `state`, `last_event_ts`
   - `spec.s_deserialize_key` восстанавливает `'k`
   - `state_of_json` восстанавливает `state` variant с
     `s_deserialize_value` / `s_deserialize_alert` где нужно
   - `Hashtbl.replace states ks (state, key)`
   - `Hashtbl.replace last_event_ts ks last_event_ts`
   - **Если state = Pending_problem или Pending_ok** — пересоздать
     timer в `Timers` с `fire_at` из state'а

Восстановленное состояние **идентично** тому что было перед
рестартом, включая pending debounce-таймеры.

## Гарантии

### Что гарантируется

- **Continuation, not restart.** Триггер в любом состоянии
  переживает рестарт и продолжает с того же места.
- **No double alerts.** Если триггер был в `Problem` перед
  крашем, после рестарта **не** будет повторного `Data alert`.
- **No lost recovery.** Если триггер был в `Pending_ok`, recovery
  debounce продолжается с того же места.
- **No false recovery.** `last_event_ts` сохраняется, late events
  после рестарта корректно фильтруются.

### Что НЕ гарантируется

- **Exactly-once на уровне output stream.** Если процесс упал
  **после** emit'а alert'а но **до** того как downstream его
  обработал — после рестарта alert повторится. Это **на уровне
  downstream**, не trigger'а.
- **Atomic coordination между триггерами.** Каждый триггер
  snapshot'ит свой namespace независимо. Если в один backend
  пишут два триггера и процесс упал между их writes — рестарт
  даст inconsistent view (один продвинулся, другой нет).
  Atomic multi-trigger checkpoint — в TODO.
- **Cross-operator consistency.** Trigger persisted, но
  `window_agg_keyed` upstream его — нет (не реализовано).
  Если пайплайн `window → trigger`, на рестарте window
  пересчитывается с нуля, trigger подгружает state — может быть
  inconsistent. См. TODO «Durable таймеры» для всех stateful
  операторов.

## Ограничения

### Сериализация

Сериализаторы пишет пользователь — библиотека не знает структуру
`'k`, `'v`, `'a`. Если backend подключён, **все шесть**
сериализаторов (`serialize_key` / `deserialize_key` /
`serialize_value` / `deserialize_value` / `serialize_alert` /
`deserialize_alert`) обязательны. Иначе `Invalid_argument` в
момент `of_stream`.

### Только string keys (детерминированно)

Внутри `Trigger.of_stream` есть локальный `key_to_string : 'k →
string` который маппит user key в string ks для внутренних
Hashtbl'ей. **Без backend'а** используется `Hashtbl.hash` (быстро,
OK для in-memory). **С backend'ом** используется JSON-сериализация
ключа через `serialize_key` — **detерминирован** между запусками
процесса.

Это значит: для триггеров с backend'ом сериализатор ключа должен
быть **детерминирован**. Для string ключей это всегда так (Yojson
выдаёт стабильный JSON для string). Для произвольных ключей
пользователь должен это обеспечить.

### Failure modes

**Invalid JSON в backend.** `restore_all` делает `failwith` с
описанием ключа и сообщением Yojson. Это **сознательно** — corrupt
snapshot молча игнорировать опасно. В production это значит:
сервис не стартует пока человек не разберётся с corruption.

**Missing JSON field.** Аналогично — `failwith`. Если backend был
заполнен старой версией формата, восстановление не пойдёт.
Migration между версиями формата — отдельная задача.

**Unknown state tag.** `failwith ("unknown state tag: ...")`. То же
самое — версионирование.

## Производительность

Замеры на in-memory backend (Hashtbl):
- Write per transition: ~1мкс (JSON serialize + Hashtbl insert)
- Read on restore: ~1мкс per key

Для типичной нагрузки (300-3000 шахтёров, transition'ы редки) это
**незначительная** доля общего времени обработки.

RocksDB write per transition: ~5-50мкс (зависит от размера значения
и WAL settings). Тоже acceptable.

## Тестирование

Тесты в `test/test_trigger_persistence.ml`:
1. Без backend поведение идентично (regression)
2. С backend — backend получает запись с правильным `state.tag`
3. Missing serializer → `Invalid_argument` с описанием
4. Roundtrip Problem-state: trigger 1 пишет state=problem, trigger 2
   с тем же backend читает и эмитит retract+recovery
5. Roundtrip Pending_problem timer: trigger 1 пишет
   state=pending_problem, trigger 2 продолжает дозревание debounce

E2E test в `test/test_trigger_persistence_e2e.ml`:
- Демо-симуляция crash mid-stream на реальном `Demo_source`
- `total_with_crash = baseline` — crash был **невидимым** для output

Integration test в `test/test_replayable_integration.ml`:
- Полная имитация настоящего рестарта: trigger + replayable_source +
  committed offset
- Демонстрирует exactly-once-style семантику
- Phase 1: обработали 33 из 67 events, committed offset
- "Crash": in-memory lost, backend сохранён
- Phase 2: new process читает offset, открывает source с этой
  позиции, новый trigger подгружает state
- Result: phase1 + phase2 = baseline

## Связь с другими модулями

| Модуль | Состояние persistence |
|---|---|
| `Trigger` | ✓ Реализовано (этот документ) |
| `lib/item.ml` silence_age | ✗ TODO |
| `Pipe.process_keyed` | ✗ TODO (есть pattern set_event_timer_for) |
| `Pipe.window_agg_keyed` | ✗ TODO |
| `Replayable_source` | ✓ Реализовано (commit offset через user code) |

Для полного recovery production-сервиса нужно подключить
persistence ко **всем** stateful операторам. Подход тот же:
record-of-functions backend + JSON serialization + restore at
startup. Trigger — образец.

## Будущее

См. TODO в README, Приоритет 6 — «Durable таймеры и persistent
state для оставшихся stateful операторов». Также Архитектурные
идеи (Goka) → «Group Table как явная first-class абстракция» —
это естественное развитие текущего trigger-only подхода в общую
абстракцию.
