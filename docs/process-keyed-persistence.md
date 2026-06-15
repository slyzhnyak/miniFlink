# Persistence для `Pipe.process_keyed`

Reference для `Pipe.process_keyed ~backend`. Сценарий, формат
backend-записи, отличия от Trigger- и silence_age-persistence.
Общий паттерн — описан в `docs/trigger-persistence.md`; здесь
только специфика process_keyed.

## Зачем

`process_keyed` — низкоуровневый оператор для построения custom
FSM с per-key state и таймерами. Главные use cases:

- **Connectivity FSM** (как в ex07.connectivity_alerts): per-lamp
  state с переходами Active/Silent, event_timer для дозревания
  alert'а «нет пакетов дольше N»
- **Heartbeat detection**: ключ перестал слать события → таймер
  срабатывает, эмитим alert
- **CEP-style паттерны**: NFA с переходами по событиям

Без persistence: после рестарта **все** state'ы и таймеры
теряются. Шахтёр который перестал слать пакеты **до** crash'а — не
получит alert после restart'а (нет напоминающего таймера). Это
unacceptable для системы безопасности.

С persistence: state и таймеры восстанавливаются. Шахтёр **получит**
свой alert когда watermark дойдёт до сохранённого `fire_at`.

## API

```ocaml
val process_keyed :
  (module Keyed.S with type t = 'a) ->
  ?now_ms:(unit -> int) ->
  ?on_stat:(stat -> unit) ->
  ?backend:Persistence_backend.t ->
  ?backend_name:string ->
  ?serialize_state:('st -> Yojson.Safe.t) ->
  ?deserialize_state:(Yojson.Safe.t -> 'st) ->
  init:(unit -> 'st) ->
  on_event:('out ctx -> string -> 'st -> 'a -> unit) ->
  on_timer:('out ctx -> string -> 'st -> Time.t -> timer_kind -> unit) ->
  'a Mf_event.t Stream.t -> 'out Mf_event.t Stream.t
```

С backend'ом **три** дополнительных параметра обязательны:
- `?backend_name` — namespace в backend (нужен для изоляции, как в silence_age)
- `?serialize_state` / `?deserialize_state` — для пользовательского
  типа `'st`

Иначе `Invalid_argument`.

Ключ всегда `string` (через `Keyed.S.key`), поэтому сериализатор
ключа **не** нужен — это упрощение по сравнению с Trigger где было
три параметризованных типа.

## Формат backend-записи

### Ключ

```
"process_keyed:{backend_name}:{key}"
```

Например для FSM с `backend_name="connectivity"` и lamp `"M_critical"`:

```
process_keyed:connectivity:M_critical
```

### Значение

JSON с тремя полями:

```json
{
  "state":     <serialized 'st>,
  "ev_timers": [t1, t2, ...],
  "pt_timers": [t1, t2, ...]
}
```

- `state` — пользовательское состояние через `serialize_state`. Если
  state для ключа отсутствует (например, был очищен через
  `ctx.clear_state`) — поле = `null`.
- `ev_timers` — список event-time таймеров для этого ключа (int
  миллисекунды). Может быть пустым.
- `pt_timers` — список processing-time таймеров для этого ключа.

Если state и оба списка таймеров пусты — запись **удаляется** из
backend (`backend.delete`), не записывается с пустыми значениями.

## Когда происходит запись

`persist_key (ks)` вызывается:

- **После `on_event`** для ключа `ks` — пользовательский callback мог
  изменить state, поставить/снять таймеры
- **После `on_timer`** для ключа `ks` — таймер сработал, state и
  оставшиеся таймеры могли поменяться

Это **больше** записей чем у Trigger (только на transitions) и
эквивалентно silence_age (на каждое event + timer). Причина: в
`process_keyed` нет понятия «transition» — пользовательский
on_event может **что угодно** делать со state'ом. Безопаснее
снапшотить **после каждого** вызова.

**Trade-off:** для 4096 ключей с пакетами каждые 15с — 270 writes/sec.
Acceptable для RocksDB, не для high-frequency сетевых протоколов.
Watermark-based batched snapshot — в TODO.

## Восстановление при startup

`Pipe.process_keyed ~backend ...` вызывает `restore_all` до
обработки первого event:

1. `backend.keys ()` → список всех ключей
2. Отфильтровать по префиксу `"process_keyed:{backend_name}:"`
3. Для каждого:
   - parse JSON → `state`, `ev_timers`, `pt_timers`
   - `state ≠ null` → `Hashtbl.replace states ks (deserialize_state json)`
   - Каждый `t` из `ev_timers` → `TimerSet.add (ks, t) ev_timers`
   - Каждый `t` из `pt_timers` → `TimerSet.add (ks, t) pt_timers`

Восстановленные таймеры сработают когда watermark или wall-clock
достигнет соответствующего `t`.

## Отличия от Trigger и silence_age

| Аспект | Trigger | silence_age | process_keyed |
|---|---|---|---|
| Тип ключа | `'k` (полиморфный) | `'k` (полиморфный) | `string` (фиксированный) |
| Сериализаторы ключа | Обязательны | Обязательны | Не нужны |
| Параметризованные сериализаторы | `key`, `value`, `alert` (6) | `key` (2) | `state` (2) |
| Когда пишет | На transitions | На каждое event + tick | На каждое event + on_timer |
| Состояние | 4-state machine (фиксированная) | `(last_seen, key)` + timer | Пользовательский `'st` + timers |
| Сложность реализации | Высокая (4 варианта state) | Низкая | Средняя (custom state + N таймеров) |

`process_keyed` **проще** по типам (один ключ-string), но **гибче**
по состоянию — пользователь определяет любой `'st`.

## Namespace изоляция

Несколько `process_keyed` instance'ов на одном backend без коллизий,
если `backend_name` разные:

```ocaml
let fsm1 = stream |> Pipe.process_keyed ~backend
  ~backend_name:"connectivity" ... in
let fsm2 = stream |> Pipe.process_keyed ~backend
  ~backend_name:"gas_dosage" ...
```

Также `process_keyed`, `silence_age` и `Trigger` могут делить один
backend — их префиксы (`process_keyed:`, `item:silence_age:`,
`trigger:`) не коллидируют.

## Ограничения

### Snapshot на каждое event/timer

Как уже отмечено — snapshot пишется после **каждого** on_event и
on_timer, даже если пользователь не менял state. Это безопасно но
дорого по I/O при высокой частоте событий.

**Workaround:** не использовать persistence для high-frequency
операторов; ставить persistence на operators типа FSM с редкими
event'ами.

**Future:** добавить `?dirty:bool` флаг в ctx чтобы пользователь
явно отмечал «state изменился, нужен snapshot». Не делается сейчас —
требует API change ctx и опасно с точки зрения backwards compat.

### Filter O(N) при snapshot per key

Для записи таймеров одного ключа фильтруется **весь** TimerSet:

```ocaml
let timers_for_key timers key =
  TimerSet.fold (fun (k, t) acc ->
    if k = key then `Int t :: acc else acc
  ) !timers []
```

Это O(N) где N = общее число таймеров **всех** ключей. Для 4096
ключей с 1 таймером каждый — 4096 операций per snapshot. Acceptable.
Для high-cardinality (миллионы ключей) — медленно.

**Workaround:** перейти на `Hashtbl<key, TimerSet>` (per-key) вместо
flat TimerSet. Это инвазивный refactor process_fn.ml — отложен.

### Late events после рестарта

Mock_source.Default добавляет late dup events в конец stream'а. Если
`split_at_ts` разделяет stream по `ts`, late events с малым `ts`
попадают в Phase 1, а regular events с большим `ts` — в Phase 2.
После рестарта Phase 2 видит state восстановленный из Phase 1 и
обрабатывает свои события **корректно**, но **общий** результат
не равен «full stream без splits».

Это **не баг persistence** — это особенность split'а stream'а с late
events. В реальном production source-offset commit обеспечивает
правильную recovery: rerun **с того же offset'а**, а не split по ts.

См. test_process_keyed_persistence_e2e.ml где baseline считается на
**том же** phase1+phase2 concat, не на full source.

## Тесты

`test/test_process_keyed_persistence.ml` (5 unit-сценариев):
1. Без backend — regression (counter pipeline идентично)
2. С backend — state.count появляется в backend с правильным
   значением
3. Backend без backend_name → `Invalid_argument`
4. Restore state: counter at 2 в phase 1, новый instance продолжает
   с 3 в phase 2
5. Restore event_timer: phase 1 ставит timer на 10000, watermark
   только до 5000 (не сработал). Phase 2 (новый instance, same
   backend), watermark 15000 → timer fires

`test/test_process_keyed_persistence_e2e.ml` (E2E на Mock_source):
- FSM { Active | Silent } per lamp, event_timer на silence_threshold
- Phase 1: events до t=180с, FSM states + timers персистятся
- "Crash"
- Phase 2: новый process_keyed с тем же backend на оставшихся events
- **Invariant per key**: `phase1_alerts + phase2_alerts = baseline_alerts`
- **Edge cases**: M5 stops mid-stream (1+0=1), M3 timer fires only
  in phase 2 (0+1=1), etc

## Связь с другими модулями

`process_keyed` — низкоуровневый. Обычно его выход подключается к
другим операторам (`Trigger.of_stream`, `window_agg_keyed`). Для
полной end-to-end recovery нужно **все** stateful operators в
пайплайне иметь persistence:

```
events → process_keyed (persisted)
       → Trigger (persisted)
       → alerts
            ↑               ↑
            └── один backend ──┘
```

Без persistence у одного из операторов — recovery будет
inconsistent. Например, если `process_keyed` persisted но trigger
нет: trigger получит «свежий» state на старте и пропустит pending
Pending_problem из state'а.

## Будущее

См. TODO в README, Приоритет 6 — «Durable таймеры» для последнего
оставшегося оператора (`window_agg_keyed`). Подход тот же: общий
`Persistence_backend.t`, JSON serialization, restore at startup.
Образцы: `trigger.ml`, `item.ml`, `process_fn.ml`.

После закрытия `window_agg_keyed` все четыре stateful оператора
будут переживать рестарт, и production deployment minePASS станет
надёжным к перезапускам без потери активных alert'ов.
