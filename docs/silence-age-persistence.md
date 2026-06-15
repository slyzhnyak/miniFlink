# Persistence для `Item.silence_age`

Reference для `Item.silence_age ~backend`. Сценарий, формат
backend-записи, отличия от Trigger-persistence. Общий паттерн —
описан в `docs/trigger-persistence.md`; здесь только специфика
silence_age.

## Зачем

Без persistence: `Item.silence_age` хранит `last_seen_ts` per key
в памяти. После рестарта последний пакет от шахтёра «исчезает»,
silence-age начинает с 0. Триггер `no_packets` (порог 2 минуты)
выдаёт `Recovery` на ровном месте, потом снова `Problem` через
ещё 2 минуты — диспетчер видит дребезг.

С persistence: `last_seen_ts` сохраняется, после рестарта silence
продолжается с того же момента. Триггер `no_packets` корректно
дозревает debounce без сбоя.

## API

```ocaml
val silence_age :
  ?backend:Persistence_backend.t ->
  ?backend_name:string ->
  ?serialize_key:('key -> Yojson.Safe.t) ->
  ?deserialize_key:(Yojson.Safe.t -> 'key) ->
  by:('event -> 'key) ->
  tick:Time.t ->
  'event Mf_event.t Stream.t ->
  ('key * Time.t) Mf_event.t Stream.t
```

С backend'ом **все три** дополнительных параметра обязательны:
- `?backend_name` — namespace в backend (например, `"no_packets"`)
- `?serialize_key` / `?deserialize_key` — сериализация ключа

Иначе `Invalid_argument`.

## Формат backend-записи

### Ключ

```
"item:silence_age:{backend_name}:{json_serialized_user_key}"
```

Например для silence_age с `backend_name="no_packets"` и ключом
`"M1"`:

```
item:silence_age:no_packets:"M1"
```

### Значение

JSON с тремя полями:

```json
{
  "key":          <serialized 'k>,
  "last_seen_ts": 240000,
  "fire_at":      270000
}
```

- `key` — пользовательский ключ, чтобы восстановить тип `'k` при
  старте (backend хранит `string`, нам нужен `'k`)
- `last_seen_ts` — момент последнего реального события (millis)
- `fire_at` — следующее запланированное срабатывание таймера

## Когда происходит запись

- **on_data**: новое событие → новый `last_seen_ts = ts`, новый
  `fire_at = ts + tick`. Старый таймер удаляется, новый
  регистрируется. **Snapshot write.**
- **on_timer**: таймер сработал → `last_seen_ts` не меняется
  (нового события не было), `fire_at = wm + tick`. Регистрируем
  следующий таймер. **Snapshot write.**

То есть **запись на каждый event** + **запись на каждый tick**.
Это **больше** чем у Trigger (только на transitions), но
silence_age и **не имеет** transitions — каждое event «важно».

## Восстановление при startup

`Item.silence_age ~backend ...` вызывает `restore_all` до
обработки первого event:

1. `backend.keys ()` → список всех ключей
2. Отфильтровать по префиксу `"item:silence_age:{backend_name}:"`
3. Для каждого:
   - parse JSON → `key`, `last_seen_ts`, `fire_at`
   - `Hashtbl.replace states ks (last_seen_ts, key)`
   - `Timers.insert` с `fire_at`

Восстановленный таймер сработает в момент когда watermark достигнет
сохранённого `fire_at`, выдав `(key, age)` где `age = wm -
last_seen_ts`.

## Отличия от Trigger-persistence

| Аспект | Trigger | silence_age |
|---|---|---|
| Namespace | через `spec.name` | через `?backend_name` параметр |
| Когда пишет | Только на transitions | На каждый event + каждый tick |
| Состояние | 4-state machine | Просто `(last_seen, key)` + один таймер |
| Сериализация alert | Через `serialize_alert` | Не нужна (output это `(key, age)`) |
| Сериализация value | Через `serialize_value` | Не нужна (внутреннее value — last_seen которое уже int) |

silence_age **проще** в формате backend-записи — нет nested
state-machine, нет `last_value`/`last_alert`.

## Namespace изоляция

Несколько `silence_age` instance'ов могут писать в один backend
без коллизий, если `backend_name` разные:

```ocaml
(* Два независимых silence_age на одном backend *)
let no_packets = packets |> Item.silence_age ~backend
  ~backend_name:"no_packets" ... in
let no_gas = gas_packets |> Item.silence_age ~backend
  ~backend_name:"no_gas" ...
```

Тестируется в test 5 `test_silence_age_persistence.ml`.

Также `silence_age` и `Trigger` могут делить один backend — их
префиксы (`item:silence_age:` vs `trigger:`) не коллидируют.

## Тесты

`test/test_silence_age_persistence.ml` (5 unit-сценариев):
1. Без backend — regression (поведение идентично)
2. С backend — backend.set вызывается, запись содержит корректные
   поля
3. Backend без backend_name → `Invalid_argument`
4. Restore: фаза 1 создаёт pending state, фаза 2 продолжает
   с восстановленного `last_seen` (age правильный)
5. Namespace isolation — два instance'а с разными `backend_name`
   не интерферируют

`test/test_silence_age_persistence_e2e.ml` (E2E на Mock_source):
- Phase 1: 78 events до t=180с
- "Crash"
- Phase 2: 67 events от t=180с до конца
- **Invariant per key**: `phase1_zeros + phase2_zeros = baseline_zeros`
- **Edge case** M5: 7+0=7 (M5 перестал слать до cutoff'а —
  Phase 2 правильно не фабрикует события)

## Связь с другими модулями

silence_age + Trigger вместе закрывают паттерн «нет пакетов
дольше N»:

```
packets → silence_age (persisted) → Trigger (persisted) → alerts
                  ↑                          ↑
                  └──── один backend  ───────┘
```

С `?backend` подключённым у обоих операторов — после рестарта
оба восстанавливаются из того же storage. Результат: триггер
видит правильный `age` (silence_age знает `last_seen`), и сам
помнит свой Pending_problem.

## Будущее

См. TODO в README, Приоритет 6 — «Durable таймеры» для оставшихся
операторов (`process_keyed`, `window_agg_keyed`). Подход тот же:
общий `Persistence_backend.t`, JSON serialization, restore at
startup. Образцы: trigger.ml и item.ml.
