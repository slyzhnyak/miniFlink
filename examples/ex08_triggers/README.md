# ex08_triggers — триггерная система в стиле Zabbix

Демонстрация **декларативных триггеров** как ортогонального
дополнения к существующим пайплайнам. Использует общие модули
`Trigger` (`lib/trigger.ml`) и `Item` (`lib/item.ml`), переиспользует
`Mock_source` из ex07.

## Запуск

```bash
dune exec examples/ex08_triggers/ex08_triggers.exe
```

Вывод (на Mock_source.Default):

```
⚠ M6 [30000 мс]: Low voltage 3.30 В
⚠ M1 [30000 мс]: CO 130 ppm
⚠ M1 [270000 мс]: Low voltage 3.37 В
🆘 M1 [350000 мс]: CRITICAL voltage 3.20 В
```

## Структура

```
ex08_triggers/
  domain.ml          — собственный type alert с парами Problem+Recovery
  items.ml           — функции построения item-потоков (voltage, gas_co,
                       no_packets через silence_age)
  triggers.ml        — декларации триггеров (low_voltage, voltage_critical,
                       no_packets, gas_co_warning)
  ex08_triggers.ml   — топология: source → items → triggers → union → sink
  dune
```

## Ключевая идея

Чтобы добавить новый триггер, достаточно:

1. Дописать конструктор в `Domain.alert` (Problem + Recovery)
2. Добавить функцию извлечения item-потока в `Items` (если нужен новый item)
3. Дописать декларацию `Trigger.create ...` в `Triggers.ml`
4. Подключить в топологии — `Trigger.of_stream` или добавить в `Trigger.combine`

**Ничего больше не править**: ни источника, ни sink, ни существующие триггеры,
ни модули ex07. Это и есть ортогональность.

## Чем отличается от ex07

| | ex07 connectivity_alerts | ex08 triggers |
|---|---|---|
| Подход | FSM-машина в `process_keyed` | Декларативные триггеры |
| Добавить алерт | Править `Last_seen`, `Fsm`, `next_check`, `check_and_emit`, `Domain.alert` (5 точек) | Одна декларация в `triggers.ml` |
| Композиция | Один монолит на всю логику | `Trigger.combine [t1; t2; ...]` |
| Refresh внутри Problem | Поддерживается | Нет (по дизайну MVP) |
| Late events | Через `allowed_lateness` окон | Игнорируются (forward-in-time only) |
| Когда лучше | Сложная логика с переходами между состояниями (SOS, multi-condition) | Простые threshold + hysteresis + debounce |

Оба подхода **сосуществуют**: в реальном сервисе можно использовать FSM
там где нужны сложные переходы, и триггеры — для большинства простых
threshold-алертов.

## Persistence (опционально)

Каждый из триггеров в `Triggers` можно запустить с **persistent
state** через опциональный параметр `?backend` у `Trigger.of_stream`
или `Trigger.combine`. После рестарта сервиса:

- Триггер в `Problem` помнит что он в `Problem` — не выпустит повторный alert
- Триггер в `Pending_problem` помнит свой `since`-момент — debounce продолжается с того же места, а не начинается заново
- `last_event_ts` сохраняется — out-of-order фильтр работает корректно после рестарта

В ex08 persistence не используется (упрощённый учебный пример).
Чтобы подключить, нужно:

1. Добавить серилизаторы в `Trigger.create`:
   ```ocaml
   ~serialize_key:(fun k -> `String k)
   ~deserialize_key:(fun j -> Yojson.Safe.Util.to_string j)
   ~serialize_value:(fun v -> `Float v)
   ~deserialize_value:(fun j -> Yojson.Safe.Util.to_number j)
   ~serialize_alert:Domain.alert_to_json
   ~deserialize_alert:Domain.alert_of_json
   ```

2. Создать backend (memory для тестов, RocksDB для прода):
   ```ocaml
   let tbl = Hashtbl.create 64 in
   let backend = Trigger.backend_of_memory tbl in
   ```

3. Передать backend в `of_stream` или `combine`:
   ```ocaml
   item |> Trigger.of_stream ~backend triggers_spec
   ```

### no_packets и persistence

Триггер `no_packets` особенный — он опирается на `Item.silence_age`
который тоже хранит per-key state (`last_seen` и pending tick
timer). Чтобы триггер «нет пакетов 2 минуты» **полностью** пережил
рестарт, нужно подключить persistence и к silence_age **тоже**:

```ocaml
let no_packets_item packets =
  packets
  |> Item.silence_age
       ~backend                       (* тот же что у Trigger *)
       ~backend_name:"no_packets"
       ~serialize_key:(fun k -> `String k)
       ~deserialize_key:(fun j -> Yojson.Safe.Util.to_string j)
       ~by:(fun (p : Domain.packet) -> p.lamp)
       ~tick:(Time.seconds 30)
```

С обоими подключёнными к одному backend'у после рестарта
восстанавливается **и** silence_age (знает `last_seen` каждого
шахтёра), **и** trigger (помнит свой Pending_problem). Без
silence_age persistence триггер `no_packets` после рестарта получит
ложный `(key, 0)` и сбросит debounce.

Подробности — в `docs/triggers-cookbook.md` (раздел Persistence),
`docs/trigger-persistence.md` (Trigger reference) и
`docs/silence-age-persistence.md` (silence_age reference).
