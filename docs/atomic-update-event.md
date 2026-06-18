# TODO: атомарный Update event

## Проблема

В текущей системе `Mf_event.t` имеет три варианта:

```ocaml
type 'v t =
  | Data of 'v * Time.t
  | Retract of 'v * Time.t
  | Watermark of Time.t
```

Операторы которые корректируют ранее эмитированное значение
(например, Window на late event correction) эмитят **два**
последовательных события:

```ocaml
emit_retract (key, old_result, ts);   (* retract старого *)
emit_data    (key, new_result, ts);   (* data нового *)
```

Downstream операторы видят их **последовательно**, не атомарно.
Это создаёт **flicker** в state'ах которые отслеживают snapshot:

```
keyed_join slot:
  Data(v=20)         → slot = Some 20, emit [Some 20]
  Retract(v=20)      → slot = None,    emit [None]      ← flicker
  Data(v=25)         → slot = Some 25, emit [Some 25]
```

Между Retract и Data downstream видит **None**. Если downstream
эмитит side-effect (alert, persistence write, FSM transition)
при first-None — это **ложное** срабатывание.

## Реальное влияние

В текущем minePASS workload **проблемы нет**:
- `keyed_join` подаётся выходом `Pipe.map` от raw packets
- `Pipe.map` не генерирует retract'ы
- Window'ы используются в `median_rssi`, но их выход идёт
  в `gas_alerts` (manual Hashtbl), не в `keyed_join`

То есть **в production** сейчас flicker не возникает.

Но если в будущем чей-то pipeline подаст window output в
`keyed_join` или в FSM `process_keyed` — flicker возможен.

## Архитектурное решение

Расширить `Mf_event.t`:

```ocaml
type 'v t =
  | Data of 'v * Time.t
  | Retract of 'v * Time.t
  | Update of { old : 'v; new_value : 'v; ts : Time.t }  (* NEW *)
  | Watermark of Time.t
```

Где `Update` — atomic корекция. Семантика:
- `Update { old; new_value; ts }` означает "ранее эмитировался
  `Data(old, _)`, теперь его нужно отозвать и заменить на
  `Data(new_value, ts)`"
- Downstream обрабатывает это **за один шаг**: видит old → new
  как одну транзакцию

Альтернатива — `Retract` несёт **optional replacement**:

```ocaml
| Retract of { old : 'v; replacement : 'v option; ts : Time.t }
```

Где `replacement = Some _` ≡ atomic update,
`replacement = None` ≡ standalone retract.

Первый вариант чище семантически, второй — компактнее.

## Масштаб работы

Изменение `Mf_event.t` затронет:

1. **Все операторы** (примерно 30 pattern match'ей на `Retract`):
   - Trigger, Item, Pipe, Window, Process_fn
2. **Все persistence schemas** — Trigger хранит alert events
3. **Все тесты** (~30 файлов) — конструкции `Mf_event.retract`
4. **Все примеры** — output traces
5. **Документация** — концепция retract в туториалах

**Оценка:** 2-3 сессии серьёзной работы. Backwards compat
через переходный период с **обоими** конструкторами в типе.

## Почему сейчас не делаем

1. **Не блокирует** ни одного текущего use case
2. **Не выявлено** в production minePASS workload
3. **Большой** refactor с риском introduce regressions
4. **Альтернативные** workarounds существуют:
   - Downstream проверяет "все Some" перед действием
     (через `keyed_join_map`)
   - Можно использовать `process_keyed` с явной state machine
     которая знает про transient None

## Когда делать

Триггеры для рефакторинга:
- Production использование window output в FSM-style downstream
- Жалобы пользователей на ложные срабатывания
- Появление более сложных update patterns
  (sliding aggregation с retractable агрегатами)

## Связь с другими TODO

- **M-1 (Retract dropped in Window operators)** — требует
  retractable `Agg.t`. Логически связано: если window поддерживает
  retract → ему может прийти atomic Update вместо двух событий.
- **`Agg.t` refactor** — если делать retractable Agg, то и atomic
  Update — естественно делать вместе.

Эти три задачи (retractable Agg.t, atomic Update event, M-1 Window
retract) — один большой refactor который имеет смысл делать вместе.
