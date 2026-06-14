# ex09_complex_trigger — Multi-condition trigger через композицию

Живой пример сложного триггерного выражения, разобранного в
`docs/triggers-cookbook.md`.

## Сценарий

> **Критическая ситуация эвакуации:** алерт срабатывает если шахтёр
> M находится в опасной зоне (повышенный CO **И** падающая батарея
> **И** слабая связь) дольше 1 минуты.

В Zabbix DSL это:
```
last(co_ppm) > 50 and last(voltage) < 3.5 and avg(rssi, 5m) < -75
with problem_for = 1m
```

В нашей системе — композиция четырёх операторов:

```ocaml
let voltage = Items.voltage_item packets in
let co      = Items.co_item gas in
let rssi    = Items.avg_rssi_item packets in        (* 5-min sliding avg *)
let combined = Items.combined_item ~voltage ~co ~rssi in
combined |> Trigger.of_stream Triggers.evacuation
```

## Запуск

```bash
dune exec examples/ex09_complex_trigger/ex09_complex_trigger.exe
```

На `Mock_source.Default` алерт **не срабатывает** — это правильное
поведение. Default-сценарий не содержит шахтёра, у которого одновременно:
- CO выше 50 ppm
- voltage ниже 3.5 В
- среднее RSSI за 5 минут хуже -75 dBm

Сложный multi-condition trigger срабатывает редко, только при настоящей
опасности — в этом и смысл.

## Структура

```
ex09_complex_trigger/
  domain.ml          — type alert (Evacuation_critical, Evacuation_cleared)
                        + type combined для triplet значения
  items.ml           — voltage_item, co_item, avg_rssi_item, combined_item
  triggers.ml        — Triggers.evacuation с Trigger.custom predicate
  ex09_complex_trigger.ml  — топология
  dune
```

## Что демонстрирует

1. **Объединение нескольких потоков** через `Mf_event.union` +
   `Pipe.process_keyed`. Per-key state накапливает последние значения
   каждой компоненты.

2. **Скользящее среднее за окно** через `window_agg_keyed` с
   sliding(5m, 30s) — стандартная композиция, не отдельный
   "встроенный" функционал триггера.

3. **Custom predicate** через `Trigger.custom ~problem ~recovery`.
   Predicate — это произвольная OCaml-функция, любой сложности.

4. **Multi-dimensional hysteresis** — recovery срабатывает когда
   **любое** из условий отпустилось ниже своего отдельного
   hysteresis-порога. CO (50→45), voltage (3.5→3.7), rssi (-75→-70).

5. **Has-флаги для инициализации** — триггер не срабатывает пока
   данные по всем трём компонентам не получены. Защищает от
   ложных алертов на init-значениях.

## Тестирование (test_ex09_complex_trigger.ml)

Четыре сценария с синтетическими triplet-данными:
1. Все три условия выполнены, держатся дольше problem_for → alert
2. Сильный RSSI ломает AND → нет алерта
3. Нет данных по CO (`has_co=false`) → нет алерта (инициализация)
4. Alert + recovery с retract'ом → 2 Data + 1 Retract

## Сравнение с ex08

| | ex08_triggers | ex09_complex_trigger |
|---|---|---|
| Items | Простые scalar (voltage, co, silence) | Composite triplet через union+process_keyed |
| Окна | Нет | Sliding 5min для avg_rssi |
| Predicate | Threshold с hysteresis | Custom function от записи |
| Hysteresis | На одной размерности | На трёх независимых |
| Когда подходит | Большинство простых случаев | Корреляция нескольких метрик |

ex09 строится **поверх** инфраструктуры ex08 — те же `lib/trigger.ml`,
`lib/item.ml`, `Mock_source`. Композиция операторов даёт сложную
семантику без расширения библиотечного API.
