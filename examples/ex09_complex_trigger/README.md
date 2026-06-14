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

Вывод:

```
=== ex09: multi-condition evacuation trigger ===
Условие: CO>50ppm И V<3.5В И avg_rssi_5m<-75dBm, дольше 1мин

Источник: 2 шахтёра, симуляция 8 минут
  M_critical: voltage 3.8→3.0В, RSSI~-80dBm (далеко), CO 30→100ppm в t=240с
  M_safe:     voltage 3.9В, RSSI -45dBm (рядом), CO 10ppm (норма)

  🆘 M_critical [360000 мс]: CRITICAL evacuation — CO=100 ppm, V=3.17 В, RSSI=-80.0 dBm

Итого: 1 алертов (0 retracts)
Триггер сработал на M_critical когда все три условия дозрели одновременно.
```

Логика:
- voltage M_critical пересекает 3.5 примерно в t=225с
- CO M_critical скачет с 30 до 100 в t=240с (становится > 50)
- avg_rssi M_critical = -80 dBm (стабильно), первое 5-минутное окно
  закрывается в t=300с
- В t=300с все три условия выполнены одновременно
- problem_for = 1 минута дозревает в t=360с → emit alert

M_safe в норме (voltage=3.9В, RSSI=-45dBm, CO=10ppm) → нет алерта.

## Структура

```
ex09_complex_trigger/
  domain.ml          — type alert (Evacuation_critical, Evacuation_cleared)
                        + type combined для triplet значения
  items.ml           — voltage_item, co_item, avg_rssi_item, combined_item
  triggers.ml        — Triggers.evacuation с Trigger.custom predicate
  demo_source.ml     — собственный источник: 2 шахтёра с заданными
                        сценариями (M_critical в опасной зоне, M_safe
                        в норме); 8 минут симуляции event-time
  ex09_complex_trigger.ml  — топология
  dune
```

Demo_source — не модификация ex07's Mock_source, а отдельный
небольшой источник с **детерминированным** сценарием, в котором
триггер заведомо срабатывает. Это даёт живую регрессию полной цепочки
(источник → items → window aggregate → process_keyed merger → trigger),
не зависящую от Mock_source.Default ex07.

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
