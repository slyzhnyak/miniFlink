# ex10 — Multi-stream join по ключу через `Pipe.keyed_join`

Демонстрация оператора `Pipe.keyed_join` на простом IoT-сценарии:
объединение трёх sensor-потоков (температура, влажность, давление)
в единый поток snapshot'ов "последние значения каждого сенсора по
станции".

## Что показывает

```
[temperature]  ─┐
[humidity]    ──┼─→ keyed_join (by station_id) ─→ snapshots
[pressure]    ──┘
```

Каждый snapshot имеет вид `(station_id, [temp_opt; humidity_opt; pressure_opt])`.

## Контраст с ex09

`ex09.combined_item` решает ту же задачу для voltage/CO/RSSI через:
- Tagged union type `update = U_voltage | U_co | U_rssi`
- 3-way `Mf_event.union` nesting
- `Pipe.process_keyed` с mutable state и manual case-analysis
- ~30 строк кода

`ex10` делает то же одной строкой:

```ocaml
let joined = Pipe.keyed_join (module By_station)
               [temp_stream; humidity_stream; pressure_stream]
```

## Запуск

```
dune exec examples/ex10_keyed_join/ex10_keyed_join.exe
```

## Семантика join'а

- Каждый `Data` event в любом из входных потоков триггерит emit.
- Snapshot содержит **последнее** значение каждого канала для ключа
  (`None` если канал ещё не присылал данные для этого ключа).
- Watermarks: минимум входов (через `Mf_event.union`).
- `Retract` в input streams игнорируется.

## Когда использовать

Use case: multi-sensor data fusion, корреляция событий из разных
источников, real-time dashboards "current state per entity".

Не подходит когда нужна **строгая** синхронизация по event-time
(использовать `Pipe.window_*` для batched join) или когда каналы
имеют **разные** типы значений (использовать `Pipe.process_keyed`
напрямую с tagged union — это случай ex09).
