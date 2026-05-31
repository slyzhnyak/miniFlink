# Примеры использования miniFlink

Самодостаточные примеры — каждый со своими типами событий, как у
пользователя библиотеки (не на внутренних доменных типах). От простого
к сложному.

| Пример | Что показывает |
|--------|----------------|
| [`ex01_minimal.ml`](ex01_minimal.ml) | Минимум: свой тип, `KEYED`-модуль для ключа, `filter → window → aggregate`. Средняя температура датчиков по окнам. |
| [`ex02_alerts.ml`](ex02_alerts.ml) | `enrich` из справочной таблицы, генерация алертов через `flat_map`, `dedup` с cooldown (подавление дублей). Вход и выход — разные типы. |
| [`ex03_windows.ml`](ex03_windows.ml) | Четыре типа окон на одном потоке: tumbling (время), count (количество), session (паузы активности), global + trigger (по событию). |
| [`ex04_mine.ml`](ex04_mine.ml) | **Комплексная топология** (мониторинг шахты): три источника, `union` (слияние по event-time), `update_table` (таблица порогов из потока конфигурации), `enrich` из таблиц, tumbling + session окна, агрегация, правила, `dedup`. Показывает весь функционал вместе. |

## Запуск

```bash
dune exec examples/ex01_minimal.exe
dune exec examples/ex02_alerts.exe
dune exec examples/ex03_windows.exe
dune exec examples/ex04_mine.exe
```

## С чего начать своё

Минимальный скелет (из `ex01`):

```ocaml
(* 1. свой тип события *)
type reading = { sensor_id : string; celsius : float; ts : int }

(* 2. как извлечь ключ группировки — один раз *)
module Sensor = Keyed.Make (struct
  type t = reading
  let key r = r.sensor_id
end)

(* 3. пайплайн как композиция операторов *)
let pipeline source =
  source
  |> Pipe.filter (fun r -> r.celsius < 200.)
  |> Mf_event.with_watermarks ~latency:(Time.seconds 2)
  |> Pipe.window (module Sensor) (Pipe.tumbling (Time.seconds 10))
  |> Pipe.aggregate (fun key readings -> ...)

(* 4. подать данные *)
Stream.of_list (List.map (fun r -> Mf_event.data r r.ts) data)
|> pipeline |> Stream.to_list
```

Полный справочник операторов — в [API-документации](../docs/api/index.html).
