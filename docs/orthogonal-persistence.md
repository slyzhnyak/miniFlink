# Ортогональная persistence

Один и тот же пайплайн работает **с** persistence и **без** неё — без
единого изменения в логике. Persistence и сериализация — решения
деплоя, принимаемые снаружи, а не свойства операторов или пайплайна.

Это аналог Flink managed state: оператор объявляет состояние, а
checkpoint/restore делает runtime единообразно; пользователь про это
не пишет.

## Суть

```ocaml
let pipeline src =
  src
  |> Pipe.window_agg (module K) (Pipe.tumbling 60_000) Agg.(mean speed)
  |> Trigger.of_stream alert_spec

(* без persistence — состояние в памяти *)
let results = pipeline source

(* С persistence — ТОТ ЖЕ pipeline, режим задан снаружи *)
let results =
  Runtime_context.with_context
    (Runtime_context.durable backend)
    (fun () -> pipeline source)
```

Пайплайн идентичен. Разница только в обёртке `with_context` на
вызывающей стороне.

## Два понятия

### `Runtime_context` — политика деплоя

Ambient-контекст (динамический scope через `with_context`), который
решает, как ведёт себя состояние:

- `Runtime_context.ephemeral` — состояние только в памяти, ничего не
  пишется. Это поведение «без persistence» (и дефолт, если
  `with_context` вообще не вызван).
- `Runtime_context.durable ?codecs backend` — состояние снапшотится в
  `backend` (любой `Persistence_backend.t`: in-memory, RocksDB, …) и
  восстанавливается на старте.

### `Managed_state` — состояние оператора

Операторы внутри используют `Managed_state` вместо сырого `Hashtbl`.
Снаружи это обычная keyed-map (`get`/`set`/`remove`/`fold`), но:

- на создании читает ambient-контекст: в ephemeral — память, в
  durable — restore из backend по префиксу имени;
- `checkpoint` / `checkpoint_key` снапшотят в backend (оператор зовёт
  на своём barrier — watermark или per-change); в ephemeral это
  **noop**, поэтому оператор зовёт безусловно — один путь кода в обоих
  режимах.

Пользователь `Managed_state` не видит — это деталь реализации
операторов.

## Сериализация — тоже ортогональна

Сериализация принадлежит runtime-конфигурации, не пайплайну. Оператор
говорит лишь «у меня состояние типа `'v` с именем N». Как N
превращается в байты — решает контекст:

### Уровень 1 — Marshal по умолчанию (нулевая конфигурация)

Состояние всех операторов — замкнуто-свободные данные
(Map/float/list/record/variant), поэтому `Marshal` сериализует их
**автоматически**. Пользователь не пишет ни одного сериализатора.
Идеально для **crash-recovery** (тот же бинарник упал → поднялся:
формат гарантированно совпадает).

```ocaml
Runtime_context.durable backend   (* codecs = Marshal по умолчанию *)
```

Ограничение: Marshal-формат не стабилен между версиями OCaml и при
смене типа состояния — для долговременной schema evolution через
разные деплои используйте Уровень 2.

### Уровень 2 — codec-реестр (для schema evolution)

Сериализаторы по имени состояния, объявленные **на краю деплоя** (не
в пайплайне):

```ocaml
let codecs = Runtime_context.Registry (fun name ->
  match name with
  | "window_fold:tumbling:60000" -> Some my_versioned_codec
  | _ -> None)   (* None → fallback на Marshal *)
in
Runtime_context.durable ~codecs backend
```

Пайплайн в обоих случаях не упоминает сериализацию.

## Namespace

Каждый оператор задаёт стабильное имя (`~name`, или выводимое из
spec). Пользователь его не видит. Ключ в backend —
`"{operator}:{name}:{user_key}"`. Имя стабильно при рестарте, поэтому
restore находит состояние; разные операторы на одном backend не
коллидируют.

## Поддерживающие операторы

Все четыре stateful-оператора на этой модели:

| Оператор | Namespace | Когда checkpoint |
|----------|-----------|------------------|
| `window_fold` (и `window_agg` поверх) | `window_fold:{spec}` | на watermark (batch) |
| `process_keyed` | `process_keyed:{name}` | per-key на изменение |
| `silence_age` | `item:silence_age:{name}` | per-key на изменение |
| `trigger` | `trigger:{name}` | per-key на transition |

Каждый хранит per-key значение, упаковывающее всё нужное для
восстановления (для process_keyed/trigger — включая pending-таймеры,
которые derive'ятся обратно при restore из состояния).

## Что это заменило

Раньше persistence была вплетена в тело каждого оператора: отдельный
`?persistence`-bundle (или у trigger — собственный `?backend` тип с
шестью сериализаторами), ручная JSON-сериализация, `persist_all` /
`restore_all` внутри оператора. Каждый оператор реализовывал один и
тот же restore→process→snapshot заново. Пайплайн с persistence и без
были разным кодом.

Теперь: persistence-машинерии в операторах нет (вынесена в
`Managed_state`), сериализаторы не нужны (Marshal по умолчанию),
пайплайн один на оба режима.

## Связь с exactly-once

`Managed_state` даёт per-operator checkpoint/restore. Это **не** то же
самое, что end-to-end exactly-once (`Checkpoint_parallel.run_exactly_once`),
который добавляет source-offset tracking, Chandy-Lamport barrier
alignment между воркерами и 2PC transactional sink. Это два уровня
гарантий, а не два способа одного и того же; см.
[exactly_once.md](exactly_once.md).
