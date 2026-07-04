# Expressiveness improvements

Свод улучшений выразительности API. Каждое — самостоятельная фича
с своим тестом и (где применимо) обновлением старых примеров.

## 1. `Pipe.iter_data` family — концизные потребители stream'ов

Часто повторяющийся паттерн «прогнать stream, что-то сделать с
каждым Data» теперь записывается одной строкой вместо
многострочного manual loop'а.

```ocaml
val iter_data : ('a -> unit) -> 'a Mf_event.t Stream.t -> unit
val fold_data : init:'b -> f:('b -> 'a -> 'b) -> 'a Mf_event.t Stream.t -> 'b
val count_data : 'a Mf_event.t Stream.t -> int
val iter_events : ('a Mf_event.t -> unit) -> 'a Mf_event.t Stream.t -> unit
```

`iter_data` — основной helper, проходит [Data] и пропускает
[Watermark]/[Retract]. `iter_events` — escape hatch для случаев когда
нужны все события (тесты, логирование, подсчёт retract'ов).

**Замещает** в существующих примерах (ex08, ex09):

```ocaml
(* раньше — 11 строк boilerplate *)
let rec loop () =
  match stream () with
  | None -> ()
  | Some ev ->
    (match ev with
     | Mf_event.Data _ -> incr total
     | Mf_event.Retract _ -> incr retracts
     | Mf_event.Watermark _ -> ());
    print_event ev;
    loop ()
in loop ();

(* теперь — 6 строк *)
stream |> Pipe.iter_events (fun ev ->
  (match ev with
   | Mf_event.Data _ -> incr total
   | Mf_event.Retract _ -> incr retracts
   | Mf_event.Watermark _ -> ());
  print_event ev);
```

Tests: `test/test_iter_data.ml` (8 сценариев).

## 2. `Pipe.keyed_join` — multi-stream join по ключу

Объединение N streams одного типа в один поток snapshot'ов
«latest values per source for each key».

```ocaml
val keyed_join :
  (module Keyed.S with type t = 'a) ->
  'a Mf_event.t Stream.t list ->
  (string * 'a option list) Mf_event.t Stream.t
```

Семантика:
- Каждое [Data] в любом из входных потоков триггерит emit
- Emit'ится `(key, options)` где `options` — список длины
  `List.length streams` с Some/None per source
- Список сохраняет порядок входных потоков
- [Watermark] объединяются через `Mf_event.union` (min входов)
- [Retract] в входах игнорируется

**Замещает** в ex09 паттерн tagged-union + 3-way `Mf_event.union` +
`process_keyed` с mutable state. ex09's `combined_item` ужался
с ~50 до ~25 строк, исчезли:
- `type update` (3 constructors)
- `module By_update`
- `type combined_state` (6 mutable fields)
- `let make_state`

ex10 — самостоятельный пример демонстрирующий `keyed_join` на
multi-sensor IoT-сценарии.

Tests: `test/test_keyed_join.ml` (13 сценариев).

## 3. Ортогональная persistence — пайплайн не меняется

Все четыре stateful-оператора (silence_age, process_keyed,
window_fold, trigger) не принимают persistence-параметра вообще. Тот
же пайплайн работает с persistence и без; режим задаётся снаружи.

**До** (persistence вплетена в оператор, у каждого свои сериализаторы):
```ocaml
Item.silence_age
  ~backend
  ~backend_name:"no_packets"
  ~serialize_key:(fun k -> `String k)
  ~deserialize_key:(fun j -> Yojson.Safe.Util.to_string j)
  ~by:... ~tick:... source
```

**После** (оператор объявляет только namespace; режим и сериализация —
снаружи):
```ocaml
let pipeline source =
  Item.silence_age ~name:"no_packets" ~by:... ~tick:... source

(* без persistence *)  pipeline source
(* с persistence *)    Runtime_context.with_context
                         (Runtime_context.durable backend)
                         (fun () -> pipeline source)
```

Сериализация ортогональна: Marshal по умолчанию (состояние
closure-free), codec-реестр для schema evolution — всё в контексте,
не в пайплайне. trigger потерял **6** сериализаторов и собственный
backend-тип. Подробно: [orthogonal-persistence.md](orthogonal-persistence.md).

Tests: `test_window_fold_persistence(_e2e)`,
`test_process_keyed_persistence(_e2e)`,
`test_silence_age_persistence(_e2e)`, `test_trigger_persistence(_e2e)`
— каждый показывает `phase1 + phase2 = baseline` через рестарт.

## 4. DSL-раунд 2026-07 (P1–P3) — закрытие разрывов выразительности

По итогам ревью `review-dsl-2026-07.md` (флагманский пример ex07 был
наполовину императивным) добавлены операторы, закрывшие шесть разрывов
выразительности (G-1…G-6). Детальный разбор и мотивация — в ревью и
`dsl-roadmap.md`; кратко:

- **`Pipe.map_ts : ('a -> Time.t -> 'b) -> …`** (G-1) — map с доступом к
  timestamp события; убирает ручной разбор потока по конструкторам, когда
  выход зависит от времени (например конец окна как поле записи).
- **`ctx.emit_retract` / `emit_update` / `emit_event`** (G-4) —
  retract-семантика в keyed-логике. Раньше `process_keyed.emit` умел
  только Data, и retract/update приходилось собирать вручную.
- **`Pipe.co_process2` / `co_process3`** (G-5) — co-обработка нескольких
  разнотипных потоков на общем per-key состоянии. Построены поверх
  `process_keyed` (union + диспетчеризация). Попутно бесплатно закрыли
  G-6 (reactive enrich: обновление side-потока переэмитит активные
  выходы через `emit_event`).
- **`Pipe.Single_timer`** (G-3) — один логический event-таймер на ключ с
  идемпотентным переносом цели; вынесен из ex07 в библиотеку.
- **`Pipe.on_silence`** (G-2a) — детекция отсутствия как первичная
  операция: «признак не наблюдался дольше `within`» → алерт с временем
  перехода, recovery при возобновлении. Late-события не откатывают дедлайн
  (event-time монотонность).
- **`Pipe.suppress_while`** (G-2b) — подавление одного потока алертов
  другим как композиция (а не переплетение FSM).

Плюс unhappy-path: `?on_error` в `process_keyed`/`window_fold` (ядовитое
событие → обработчик/DLQ, не крах), валидация Time/int-параметров всех
конструкторов, ранний warning об окне без watermark.

**Эффект на ex07:** три пайплайна стали декларативными композициями,
pipelines.ml 605 → 457 строк. `gas_alerts` (был ~90 строк ручного
union-цикла) → `co_process3` с тремя доменными обработчиками;
`connectivity_alerts` (был ~150 строк трёх ручных FSM) → композиция трёх
`on_silence` + `Trigger` (гистерезис) + `suppress_while`. Не осталось
ручных `match stream () with`, сентинелов `min_int/max_int` и
признательных комментариев. То же применено к ex11 (presence): ручной
Hashtbl+Queue → `process_keyed` + `materialize`.

Tests: `test_map_ts`, `test_co_process`, `test_single_timer`,
`test_silence_suppress`, `test_on_error`, `test_input_validation`,
`test_no_watermark_warn`; ex07 smoke покрывает все шесть видов алертов +
монотонность late-событий.

## Сравнение

| Улучшение | LoC до | LoC после | Test scenarios | Backwards compat |
|---|---|---|---|---|
| `iter_data` family | 11 (per loop) | 6 (per loop) | 8 | Add-only |
| `keyed_join` (ex09) | ~50 | ~25 | 13 | Add-only |
| `persistence` bundle | 4 params | 1 param | 6 | Yes (`Invalid_argument` on mix) |
| DSL-раунд P1–P3 (ex07) | 605 | 457 | 7 новых сюит | Add-only |

## Что не делалось

Из изначального списка проблем выразительности, **не** делал:
- **Inline `~by:('a -> string)` вместо `(module Keyed.S)`** — для
  generic операторов module-style даёт лучше type-safety, поэтому ядро
  (window, process_keyed) осталось на модулях. Но в high-level
  композиторах inline-ключи появились там, где они естественны:
  `co_process2/3` берут `~key_a/~key_b/~key_c`, `suppress_while` —
  `~controller_key/~suppressed_key`.
- **`Trigger.spec` rebuilding** — 13 параметров звучит плохо, но все
  имеют смысл в production scenario. Documentation решает discoverability
  лучше чем API split.
- **`Stateful.t` (universal abstraction)** — обсуждено в отдельной
  сессии, не было пользы соразмерной cost'у.
- **`Stream.t` low-level combinators** (`Stream.zip`, `Stream.take`,
  `Stream.observe`, etc) — не сделано, low priority. Можно добавить
  отдельным маленьким коммитом если надо.
