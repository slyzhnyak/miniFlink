# Persistence для `Pipe.window_fold`

Reference для `Pipe.window_fold ~backend`. Сценарий, формат
backend-записи, главные отличия от Trigger/silence_age/process_keyed
persistence. Общий паттерн — описан в `docs/trigger-persistence.md`;
здесь только специфика window_fold.

## Зачем

`window_fold` — инкрементальная оконная агрегация с
`O(1)` памятью на окно (vs `O(n)` у `window |> aggregate`). Для
sliding-окон с большим количеством overlap'ов или для общего числа
ключей >1000 это **критично** для production deployment.

Без persistence: после рестарта **все** аккумуляторы окон в зоне
`allowed_lateness` теряются. На запущенный сервис это значит:
- 60-секундное скользящее окно средней voltage прерывается, новый
  start с нуля
- Late events, прибывающие через 30 секунд после рестарта,
  попадают в **новые** окна вместо тех в которые они должны были
  попасть → retract'ы не происходят, downstream видит fictitious
  windows
- 4-минутные moving averages для тренд-анализа сбрасываются на
  каждом deploy

С persistence: окна `FOpen` продолжают накопление с того места где
crash'нулись; окна `FFired` сохраняют свой acc для **корректного**
retract+re-emit на late events.

## API

```ocaml
val window_fold :
  (module Keyed.S with type t = 'a) ->
  ?latency:Time.t ->
  ?allowed_lateness:Time.t ->
  ?backend:Persistence_backend.t ->
  ?backend_name:string ->
  ?serialize_acc:('acc -> Yojson.Safe.t) ->
  ?deserialize_acc:(Yojson.Safe.t -> 'acc) ->
  win_spec ->
  init:(unit -> 'acc) ->
  add:('acc -> 'a -> 'acc) ->
  'a Mf_event.t Stream.t -> (string * 'acc) Mf_event.t Stream.t
```

С backend'ом **три** дополнительных параметра обязательны:
- `?backend_name` — namespace
- `?serialize_acc` / `?deserialize_acc` — для пользовательского `'acc`

Ключ всегда `string` (через `Keyed.S.key`).

## Формат backend-записи

### Ключ

```
"window_fold:{backend_name}:{user_key}:{start}:{stop}"
```

**Каждое окно** имеет свою запись в backend (не «один ключ — много
окон»). Для sliding(60s, 15s) с 100 шахтёрами и допуском lateness =
30s — это до 1000 одновременных записей. Это **цена** оконного
state'а: окна независимы и каждое снапшотится отдельно.

Пример для `backend_name="voltage"`, lamp `"M_critical"`, окна
`[180s, 240s)`:

```
window_fold:voltage:M_critical:180000:240000
```

### Значение

JSON с тремя полями:

```json
{
  "state":    "open" | "fired",
  "acc":      <serialized 'acc>,
  "nonempty": bool
}
```

- `state` — `"open"` или `"fired"` (FFired = окно эмитнуло, но в
  зоне `allowed_lateness` ещё реагирует на late events)
- `acc` — пользовательский аккумулятор через `serialize_acc`
- `nonempty` — был ли хотя бы один `add` (FOpen с пустым acc не
  fire'ится — нужен флаг)

## Когда происходит запись

**Только на каждом watermark'е**. Это **главное** отличие от других
persistent операторов:

| Оператор | Когда snapshot |
|---|---|
| `Trigger` | На transitions (4-state machine) |
| `silence_age` | На каждое event + каждый tick |
| `process_keyed` | После каждого on_event/on_timer |
| **`window_fold`** | **На каждом watermark** |

### Почему именно watermark

Watermark в event-time processing — это **естественный checkpoint
barrier**. Когда watermark = T, гарантируется что:
- Все events с ts ≤ T обработаны (модулo allowed_lateness)
- Все окна со stop + latency ≤ T fire'ены
- Все окна со stop + latency + allowed_lateness ≤ T удалены

Это **в точности** правильный момент для consistent snapshot всех
окон. Snapshot'ить на каждом event'е дал бы:
- 10× больше I/O на sliding-окнах (каждое event попадает в 10
  окон)
- Inconsistent snapshots (event приземлился в FOpen окно но
  watermark ещё не сдвинул FFired окна с тем же ключом)
- Зря — между watermark'ами downstream всё равно не получает
  результатов

Поэтому **watermark = snapshot tick**. Просто и эффективно.

### Backend cleanup

`persist_all` синхронизирует backend с tbl:
1. Для каждой записи в tbl — `backend.set` с текущим state'ом
2. Для каждой записи в `backend.keys()` под нашим prefix'ом — если
   её **нет** в tbl, `backend.delete` (окно ушло из tbl потому что
   FFired-and-beyond-lateness)

Это значит backend всегда отражает текущий tbl точно — нет
утечки старых FFired записей.

## Восстановление при startup

`Pipe.window_fold ~backend ...` вызывает `restore_all`:
1. `backend.keys ()` → список всех ключей
2. Для каждого, если начинается с `"window_fold:{backend_name}:"`:
   - Parse remaining как `{user_key}:{start}:{stop}` (последние два
     `:`-separated токена — int'ы start и stop, остальное — user_key)
   - `backend.get` → JSON bytes
   - Parse JSON → `(state, acc, nonempty)` tuple
   - `Hashtbl.replace tbl (user_key, start, stop) (FOpen/FFired with acc)`

После restore оператор готов обрабатывать новые events: open окна
продолжают накапливать, fired окна корректно реагируют на late
events.

## End-of-stream semantics

Особый случай: **что делать когда upstream возвращает `None`?**

**Без backend** (default): пройти по всем FOpen окнам и fire их
как final flush. Это правильно для batch processing: пользователь
видит результаты всех окон, даже неполных.

**С backend**: НЕ делать final flush — оставить open окна в backend
как есть. Логика: если backend подключён, мы рассчитываем что
upstream может вернуться (recovery scenario), а не «упорядоченный
shutdown».

Это сделано чтобы тест crash mid-stream был корректным: stream
заканчивается на cutoff watermark, но open windows должны
**остаться в backend**, чтобы Phase 2 их подхватил.

В production это означает: если деплоймент с persistence
завершается graceful'но (`SIGTERM` + clean shutdown), open windows
не fire'ятся — они переживут до next deploy. Это **подходящее**
поведение для production — мы не хотим терять данные in-flight.

Если нужен financial-style «закрыть всё что недозакрыто на финале»
— использовать `?backend=None` для этого pipeline'а или послать
финальный `wm = max_int` перед shutdown.

## Отличия от других операторов

| Аспект | Trigger | silence_age | process_keyed | window_fold |
|---|---|---|---|---|
| Тип ключа | `'k` | `'k` | `string` | `string` |
| Сериализаторы ключа | да | да | нет | нет |
| Записи per ключ | 1 | 1 | 1 | **много** (по одной на окно) |
| Когда пишет | На transitions | Event + tick | Event + on_timer | **На watermark** |
| End-of-stream без backend | flush | flush | warn | final flush |
| End-of-stream с backend | flush (same) | flush (same) | warn (same) | **leave in backend** |

`window_fold` единственный с записью **per окно** (vs per ключ) и
со специальным end-of-stream поведением.

## Ограничение: `Pipe.window_agg` без persistence

`Pipe.window_agg` и `Pipe.window_agg_keyed` — sugar поверх
`window_fold` с `Agg.t` (комбинируемые агрегаторы). **Persistence
для них напрямую невозможна** потому что `Agg.t` имеет
**экзистенциальный** тип аккумулятора:

```ocaml
type ('a, 'r) Agg.t  (* 'acc скрыт *)
```

Пользователь видит только вход `'a` и результат `'r`, но не
`'acc`. Поэтому он **не может** дать `serialize_acc`.

**Workaround:** использовать `window_fold` напрямую где нужна
persistence в окнах:

```ocaml
(* Вместо: *)
stream |> Pipe.window_agg_keyed ~by ~backend ... spec (Agg.mean f)

(* Использовать: *)
stream |> Pipe.window_fold ~by ~backend ... spec
  ~init:(fun () -> (0., 0))
  ~add:(fun (s, n) v -> (s +. f v, n + 1))
|> Pipe.map (fun (k, (s, n)) -> (k, s /. float_of_int n))
```

Это **теряет** удобство `Agg.mean / Agg.both / ...` но **получает**
persistence.

**Future:** расширить `Agg.t` с опциональными persistence hooks per
встроенный агрегатор (`count` → `int`, `sum` → `float`,
`mean` → `(float, int)`, и т.д.). Это refactor `lib/agg.ml`,
отдельный TODO. После этого `window_agg` тоже станет полностью
persistent.

## Тесты

`test/test_window_fold_persistence.ml` (5 unit-сценариев):
1. Без backend — regression (sum window emits identically)
2. С backend — backend получает запись со state=open, acc=12,
   nonempty=true
3. Backend без backend_name → `Invalid_argument`
4. Restore open window: phase 1 acc=15 в backend (окно не закрыто).
   Phase 2 добавляет 25, закрывает → emit (Y, 40), не 25.
5. Restore FFired: late event прибывает через рестарт,
   retract старый + emit новый. FFired state корректно сохранён.

`test/test_window_fold_persistence_e2e.ml` (E2E на Mock_source):
- sliding(60s, 15s), `allowed_lateness=30s`
- Phase 1: events до t=180с, backend хранит mix FOpen и FFired
- "Crash"
- Phase 2: новый window_fold с тем же backend
- **Invariant per key**: `phase1_emits + phase2_emits = baseline_emits`
- 132 = 52 + 80 (8 шахтёров × ~25 окон)

## Связь с другими модулями

Полная end-to-end recovery в пайплайне с окнами требует persistence
**всех** stateful операторов:

```
events → window_fold (persisted)
       → process_keyed (persisted)
       → Trigger (persisted)
       → alerts
            ↑                       ↑                ↑
            └────── один backend ────┴──────────────┘
```

Все четыре stateful оператора могут делить один backend — их
префиксы (`window_fold:`, `process_keyed:`, `item:silence_age:`,
`trigger:`) не коллидируют.

После всех закрытых пунктов persistence в текущем сезоне:
minePASS production deployment может пережить рестарт без потери:
- Текущих окон с агрегатами (window_fold)
- FSM-состояний шахтёров (process_keyed)
- last_seen timestamps (silence_age)
- Pending_problem state триггеров (Trigger)

Это и есть **production-grade fault tolerance**.

## Будущее

Кроме упомянутого refactor'а `Agg.t`:
- `Pipe.count_window`, `Pipe.session_window`, `Pipe.global_window` —
  не в scope изначального TODO. Та же схема (Persistence_backend +
  JSON) применима если понадобится.
- Atomic multi-operator checkpoint coordination — сейчас каждый
  оператор snapshot'ит независимо в свой namespace. Для строгого
  atomic recovery нужен координированный checkpoint barrier поверх
  всех операторов. Это **Flink-style** checkpoint algorithm —
  более сложный и (пока) не нужен для текущих use cases minePASS.
