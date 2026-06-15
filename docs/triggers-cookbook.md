# Сложные триггерные выражения: cookbook

В Zabbix сложные триггеры пишутся inline в DSL:

```
({TRIGGER.VALUE}=0 & {server:vfs.fs.size[/,free].max(5m)}<10G) |
({TRIGGER.VALUE}=1 & {server:vfs.fs.size[/,free].min(10m)}<40G)
```

У нас триггер — это OCaml-значение, и «сложное выражение» строится
через **композицию stream-операторов перед триггером** плюс
condition в самом spec. Эта статья показывает четыре способа
выражения сложной логики на одном realistic сценарии.

## Сценарий

> **Критическая ситуация эвакуации:** алерт по шахтёру M срабатывает
> если он одновременно: получил **повышенный CO** (>50 ppm),
> **батарея падает** (<3.5 В), **средний RSSI за 5 минут низкий**
> (<-75 dBm). Все три условия должны держаться **дольше 1 минуты**
> для дозревания. Алерт срабатывает один раз (sticky), снимается
> когда **любое** из условий перестаёт выполняться.

Это типичный multi-condition trigger из реального мониторинга.
В Zabbix писалось бы так (псевдокод):

```
(last(co_ppm) > 50) and (last(voltage) < 3.5) and (avg(rssi, 5m) < -75)
with problem_for = 1m
```

Разберём как это сделать у нас. Покажу четыре подхода от простого к
гибкому.

## Подход 1: Произвольный predicate через `Trigger.custom`

Если у нас уже есть **один stream** с триплет-значением (CO, voltage,
avg_rssi), можно навесить триггер с custom predicate:

```ocaml
type combined = {
  co_ppm   : float;
  voltage  : float;
  avg_rssi : float;
}

let evacuation_trigger =
  Trigger.create
    ~name:"evacuation_critical"
    ~condition:(Trigger.custom
                  ~problem:(fun c ->
                    c.co_ppm > 50.
                    && c.voltage < 3.5
                    && c.avg_rssi < (-75.))
                  ~recovery:(fun c ->
                    c.co_ppm <= 45.    (* hysteresis на CO *)
                    || c.voltage >= 3.7  (* hysteresis на voltage *)
                    || c.avg_rssi >= (-70.)))  (* hysteresis на RSSI *)
    ~problem_for:(Time.minutes 1)
    ~severity:Trigger.Disaster
    ~produce_alert:(fun ~key ~value ~ts ->
      Evacuation_alert {
        lamp = key;
        co = value.co_ppm;
        voltage = value.voltage;
        rssi = value.avg_rssi;
        ts;
      })
    ~produce_recovery:(fun ~key ~ts ->
      Evacuation_cleared { lamp = key; ts })
    ()
```

Главное здесь — `Trigger.custom ~problem ~recovery`, две произвольные
функции от типа `combined`. **Любая** сложность выражается обычным
OCaml-кодом, не специальным DSL. Компилятор проверяет типы.

**Hysteresis на трёх измерениях.** Recovery срабатывает когда **любое**
условие отпустилось ниже своего hysteresis-порога. Это правильная
семантика: если CO упал — ситуация улучшилась, держать алерт не
надо. **Существенно**: пороги recovery отличаются от пороговx problem:
- CO: problem 50, recovery 45
- voltage: problem 3.5, recovery 3.7
- rssi: problem -75, recovery -70

Иначе мы бы получили flapping в зоне ровно у порога.

**Но** где взять `combined` stream? Это вопрос derived item — следующий
подход.

## Подход 2: Derived item — собрать несколько потоков в один

Чтобы триггер видел триплет, нужно **объединить** три потока в один.
Это типичная задача, и решается через **per-key state**, который
накапливает последние значения каждой компоненты:

```ocaml
let combined_item
    ~(rssi_window : (string * float) Mf_event.t Stream.t)
    ~(voltage     : (string * float) Mf_event.t Stream.t)
    ~(co          : (string * float) Mf_event.t Stream.t)
  : (string * combined) Mf_event.t Stream.t =
  (* объединяем все три потока в один с tagged union *)
  let s_rssi    = rssi_window |> Pipe.map (fun (k, v) -> (k, `Rssi v))    in
  let s_voltage = voltage     |> Pipe.map (fun (k, v) -> (k, `Voltage v)) in
  let s_co      = co          |> Pipe.map (fun (k, v) -> (k, `Co v))      in
  let unioned   = Mf_event.union s_rssi (Mf_event.union s_voltage s_co)  in

  (* per-key state: последние видимые значения *)
  unioned
  |> Pipe.process_keyed
       (module ByLamp)
       ~init:(fun () ->
         { co_ppm = 0.; voltage = 4.0; avg_rssi = 0. })
       ~on_event:(fun ctx key st update ->
         (match update with
          | `Rssi v    -> st.avg_rssi <- v
          | `Voltage v -> st.voltage  <- v
          | `Co v      -> st.co_ppm   <- v);
         ctx.emit (key, { st with co_ppm = st.co_ppm }))
```

**Что делает.** Объединяет три потока в один. На каждое обновление
**любого** из источников эмитирует triplet с актуальными значениями
**всех трёх**. Триггер на этом потоке видит свежий combined каждый
раз когда что-то поменялось.

**Тонкость с watermarks.** `Mf_event.union` берёт минимум watermark'ов
входных потоков. Если один из потоков сильно отстаёт, downstream
блокируется. Для нашего сценария это **правильно**: триггер не должен
эмитировать на устаревших данных.

**Тонкость с инициализацией.** Init-значения (`voltage = 4.0`,
`co_ppm = 0.`) важны: до того как первое значение пришло, триггер
не должен ложно сработать. По умолчанию voltage = 4.0 (норма),
co = 0 (норма), rssi = 0 (сильный сигнал) — всё в безопасной зоне.

## Подход 3: Window-based aggregate для `avg_rssi`

В Подходе 2 я схалявил — RSSI взял как `Rssi v`, но не сказал откуда
этот поток. На самом деле «avg RSSI за 5 минут» это **окно с
агрегатом**:

```ocaml
let avg_rssi_5m
    (rssi_packets : packet Mf_event.t Stream.t)
  : (string * float) Mf_event.t Stream.t =
  rssi_packets
  |> Pipe.flat_map (fun p ->
       List.map (fun (_, dbm) -> (p.lamp, dbm)) p.readings)
  |> Pipe.window_agg_keyed
       ~by:fst
       (Pipe.sliding (Time.minutes 5) (Time.seconds 30))
       Agg.(mean (fun (_, v) -> v))
  |> Pipe.map (fun ((lamp, _wend), avg) -> (lamp, avg))
```

Здесь стандартная композиция: `flat_map` → `window_agg_keyed`
→ `map`. На выходе — поток обновлений `(lamp, avg_rssi)` каждые
30 секунд при закрытии окна. Это и подаётся в `combined_item` как
`rssi_window`.

**Шаг окна 30 секунд.** Это компромисс: меньше — больше эмиссий,
больше latency на close (`window_agg_keyed` эмитирует на закрытии
окна). 30 секунд — пять-десять обновлений в минуту, достаточно для
триггера с `problem_for: 1m`.

**Tumbling vs sliding.** Я взял sliding. Для avg-агрегата это даёт
«движущееся среднее» (moving average) — стандарт для smoothing
шумных данных типа RSSI. Tumbling дал бы независимые 5-минутные
бакеты, каждое обновление с большим скачком.

## Подход 4: Composition of single-condition triggers

Альтернатива всему вышеперечисленному — **не делать** derived item,
а **отдельные** триггеры на каждое условие, и **снаружи** комбинировать
их состояния. Псевдо-код:

```ocaml
let co_high      = Trigger.create ~name:"co_high"      ~condition:(...) ... in
let voltage_low  = Trigger.create ~name:"voltage_low"  ~condition:(...) ... in
let rssi_weak    = Trigger.create ~name:"rssi_weak"    ~condition:(...) ... in

let evacuation_alert (key : string) =
  if Trigger.is_problem co_high      key
  && Trigger.is_problem voltage_low  key
  && Trigger.is_problem rssi_weak    key
  then Some (Evacuation_alert { lamp = key; ... })
  else None
```

**Проблема.** Это **псевдокод**. У нас сейчас `Trigger.of_stream`
возвращает только Mf_event Stream — нет API «спросить current state
для ключа K». Это **именно** то что предлагает Goka через **Views**
(см. `docs/comparison-goka.md`) — read-only API доступа к keyed state.

Без Views Композиция 4 на сегодняшний день **не делается напрямую**.
Можно эмулировать через materialize + cross-stream join, но это
довольно сложно. Подход 1+2+3 **проще** и **работает уже сейчас**.

Это хорошая иллюстрация **зачем нужны Views** — Composition of
triggers становится естественной идиомой.

## Сравнение подходов

| Подход | Декларативность | Сложность реализации | Когда подходит |
|---|---|---|---|
| 1. Custom predicate | Высокая (если есть item) | Средняя (нужен Подход 2) | Логика над одним типом, hysteresis сложный |
| 2. Derived item | Средняя | Средняя (process_keyed) | Multi-stream объединение |
| 3. Window aggregate | Высокая (стандарт) | Низкая (готовые операторы) | Временные функции (avg, max, min) |
| 4. Trigger composition | Высокая | Высокая (требует Views, см. TODO) | Когда уже есть отдельные триггеры |

**В реальном коде** на сегодняшний день для нашего сценария я бы
делал **комбинацию 1+2+3**:

```ocaml
(* Финальная сборка пайплайна *)
let () =
  let module Src = Mock_source.Default in
  let rssi_packets = Src.read () |> Pipe.event_time ~lateness:(seconds 1) in
  let gas_packets  = Src.read_gas () |> Pipe.event_time ~lateness:(seconds 1) in

  (* Подход 3: оконный агрегат для avg RSSI *)
  let rssi_window = avg_rssi_5m rssi_packets in

  (* Простые items для voltage и CO *)
  let voltage_stream =
    rssi_packets |> Pipe.map (fun p -> (p.lamp, p.voltage)) in
  let co_stream =
    gas_packets |> Pipe.flat_map (fun g ->
      match g.g_co with Some v -> [(g.g_lamp, v)] | None -> []) in

  (* Подход 2: derived combined item *)
  let combined_stream =
    combined_item ~rssi_window ~voltage:voltage_stream ~co:co_stream in

  (* Подход 1: триггер с custom predicate *)
  let alerts =
    combined_stream |> Trigger.of_stream evacuation_trigger in

  alerts |> Stream.iter print_alert
```

## Сравнение с Zabbix-DSL подходом

Zabbix-эквивалент:
```
last(/M1,co_ppm) > 50 and
last(/M1,voltage) < 3.5 and
avg(/M1,rssi,5m) < -75
```

Под капотом Zabbix делает **в точности то же самое**: каждое
обращение к item — это lookup в его cache, `avg(5m)` — это
вычисление по value cache за 5 минут. У нас это **explicit** через
window_agg_keyed.

| | Zabbix DSL | miniFlink combinator |
|---|---|---|
| Краткость | Высокая (одна строка) | Средняя (4-5 строк) |
| Type safety | Нет (runtime parse) | Есть (compile-time) |
| Композиция с другими операторами | Нет (DSL изолирован) | Да (можно поставить filter/map до/после) |
| Hysteresis | Через `{TRIGGER.VALUE}` (хак) | Прямо в condition (custom problem+recovery) |
| Multi-stream объединение | Через multi-host expression | Через process_keyed (видно явно) |
| Окна (avg/max/min) | Встроены в выражение | Явный оператор window_agg_keyed |
| Дебаг | Чтение трассы из БД | Стандартные средства OCaml |

**Zabbix компактнее**, потому что много стандартизировано в DSL.
**Наш подход выразительнее**, потому что весь OCaml-язык доступен в
predicate'ах и derived items.

## Когда наш подход избыточно сложен

Если триггер — это **простой scalar threshold** (как 90% случаев в
мониторинге):

Zabbix:
```
last(/server,cpu.load) > 5
```

У нас:
```ocaml
let high_cpu = Trigger.create
  ~name:"high_cpu"
  ~condition:(Trigger.greater_than 5.0)
  ~produce_alert:(fun ~key ~value ~ts -> ...)
  ~produce_recovery:(fun ~key ~ts -> ...)
  ()
```

Объёмнее, чем Zabbix. Но **компилятор проверяет типы**, и
**компонуется** с другими операторами.

## Persistence: триггер переживает рестарт

Все приведённые выше подходы работают и с persistent state — нужно
только подключить backend и сериализаторы в spec'ах.

### Зачем

Без persistence триггер в `Problem` или `Pending_problem` забывает
своё состояние при рестарте сервиса. Конкретные последствия:

- Активный газовый алерт **исчезает** — диспетчер думает что
  обстановка нормализовалась
- Pending_problem с накопленным debounce 90 из 120 секунд
  **начинает заново** — алерт срабатывает на 2 минуты позже чем
  должен
- `last_event_ts` сбрасывается — late events после рестарта
  считаются свежими, могут сделать ложное recovery

Для системы безопасности шахты это **неприемлемо**. Persistence
решает проблему.

### Минимальный пример

```ocaml
(* 1. Spec с сериализаторами *)
let evacuation_spec =
  Trigger.create
    ~name:"evacuation"
    ~condition:(Trigger.custom ~problem:is_critical ~recovery:is_safe)
    ~problem_for:(Time.minutes 1)
    ~severity:Trigger.Disaster
    ~produce_alert:(fun ~key ~value ~ts -> mk_alert key value ts)
    ~produce_recovery:(fun ~key ~ts -> mk_cleared key ts)
    (* Persistence: указать как (де)сериализовать key, value, alert *)
    ~serialize_key:(fun k -> `String k)
    ~deserialize_key:(fun j -> Yojson.Safe.Util.to_string j)
    ~serialize_value:value_to_json
    ~deserialize_value:value_of_json
    ~serialize_alert:alert_to_json
    ~deserialize_alert:alert_of_json
    ()

(* 2. Backend — обёртка над key-value хранилищем *)
let tbl = Hashtbl.create 64 in
let backend = Trigger.backend_of_memory tbl in
(* для production: Trigger.backend_of_rocksdb или ваша обёртка *)

(* 3. of_stream с подключённым backend *)
combined |> Trigger.of_stream ~backend evacuation_spec
```

На каждом изменении state-машины (Ok→Pending_problem,
Pending_problem→Problem, и т.д.) триггер автоматически пишет
snapshot в backend. На старте `of_stream` читает существующие
записи и восстанавливает state, включая pending debounce-таймеры.

### Crash + restart pattern с replayable_source

Для полноценной recovery-семантики нужно не только сохранять
состояние триггера, но и **знать с какого места** перечитать
source. Сейчас в библиотеке есть `lib/replayable_source` — простой
in-memory source с возможностью seek to offset (как в Kafka):

```ocaml
(* Создаём source *)
let src = Replayable_source.of_list events in

(* Phase 1: читаем, периодически коммитим offset в backend *)
let stream, get_offset = Replayable_source.read_from src in
let pipeline =
  stream
  |> Pipe.event_time ~lateness:(Time.seconds 1)
  |> Pipe.map extract_item
  |> Trigger.of_stream ~backend evacuation_spec in
let rec loop () = match pipeline () with
  | None -> ()
  | Some event ->
    handle event;
    (* Каждые N events — commit offset *)
    if !processed mod 5 = 0 then
      backend.set "consumer:offset"
        (Bytes.of_string (string_of_int (get_offset ())));
    loop ()
in loop ()
```

После рестарта:

```ocaml
(* Phase 2: новый процесс. Тот же src, тот же backend.
   Восстанавливаем offset, открываем source с этой позиции. *)
let restored_offset =
  match backend.get "consumer:offset" with
  | Some b -> int_of_string (Bytes.to_string b)
  | None -> 0 in
let stream', _ = Replayable_source.read_from ~offset:restored_offset src in
let pipeline' =
  stream'
  |> Pipe.event_time ~lateness:(Time.seconds 1)
  |> Pipe.map extract_item
  |> Trigger.of_stream ~backend evacuation_spec in
  (* Trigger автоматически подгрузит state из того же backend *)
(* ... обработка продолжается без потерь и дублей ... *)
```

Это **exactly-once-style** semantics после рестарта:
- Source — committed offset гарантирует **no skip, no replay**
- Trigger — persisted state гарантирует **continuation, not restart**
- Один и тот же backend хранит обе вещи синхронно

### Когда нужно / не нужно

**Нужно:**
- Production-deployment системы безопасности (mining, healthcare, finance)
- Длительные debounce'ы (минуты-часы) которые жалко терять при рестарте
- Sticky-alerts критической важности (Disaster, High severity)

**Не нужно:**
- Демо-примеры и тесты (overhead не оправдан)
- Триггеры с очень короткими debounce'ами (<1с — переживание рестарта не важно)
- Stateless обработка (просто threshold без hysteresis/debounce — нечего сохранять)

### Ограничения текущей реализации

Описаны подробно в `docs/trigger-persistence.md`:

- Snapshot **только на state-машинных transitions**, не на каждом event.
  Если упали между двумя event'ами без перехода — теряем
  ≈секундный окно out-of-order tolerance, не критично.
- Только `'k = string` сейчас работает на 100%. Для произвольных
  ключей нужен deterministic serialize_key.
- Нет координации между несколькими триггерами или с другими
  операторами — каждый триггер snapshot'ит свой namespace
  независимо. Atomic multi-operator checkpoint — в TODO.

### silence_age тоже persistent

`Item.silence_age` (главный non-trivial item для триггеров «нет
пакетов дольше N») использует тот же паттерн persistence:

```ocaml
let no_packets_item packets =
  packets
  |> Item.silence_age
       ~backend                              (* тот же что у Trigger *)
       ~backend_name:"no_packets"            (* namespace *)
       ~serialize_key:(fun k -> `String k)
       ~deserialize_key:(fun j -> Yojson.Safe.Util.to_string j)
       ~by:(fun (p : Domain.packet) -> p.lamp)
       ~tick:(Time.seconds 30)
```

Это **важно** для триггеров с silence_age в pipeline'е: без
persistence silence_age сбрасывал бы свой `last_seen` при рестарте,
выдавая `(key, 0)` ложно, что в свою очередь сбивало бы триггер
«нет пакетов 2 минуты» — он бы перезапускал debounce с 0 секунд.

С persistence обе части (silence_age + trigger) восстанавливаются
из **того же backend'а** — конкатенация состояний.

Детали: `docs/silence-age-persistence.md`.




- **Не короче** Zabbix-выражения для простых случаев
- **Гораздо выразительнее** для сложных (multi-stream, derived
  items, окна с произвольными агрегатами)
- **Type-safe** на компиляции
- **Композируется** с любыми stream-операторами

Главная техническая задолженность: **Trigger composition по
ключу** (Подход 4) сейчас требует Views, которые в TODO. Когда они
будут — multi-trigger логика станет идиоматичной.
