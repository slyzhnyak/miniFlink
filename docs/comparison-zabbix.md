# miniFlink triggers vs Zabbix: сравнение и что подсмотреть

Сравнение нашей триггерной системы (`lib/trigger.ml`, `lib/item.ml`,
пример `examples/ex08_triggers/`) с триггерами в Zabbix.

**Главное расхождение в категории продуктов.** Zabbix — это
**монитринговая система** с 20+ годами развития: items, triggers,
actions, escalations, dashboards, templates, discovery, host
inventory, GUI с тысячами screens, role-based access, и так далее.
Триггеры — одна из её центральных абстракций, но окружены большой
инфраструктурой. miniFlink триггеры — это **декларативный
семантический слой** поверх stream processing. Мы построили **ядро**
концепции триггера (item-stream → condition → debounce →
Problem/Recovery event), а не монитринговую систему вокруг неё.

Сравнение поэтому не «кто лучше», а «где мы пересекаемся, где
расходимся, и что из Zabbix-подхода полезно у нас взять».

## Краткое сопоставление

| Аспект | miniFlink Trigger | Zabbix |
|---|---|---|
| Категория | Семантический примитив stream-processing | Полная мониторинговая система |
| Severity levels | 6 (Not_classified/Info/Warning/Average/High/Disaster) | 6 (тех же названий) ✓ совпадают |
| Условие триггера | OCaml-значения с predicate-callback'ами | DSL: `function(/host/key,param)<op><const>` |
| Hysteresis | `less_than_with_hysteresis ~problem ~recovery` | Recovery expression + макрос `{TRIGGER.VALUE}` |
| Debounce | `problem_for` + `recovery_for` (раздельные) | `time_threshold` через `min/max(time, value)` |
| Window functions | Через композицию с `window_agg_keyed` | `last/avg/min/max/count(period)` встроены в выражение |
| Источники данных | Stream-операторы (push) | Items, агенты опрашивают цели (pull) |
| State storage | Per-key Hashtbl в памяти оператора | Server + Postgres/MySQL/Oracle |
| Trigger dependencies | Нет | Есть (mask trigger if parent in Problem) |
| Actions / Notifications | Нет (вывод как Mf_event Stream, downstream sink) | Встроено (email/SMS/script/webhook) |
| Escalations | Нет | Есть (если не подтвердили — следующий уровень) |
| Templates | Нет (триггер — значение в OCaml) | Есть (наборы items+triggers для host groups) |
| Discovery (auto-create) | Нет | Есть (LLD — Low-Level Discovery) |
| Maintenance windows | Нет | Есть |
| Macros (`{HOST.NAME}` etc.) | Нет (доменный alert строит callback) | Есть |
| Out-of-order events | Игнорируются (forward-in-time) | Не релевантно (Zabbix пишет по wall-clock pull-периодам) |
| Type safety | OCaml-компилятор проверяет конструкторы alert | Runtime parsing trigger expression |
| Конфигурация | В коде (значения OCaml) | В БД через GUI / API / YAML импорт |
| Зрелость | MVP (4 примитива conditions) | Production-grade, 20+ лет |

## Что у нас совпадает с Zabbix

### Severity scale (6 уровней, те же имена)

Сознательное решение для **унификации vocabulary**. Если в команде
уже используют Zabbix, наши Warning/Critical/Disaster не требуют
переучивания.

### Hysteresis с разными порогами problem/recovery

Семантика идентичная: в обоих системах для температуры
`problem >20°C, recovery <15°C` записывается явно. Без гистерезиса
дребезг около порога. У нас это `less_than_with_hysteresis
~problem:3.5 ~recovery:3.7`, в Zabbix — recovery expression с
другим порогом.

### Debounce — «condition должен держаться N секунд»

У нас `problem_for: Time.minutes 2` — условие должно держаться 2
минуты прежде чем эмитнется Problem. В Zabbix это через
`min(/host/temp, 2m) > 20` — функция `min` за 2 минуты должна
дать значение > 20 (то есть все измерения в окне > 20). Семантически
то же.

Мы дополнительно различаем `problem_for` и `recovery_for`
(раздельные debounce). В Zabbix через сложные expressions можно
выразить то же, но это менее декларативно.

### Sticky состояние

В обеих системах: триггер один раз перешёл в Problem — остаётся
там до recovery. Не флудит обновлениями на каждом новом значении.

## Что есть у Zabbix, чего нет у нас

### Trigger expression DSL с временными функциями

Zabbix:
```
last(/server,cpu.load) > 5 and avg(/server,cpu.load,15m) > 3
```

У нас:
```ocaml
(* item уже агрегирован через window_agg_keyed; trigger смотрит на
   результат *)
let load_avg_item =
  raw_load_stream
  |> Pipe.window_agg_keyed ~by:host
       (Pipe.sliding (Time.minutes 15) (Time.minutes 1))
       Agg.mean
```

У нас это **композиция операторов**, у Zabbix это **inline в
условии**. Их подход компактнее для простых случаев. Наш — гибче и
лучше композиции (агрегат можно переиспользовать для других
триггеров и для dashboards).

**Стоит ли подсмотреть.** Можно добавить helpers вида
`Trigger.on_window_avg ~by ~window ~threshold` для типичных случаев.
Это **синтаксический сахар** поверх существующего, не новая
семантика. Опц.

### Trigger dependencies

Zabbix позволяет: «если триггер A в состоянии Problem — алерты
триггера B подавляются». Применение: «если хост недоступен по ICMP
— не алертить про CPU/память/диски этого хоста».

У нас этого нет. Для шахтного сценария аналог: «если шахтёр не
шлёт пакеты вообще (No_packets), не алертить про газ от того же
шахтёра (потому что газ-сенсор тоже offline, не значит что газа
нет)».

**Стоит подсмотреть.** Полезно для уменьшения шума диспетчеру.
Сложность: средняя — нужно вести state «какие триггеры сейчас в
Problem», и фильтровать output другого триггера. Опц-сред.

### Actions (что делать при срабатывании)

Zabbix:
```
Action: when trigger 'Low voltage' is Problem
  Step 1 (0 min): email shift_supervisor@mine
  Step 2 (5 min): SMS shift_supervisor
  Step 3 (15 min): call shift_supervisor + email director
```

У нас alert уходит в downstream Mf_event Stream. Что с ним делать —
это **другой сервис** (sink). Это **архитектурный выбор**:
библиотека делает обнаружение, доставка — отдельная подсистема.

**Стоит подсмотреть.** Не библиотечная задача. Если когда-нибудь
будет потребность — отдельный «alert manager» сервис (как
Prometheus AlertManager) — но это вне области stream processing.

### Escalations

Связано с Actions. Не библиотечная задача.

### Maintenance windows

Zabbix: «с 23:00 до 02:00 по субботам — плановые работы, алерты
подавляются».

У нас нет. Можно сделать как **фильтр** между Trigger и sink: «не
пропускать алерты в указанное время». Простая реализация — `Pipe.filter`
поверх trigger output.

**Стоит подсмотреть.** Не библиотечная задача (это конфиг сервиса),
но **паттерн** простой и полезный. Стоит упомянуть в документации.

### Templates

Zabbix template = набор items + triggers, применяемый к группе
хостов. У нас триггер — это значение, переиспользование через
`let` и list. Концептуально похоже, но Zabbix templates это
**конфигурация** для не-разработчиков (через GUI).

**Стоит подсмотреть?** Если когда-нибудь захочется конфиг
триггеров из файла (yaml/json) — то templates это естественная
группировка. Это уже в TODO как «Конфиг триггеров из файла».

### Low-Level Discovery (LLD)

Zabbix умеет автоматически создавать items+triggers по правилам:
«для каждого filesystem из `df` создать item disk_usage и trigger
если >90%». У нас items создаются в коде явно.

**Стоит подсмотреть?** Нет. Это полезно для динамических
инфраструктур (контейнеры, VM); у нас фиксированный набор шахтёров
с известными ID. Над-инжиниринг.

### GUI для настройки

Не библиотечная задача. У Zabbix это центральная фича для
не-разработчиков; у нас — стек разработчиков, всё в коде.

### Macros

Zabbix `{HOST.NAME}`, `{ITEM.VALUE}`, `{TRIGGER.NAME}` — позволяют
писать сообщения алертов с подстановками. У нас доменный alert
строится `produce_alert` callback'ом, который **получает** все
нужные значения как аргументы и сам решает что в них положить.
Гораздо более гибкое и type-safe — но **менее доступное** для
не-разработчиков.

## Что есть у нас, чего нет у Zabbix

### Event-time + watermarks

Zabbix работает по **wall-clock**. Item pollится агентом раз в N
секунд, время измерения — момент опроса. Если данные пришли из
бэкэнда с задержкой (батч из лога) — Zabbix будет считать их «по
текущему времени», а не по их event-time.

У нас триггеры **event-time aware** — late events с устаревшим
значением **игнорируются** (forward-in-time), не вызывают ложных
recovery. Это критично для нашего шахтного сценария с late packets
из mesh-сети.

В шахтных условиях с потерями и retries в mesh это существенный
плюс — Zabbix-подход дал бы ложные алерты.

### Push (stream-based) vs pull (poll-based)

Zabbix-агент **опрашивает** цели по расписанию. У нас items — это
**потоки** обновлений от источника. Pull дешевле для **редко
меняющихся** метрик (диск, CPU); push дешевле и более responsive
для **быстро меняющихся** (телеметрия с сенсоров, события от
устройств).

В шахтном сценарии (RSSI каждые 15с, газ каждые 10с) push
естественнее. Zabbix может работать через trapper-items (push-mode),
но это **не его основной режим**.

### Type safety на стадии компиляции

Декларация триггера компилируется с проверкой:
- Условие применимо к типу item'а
- `produce_alert` возвращает правильный alert-тип
- Имена alert-конструкторов существуют

Опечатка в имени = ошибка компиляции. В Zabbix DSL это runtime
parsing — опечатка в имени item'а или host'а проявится в runtime.

### Композиция в pipeline

Триггер — это **операция** в stream pipeline:
```ocaml
source
|> Pipe.map extract_value
|> Pipe.filter relevant
|> Trigger.of_stream low_voltage_spec
|> Pipe.map to_domain_alert
|> sink
```

Можно ставить **до** или **после** триггера любые stream-операторы.
В Zabbix item → trigger — отдельные шаги, между ними нельзя
вставить custom-фильтр или transformation без выноса в отдельный
preprocessor-step.

### Integration в общий stream processing flow

Триггер сосуществует с другими операторами (`window_agg_keyed`,
`process_keyed`, `gas_alerts`) в одном пайплайне. Можно использовать
FSM-подход для сложной логики (как в ex07 `connectivity_alerts`) и
**одновременно** триггеры для деклараций (как в ex08). Один поток,
один runtime, один источник.

В Zabbix триггер — единственный способ выразить алерт; для
сложной логики приходится писать на Zabbix-DSL вещи которые в
обычном языке программирования выражаются проще.

### Combinator-style декларация

`Trigger.create ~name ~condition ~problem_for ~severity
~produce_alert ~produce_recovery ()` — named arguments, нельзя
забыть параметр, default значения через optional. В Zabbix
конфигурация триггера разбросана по нескольким полям GUI или
импортируется из XML/YAML с runtime-валидацией.

### Out-of-order event filtering

См. выше — late events игнорируются. Архитектурно правильное
поведение для stream'а с возможным reorder'ом.

## Архитектурные различия (важнее чем feature-by-feature)

### Pull vs Push

| | Zabbix | miniFlink |
|---|---|---|
| Источник данных | Агенты опрашивают цели по расписанию | Источник (Kafka/MQTT/UDP) пушит события |
| Когда вычисляется trigger | Каждые 30с (time-based), на каждое значение (non-time-based) | На каждое event-update в потоке + на watermark для дозревания debounce |
| Latency реакции | Минимум период опроса (60s по умолчанию) | Latency сетевой доставки + один pull из stream |
| Подходит для | Стабильная инфраструктура, редко меняющиеся метрики | Высокочастотная телеметрия, событийные источники |

### Standalone vs Library

Zabbix — **самостоятельная система**. Запускается отдельным
сервисом, имеет свою БД, frontend, agents. Интеграция «вокруг»
Zabbix через API/webhooks.

miniFlink — **библиотека внутри** сервиса. Триггеры компилируются в
тот же бинарник что и весь pipeline. Запуска отдельного процесса
нет — это часть существующего stream-processing сервиса.

Это значит:
- Развертывание у нас проще (один сервис вместо стека Zabbix+DB+frontend+agents)
- Но мы **не годимся** для мониторинга нескольких разнородных
  сервисов из одного места (для этого нужна standalone-система типа
  Zabbix или Prometheus+AlertManager)

### Code vs Configuration

Zabbix-подход: триггеры **конфигурируются** через GUI/API. Доступно
не-разработчикам (sysadmins, DBA).

Наш: триггеры **в коде**. Доступно разработчикам, плюс гибкости и
type-safety, минус — каждое изменение требует пересборки.

Это даёт **разные** области применения:
- Sysadmin хочет «алерт если CPU >90% на любом сервере» — Zabbix
- Разработчик stream-processing сервиса хочет «алерт по
  бизнес-правилу на потоке событий» — miniFlink

## Что **стоит** подсмотреть из Zabbix

Из проанализированного, в порядке убывания пользы:

1. **[опц-сред] Trigger dependencies.** «Если шахтёр offline
   (No_packets active) — не алертить про его газ». Уменьшает шум
   диспетчеру. Реализация: вести таблицу «(trigger × key) → state»,
   фильтровать output триггера B если триггер A на том же key в
   Problem. Не сложно.

2. **[опц-низ] Helper-конструкторы для частых case'ов с
   временными окнами.** `Trigger.on_window_avg ~by ~window
   ~threshold` поверх существующего — синтаксический сахар чтобы
   не приходилось каждый раз собирать `window_agg_keyed → Trigger`
   руками. Низкая сложность, средняя польза.

3. **[опц-низ] Документация про maintenance windows как pattern.**
   Это **не библиотечная фича**, это паттерн: фильтр по wall-clock
   между Trigger и sink. Стоит описать в примере или README ex08.

Остальное (Actions, Escalations, Templates, Discovery, GUI, Macros)
— **сознательно не делаем**, это категории standalone-системы
мониторинга, не stream-processing-библиотеки.

## Что у нас **уже лучше** для нашего use case

- Event-time с watermarks — late packets не дают ложных recovery.
- Push-stream нативно — нет lag'а pull-периода.
- Type safety на компиляции — нет runtime-ошибок DSL.
- Композируется с window'ами, process_keyed, FSM из ex07.
- Один сервис, один deployment, без external dependencies.

## Когда что подходит

**Brать Zabbix:**
- Мониторинг инфраструктуры (servers, network, services)
- Pull-style сбор метрик по расписанию
- Не-разработчики настраивают триггеры через GUI
- Нужны Actions/Escalations из коробки
- Нужны Templates для применения к группам хостов

**Брать miniFlink триггеры:**
- Алерты внутри stream-processing сервиса
- High-frequency event streams
- Доменная логика на типизированном языке
- Композиция с другими stream-операторами
- Event-time matters (late events, reorder)
- Один бинарник, deployment как обычный сервис

В реальной шахтной системе **оба** будут использоваться:
- Zabbix мониторит **инфраструктуру** (серверы, БД, сетевые коммутаторы)
- miniFlink триггеры обрабатывают **доменные** алерты по потоку
  телеметрии (Low_voltage, Gas_co_warning, No_packets)

Это не конкурирующие системы, а **дополняющие**.

## Заключение

Zabbix — зрелая мониторинговая система с 20-летней историей,
богатым набором функционала и большой экосистемой. Сравнение
нашего MVP с этим не имеет смысла «лоб в лоб» — мы решаем разные
задачи.

Что важно: мы **сохранили совместимость словаря** (severity scale
6 уровней с теми же именами), что облегчает командам с опытом
Zabbix переход на наши триггеры для **доменной** части мониторинга.

Из Zabbix-подхода полезно подсмотреть **trigger dependencies**
(уменьшение шума через mask-логику) — это добавлено в TODO. Всё
остальное (Actions, Templates, Discovery, GUI) — категория **другого**
продукта (standalone monitoring system), которой мы не являемся.
