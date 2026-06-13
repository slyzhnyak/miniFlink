# miniFlink vs Goka: что полезного можно подсмотреть

Сравнение архитектуры двух библиотек stream processing с акцентом на
то, что у Goka сделано иначе и что из этого стоит привнести в
miniFlink.

**Goka** ([lovoo/goka](https://github.com/lovoo/goka)) — Go-библиотека
для распределённой stream-обработки поверх Apache Kafka. Goka
расширяет концепцию Kafka consumer groups, связывая с группой
«group table» — partitioned key-value-таблицу, хранимую в самой Kafka.

**miniFlink** — single-node OCaml-библиотека с event-time, окнами,
retract'ами, и pluggable source/sink (Kafka не обязателен).

## Краткое сопоставление

| Аспект | miniFlink | Goka |
|---|---|---|
| Транспорт | Pluggable (Kafka — один из вариантов) | **Только Kafka** |
| Deployment | Single-node (с параллелизмом через `Domain.spawn`) | **Distributed**, auto-rebalancing |
| State storage | RocksDB / memory (`state_backend_*`), но не интегрировано в operator'ы | LevelDB (по умолчанию) + Redis/memory как плагины, **автоматически** интегрировано через group table |
| Persistence таймеров | Открытый пункт TODO | N/A (у Goka таймеров нет) |
| Recovery | Manual (есть `Checkpoint`, но не подключён к ex07) | **Автоматический** — processor восстанавливает group table из Kafka topic |
| Event-time | First-class (`Mf_event.t`, watermarks, allowed_lateness) | Только processing-time, `ctx.Timestamp()` доступен но окон с event-time нет |
| Окна | Tumbling / sliding / session / count / global+trigger | Нет встроенных окон |
| Retract | First-class (`Mf_event.Retract`) | Нет — log-compaction скрывает старые значения |
| Self-timers | Event-time + processing-time, через `process_keyed` | Нет (heartbeat делается отдельным processor'ом) |
| Триггеры | `lib/trigger.ml` (Zabbix-style декларации) | Нет аналога |
| DSL | Pipeline через `|>` (combinator-style) | Императивные callback'и на event |
| Read API к state | `Pipe.materialize` (только на одном узле) | **Views** — гRPC-friendly read-only кеш group table |
| Lookup vs Join | Только `Pipe.enrich` (broadcast lookup) | Различение **Join** (co-partitioned) vs **Lookup** (cross-table) |

## Что у Goka есть, что стоит подсмотреть

### 1. Views — read-only доступ к group state снаружи

**Что это.** Goka-View — это локальный read-only кеш одной group
table. Любой другой сервис (или REST/gRPC endpoint) подключает View
и получает доступ к актуальному состоянию по ключу: «дай мне
последний alert для шахтёра M1». View **автоматически** обновляется
из Kafka topic группы.

**Зачем это нужно.** В реальном развёртывании пайплайн —
не единственный потребитель state. Дашборд диспетчера, мобильное
приложение начальника смены, отчётная подсистема — все хотят знать
«сколько шахтёров сейчас в работе», «какие активные газовые
тревоги», «у кого батарея низкая». Без Views внешние системы
должны:
- либо подписываться на поток алертов и сами материализовывать
  таблицу состояний,
- либо ждать пока пайплайн опубликует snapshot куда-то ещё.

С Views это становится first-class: тот же state, который сервис
использует внутри, **сразу** доступен снаружи через единый API.

**Что у нас сейчас.** `Pipe.materialize` сворачивает retract'ы в
финальную таблицу, но это **на стороне sink**, на одном узле, без
API доступа. Нет concept'а «здесь живёт state, любой потребитель
может на него подписаться».

**Сложность реализации.** Средняя. Нужно:
- Вынести keyed state из operator'ов в именованный реестр.
- API подписки на изменения (changelog).
- Optionally — gRPC/HTTP обвязка (за пределами библиотечного слоя,
  как и Goka делает не сама — она даёт API, gRPC прикручивается
  пользователем).

**Польза.** Высокая для production-deployment'а. Особенно для шахтных
сценариев, где диспетчерский экран — фактический потребитель.

### 2. Group Table как явная first-class абстракция

**Что это.** В Goka пользователь **явно объявляет** что его processor
имеет одну group table такого-то типа. Эта таблица:
- partitioned по тому же ключу что входной поток,
- persisted в Kafka topic (log-compacted),
- читается через `ctx.Value()` / пишется `ctx.SetValue()` в
  callback'е,
- доступна другим processor'ам как **Lookup**,
- доступна снаружи как **View**.

**Что у нас сейчас.** Keyed state есть внутри `process_keyed`,
`window_agg_keyed`, `Trigger`, `silence_age`, `gas_alerts` — но это
**внутренние** структуры данных каждого operator'а. У них нет
общего реестра, нет единого способа их сериализовать, нет API
снаружи.

**Что выиграем.**

- **Observability.** Прицельный мониторинг: размер каждой group
  table, hit rate, время последнего изменения. Сейчас это data
  невидима.
- **Persistence по умолчанию.** Если group table — first-class,
  легко привязать её к state_backend и checkpoint автоматически.
- **Recovery.** Аналогично — на restart восстановили из backend.
- **Migration / admin operations.** Можно дать команду «удалить
  всё содержимое group table для шахтёров уволенных вчера».

**Сложность.** Средняя-высокая. Затрагивает API всех stateful
operator'ов. Но направление совпадает с уже планируемой «Durable
state» (см. README TODO).

### 3. Различение Join vs Lookup

**Что это.** Goka явно различает:
- **Join** — соединение с **co-partitioned** таблицей (тот же ключ,
  то же количество partition'ов). Локальный, быстрый, без сетевого
  доступа к Kafka — данные таблицы уже на этом instance.
- **Lookup** — соединение с **не-co-partitioned** таблицей. Полная
  копия таблицы на каждом instance (через View), потому что нужный
  ключ может быть в любом partition.

**Что у нас.** Единственный enrichment-оператор — `Pipe.enrich`,
который семантически всегда **Lookup** (полная таблица доступна
оператору). Co-partitioned join не реализован.

**Что бы выиграли.**

Для нашего gas_alerts мы вручную реализовали temporal-left-join
трёх потоков. Если бы был встроенный оператор co-partitioned join,
газовый пайплайн писался бы декларативно.

В Section 3.3 статьи я писал про **«hand-rolled stateful stream»**
для gas_alerts — отчасти это из-за отсутствия такой абстракции.

**Сложность.** Средняя. Семантика join'а с retract-ами + watermarks
не тривиальна.

### 4. Visitor для итерации по всему state

**Что это.** Goka даёт API `processor.VisitAllWithStats` — пройти
по всем ключам group table во время работы processor'а. Помечено
как EXPERIMENTAL.

**Зачем.** Админские операции: миграция формата state, аудит,
очистка по custom-фильтру (TTL по чему-то нестандартному).

**Что у нас.** Нет вообще. Если хочется очистить state по custom
правилу — рестарт сервиса и надежда что новая логика правильная.

**Сложность.** Низкая.

### 5. Подтверждение наших уже-планируемых направлений

Goka делает следующее **автоматически**:
- State persisted в Kafka topic — нет потерь при рестарте.
- Pluggable storage layer — можно подменить LevelDB на Redis.
- Recovery из state on restart.

У нас всё это **в TODO** (Приоритет 6 «Durable таймеры»). Goka
подтверждает что это правильное направление и **must-have** для
production.

## Что у нас лучше / есть, у Goka нет

Для полноты — несколько мест где наше архитектурное решение **более
богатое**:

### Event-time + watermarks

Goka работает в **processing-time** (порядок Kafka topic). У нас
event-time — first-class. В шахтном сценарии с late packets
(reorder из mesh) processing-time дал бы неверные алерты
(`No_packets` сработал бы пока запоздавший пакет уже в пути).

### Окна (tumbling/sliding/session/count/global+trigger)

В Goka окон **нет**. Хочешь окно — пиши state-машину в callback'е
вручную. У нас 5 типов окон + `Agg` для композиции агрегатов.

### Retract-семантика

В Goka обновление state — это **запись нового значения**,
log-compaction в Kafka topic скрывает старое. Внешние подписчики
получают «новое значение пришло, считай его актуальным». Это
неявная семантика.

У нас retract — **явное** событие. `Pipe.materialize` сворачивает
явно. Раздел 4 статьи разбирает почему это лучше для нашего
сервиса.

### Process_keyed с event-time таймерами

В Goka таймеров **нет**. «Heartbeat если шахтёр молчит 2 минуты»
делается через отдельный processor с регулярным emit. У нас
self-timers — first-class в `process_keyed`.

### Триггерная система (ex08)

Чисто наша абстракция. Декларативный синтаксис вида
`Trigger.create ~name ~condition ...` у Goka нет.

### Combinator-style pipeline DSL

```ocaml
source
|> Pipe.dedup ~rule:...
|> Pipe.flat_map ...
|> Pipe.event_time ~lateness:...
|> Pipe.window_agg_keyed ...
|> Pipe.map ...
```

vs Goka:

```go
goka.NewProcessor(brokers,
  goka.DefineGroup(group,
    goka.Input(topic, codec, func(ctx Context, msg interface{}) {
      // imperative handler
    }),
    goka.Persist(codec),
  ))
```

Combinator DSL читается как **описание трансформации**. Goka
читается как **императивный код**.

### OCaml type-safety

Goka в значительной мере работает через `interface{}` (Go generics
появились в 1.18, но Goka API на момент основной разработки этого
не использовал — codec'и encode/decode через interface). Мы через
систему типов OCaml ловим многое на компиляции.

## Что заимствовать НЕ надо

Несколько вещей Goka делает специфично под Kafka, и их перенос в
miniFlink противоречит нашим non-goals.

### Привязка к Kafka

Goka **без Kafka не работает** — это центральная зависимость. У
нас Kafka — один из возможных source/sink, не обязательный. Это
архитектурный выбор который менять не надо: библиотека должна
работать с любым transport'ом, Kafka подключается как опция.

### Distributed deployment

Goka из коробки умеет multi-node deployment с auto-rebalancing
через Kafka consumer groups. У нас single-node + параллельность.
Distributed — отдельная категория сложности, в наших non-goals
явно указано «not distributed».

### Log-compacted topic как state storage

Goka использует Kafka topic с log-compaction как primary storage
для group table. Это **элегантно** в Kafka-centric архитектуре —
одно и то же место для event log и state. Но переносить идею
дословно к нам не имеет смысла — мы не привязаны к Kafka.
**Альтернативный паттерн** (он есть): write-ahead log в state_backend.

## Рекомендации для добавления в TODO

Из проанализированного, в порядке убывания пользы:

1. **[опц-выс] Views — read-only API доступа к group state.**
   Главное полезное приобретение. Открывает возможность интеграции
   с внешним world (web UI, мобильные клиенты, отчётные системы)
   без подписки на потоки. Добавить в TODO как новый пункт в
   приоритете 6 или 7.

2. **[опц-сред] Group Table как явная first-class абстракция.**
   Improvement DX и observability. Согласуется с уже планируемым
   «Durable таймеры». Можно делать вместе.

3. **[опц-сред] Co-partitioned Stream-Stream Join как библиотечный
   оператор.** Generalization того что мы вручную делаем в gas_alerts.
   Когда появится второй use case — будет неприятно писать опять
   вручную.

4. **[опц-низ] Visitor для итерации state.** Низкая сложность,
   средняя польза для эксплуатации.

Все четыре пункта **не критичны** для текущего ex07 — газовая
система работает. Это **улучшения для production-deployment'а и
DX** на будущее.

## Заключение

Goka и miniFlink решают **разные** задачи:
- **Goka**: distributed stateful processing поверх Kafka с упором на
  scalability и fault-tolerance через replication state в Kafka.
- **miniFlink**: single-node event-time processing с богатой
  семантикой окон, retract'ов и таймеров.

Goka **проще** при условии что у вас уже Kafka и хочется быстро
построить stateful service с auto-recovery. У нас **выразительнее**
семантика (окна, late events, retracts), но distribution и
auto-recovery надо строить руками или интегрировать с external
state-backend.

Главное что стоит подсмотреть — **Views** и **Group Table как
явная абстракция**. Эти две вещи не противоречат нашему дизайну, а
дополняют его — making state visible to the outside world without
forcing the outside to consume the event stream.
