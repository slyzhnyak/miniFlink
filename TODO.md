# TODO — miniFlink

Отложенные задачи и идеи. Сгруппированы по темам.
Приоритеты: **[крит]** — нужно для production-deployment'а;
**[опц]** — улучшение или nice-to-have.

## Persistence и recovery

**[крит] Persistence таймеров и per-key state.**
Все stateful операторы держат состояние в памяти и при рестарте
сервиса теряют его. Конкретно затронуты:

- `lib/trigger.ml` — per-key state машины (Ok/Pending_problem/Problem/
  Pending_ok), активные таймеры debounce. Триггер уже находящийся в
  Pending_problem после рестарта забывает свой since-момент и
  начинает debounce заново — теряем 2 минуты ожидания.
- `lib/item.ml` (`silence_age`) — last_seen_ts per key + активные
  tick-таймеры. После рестарта silence обнуляется ложно.
- `lib/pipe.ml` (`process_keyed`) — все его self-timers. Это касается
  ex07 connectivity_alerts FSM напрямую: Self_timer теряется,
  No_packets-таймер не сработает.
- `lib/pipe.ml` (`window_agg_keyed`) — содержимое окон. Без persistence
  закрытые-но-в-allowed_lateness окна теряются, retract'ы потенциально
  опоздавших пакетов перестают эмититься. На рестарте получим
  inconsistent state с downstream.

Нужно:
- Расширить state_backend интерфейс (есть `lib/state_backend_rocksdb.ml`,
  но он не подключен к stateful операторам).
- Сериализовать timers в backend на каждом изменении (или хотя бы
  на checkpoint).
- Восстанавливать на старте оператора.

В `ex06_topology.ml` есть демо checkpoint'а — но в ex07 не интегрирован.
Это та же задача, поэтому делать одним заходом.

**[крит] Checkpoint и recovery в ex07.**
Связано с предыдущим. `lib/checkpoint.ml` существует, ex06 показывает
интеграцию, но ex07 (production-кандидат) её не использует. Нужно:

- Политика checkpoint'ов (интервал, какие операторы участвуют).
- Что делать при несовпадении checkpoint'а и источника (Kafka offset
  vs. сохранённое состояние).
- Тестовый сценарий «убили процесс — восстановили — продолжили».

## Источники и sink'и

**[крит] Реальные Kafka source/sink для ex07.**
Сейчас Mock_source.Default / Large_mine, Mock_sink выводит в stdout.
В коннекторах есть пример (см. `connectors/`), но интеграция в
ex07-пайплайны не сделана. Альтернатива: MQTT, NATS — выбор за
deployment-сценарием.

**[опц] Поддержка backpressure от sink'а к источнику.**
Если sink (Kafka publish) медленный, source должен замедляться.
В `Pipe.channel` есть capacity-блокировка между операторами, но
end-to-end от sink до source пока не сквозная.

## Производительность

**[опц] Multi-dispatcher fan_out для очень быстрых пайплайнов.**
См. Раздел 5 статьи. `connectivity_alerts` упирается в single-thread
dispatcher на 8+ воркерах (peak 2.68x at 4, потом деградация). Решение:
несколько параллельных dispatcher'ов, каждый кормит часть воркеров.
На фактической нагрузке 270 ev/s **не нужно** — но если появится
пайплайн с очень быстрым sequential (>1M ev/s) и плохим scaling,
понадобится.

**[опц] Газовый бенчмарк на полной Large_mine.**
Параллельная версия `gas_alerts` в репозитории есть как
`bench_ex07_parallel`, но измерения именно газовой части не сделаны.
Газовых событий мало (~5% шахтёров), но stress-test с пиком "все
газовые сенсоры одновременно показывают критическое значение" был
бы полезен для capacity planning.

**[опц] Дальнейшее снижение аллокаций в window_agg_keyed.**
Hashtbl-миграция дала -61% (70GB → 27GB), но 27GB за час симуляции
всё ещё много. Дальнейшее ускорение параллельной обработки упирается
в major GC паузы (см. Раздел 5). Кандидаты: pool reusable buffers,
struct types для small records, no-alloc paths для частых случаев.

## Триггерная система (ex08)

**[опц] Раздел в статье про trigger architecture.**
Аналогично разделам про connectivity_alerts/median_rssi/gas_alerts.
Хорошая иллюстрация принципа «выбирай инструмент по форме задачи»
из Раздела 6: FSM для сложной логики, triggers — для деклараций.
~3-4 страницы.

**[опц] Persistence state триггеров.**
Подмножество общей задачи persistence выше. Trigger.of_stream
ведёт Hashtbl<key, state> + last_event_ts + timers list — всё
нужно сохранять.

**[опц] Опциональный refresh внутри Problem-состояния.**
Сейчас триггер sticky: один Data на Problem, retract+Recovery на
выход. Если для конкретной задачи нужен refresh (как в gas_alerts:
обновление при смене ppm >20% или позиции) — можно добавить
опциональный `~refresh_predicate:('v -> 'v -> bool)`, который
решает «эмитить ли refresh при изменении value».

**[опц] Триггеры на агрегированные items.**
Сейчас items в ex08 простые scalar (voltage, gas_co_ppm, silence_age).
Иногда нужны агрегированные: «avg voltage за 5 минут < 3.4» или
«rate of packets per minute». Это `window_agg_keyed` →
`Trigger.of_stream`. В принципе работает уже, но пример в ex08 не
показан — стоит добавить.

**[опц] Конфиг триггеров из файла.**
Сейчас триггеры декларируются в коде (`triggers.ml`). Для оператора
шахты, который хочет менять пороги без перекомпиляции, нужен
yaml/json конфиг. Type-safety теряется, появляется задача валидации.

## Геометрия и калибровка

**[крит для прода] RSSI калибровка на месте.**
Формула `-40 - 20*log10(d)` в `Pipelines.distance_from_rssi` —
для свободного пространства. Реальный тоннель имеет другие
коэффициенты затухания. При вводе шахты в эксплуатацию делается
обход с meter'ом, замеряется RSSI на известных расстояниях,
строится регрессия. Нужно вынести коэффициенты в конфиг (сейчас
в коде).

## Domain features

**[опц] Motion_resumed / Packets_resumed для ex07 connectivity_alerts.**
Сейчас FSM эмитит только Problem-алерты (append-only по дизайну,
см. Раздел 4 статьи). В ex08 это уже сделано через триггеры с
recovery. Если в ex07 захочется тот же effect — нужно расширить
Domain.alert и FSM (или мигрировать в триггеры).

**[опц] Расширение CEP-операторов.**
В прошлых сессиях обсуждался NFA-based CEP (followedBy,
notFollowedBy, oneOrMore). В библиотеке заложен фундамент в
`Temporal`. Полное CEP не реализовано — было бы полезно для
сложных шахтных сценариев типа «движение → молчание → SOS».

## Run-loop и операционная готовность

**[опц] Stationary run-loop.**
Сейчас прогоны примеров и бенчмарков — «прочитать всё, обработать,
выйти». В production сервис работает бесконечно. Нужны:
- Graceful shutdown (см. `lib/shutdown.ml` — частично есть).
- Retry на упавших воркерах (см. `lib/retry.ml`).
- Метрики и health-эндпойнты (см. `lib/health.ml`, `lib/metrics.ml`).
- End-to-end интеграция этого в example-сервис.

## Документация

**[опц] Раздел статьи про триггеры (см. выше).**

**[опц] Tutorial-style getting started.**
Для новых пользователей. Сейчас примеры от ex01 до ex08 идут
crescendo, но без сквозного нарратива. Один документ «начните
здесь, постепенно осваивайте операторы» отсутствует.
