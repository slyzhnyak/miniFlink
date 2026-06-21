# Глубокая инспекция miniFlink — июнь 2026 (раунд 3)

Третий заход. Фокус на областях не покрытых раундами 1-2:
variants/parallel (реальная многопоточность), connectors/kafka
(включая C FFI), schema-кодек, table/temporal (рост памяти),
shutdown.

## Резюме раунда 3

Найдено и исправлено **6 проблем** + удалён мёртвый код. Самая
серьёзная — GC-safety баг в C FFI (kafka_stubs.c), который мог
давать редкую порчу кучи под реальным librdkafka. Плюс два
unbounded-memory-growth в Kafka source и temporal join.

---

## ИСПРАВЛЕНО

### #5 — Deadlock: sink под mutex в parallel_v4 collector
**parallel_v4.ml** (commit d67082b)

Collector-поток вызывал пользовательский sink под общим mu_sink
без exception safety. v5 уже имел защиту (try/with → unlock), v4 —
нет. Бросок sink'а → deadlock всех collector-потоков. Fix:
safe_sink helper (как в v5).

### #14 — Мёртвый код: run_parallel в parallel_v4
**parallel_v4.ml** (commit 557aeb5)

run_parallel (collector-вариант, 122 строки) не в mli, нигде не
вызывается, отсутствует в v5 (доказательство неиспользования —
сборка под OCaml 5 на v5 компилируется без него). Ранняя версия,
забытая при рефакторинге на run_parallel_simple. Удалён; v4 теперь
симметрична с v5.

### #15 — Unbounded memory: Kafka snapshots
**kafka_source.ml** (commit 4e77d4f)

snapshots Hashtbl записывался на КАЖДОЕ прочитанное событие и
никогда не обрезался — утечка у long-running consumer'а. Добавлен
prune_snapshots_before ~before для обрезки снимков ниже
подтверждённого checkpoint.

### #16 — Тихая потеря данных: schema version overflow
**schema_default.ml** (commit 09c66d7)

version пишется в 2 байта (uint16). При version > 65535 старшие
биты молча терялись, decode читал другую версию → рассинхронизация
схемы. Fix: guard [0, 65535] в Schema.make → Invalid_argument на
этапе создания codec'а.

### #17 — Типобезопасность FFI: Obj.t handles
**kafka_rdkafka.ml** (commit ff880c4)

librdkafka-биндинг типизировал rd_kafka_t как Obj.t — стирает
различие consumer/producer хэндлов, компилятор не поймал бы
передачу producer'а в consumer_poll (UB в C). Заменён на abstract
type handle. C-сторона не тронута (custom block уже корректен).

### #18 — GC safety: незарегистрированные value в C FFI
**kafka_stubs.c** (commit 46bfd68)

В caml_rdk_consumer_poll (горячий путь) промежуточные value k и
payload объявлены обычными C-локалями, не CAMLlocal. caml_alloc_*
между их созданием и Store_field может триггернуть GC, который
переместит незарегистрированные блоки → в результат попадёт
висячий указатель. Редкая порча кучи. Fix: CAMLlocal3 → CAMLlocal5,
k и payload через rooted-локали. Остальные стабы проверены —
безопасны.

### #20 — Unbounded memory: temporal version history
**temporal.ml** (commit a1bba89)

Versioned table накапливала версию на каждый put_version per ключ,
без обрезки — утечка у long-running temporal join. Добавлен
prune_versions_before ~before (сохраняет версии >= before плюс
одну предшествующую для границы). Не обрезается слепо, т.к. версии
нужны для as_of опоздавших main-событий.

---

## ПРОВЕРЕНО — корректно (не трогал)

- **shutdown_default.ml**: образцовая async-signal-safety. Handler
  делает только Atomic.set; лог/on_shutdown отложены в основной
  поток (Mutex/printf в signal handler = deadlock/UB). Подробно
  документировано. Зрелее многих production-систем.
- **barrier_default.ml**: намеренно референсный, НЕ в рабочем
  пути, с честным комментарием о known issues (сброс ready,
  упавшие воркеры). Не трогаем.
- **table.ml**: exhaustive по Mf_event, Update обработан, of_stream_ttl
  для bounded памяти, документированы pump/Retract. Рост of_stream
  при растущих ключах — уже документирован с решением (ttl-вариант).
- **temporal join logic**: watermark-gated буферизация main до
  wm_upd, детерминизм по event-time. Корректно.
- **schema decode**: границы байтов корректны (length<2 проверка,
  Bytes.sub валиден), unknown version → explicit Error.
- **kafka produce/conf стабы**: String_val до OCaml-аллокаций,
  RD_KAFKA_MSG_F_COPY — GC-safe, документировано.

---

## Вывод (раунды 1-3)

Три захода. Раунд 1: функциональные находки (P1-P4, недоделки,
legacy API). Раунд 2: 4 бага в concurrency/IO ядра
(mutex-deadlock, fd-leak, div-by-zero). Раунд 3: 6 проблем в
variants/connectors/FFI (deadlock, 2× memory-leak, schema overflow,
Obj.t, GC-safety) + мёртвый код.

Паттерн: семантическое ядро (Stream, операторы, watermark, join'ы)
— зрелое и корректное во всех трёх раундах. Баги концентрировались
в (а) concurrency/IO обвязке с неполной обработкой исключений и
(б) unbounded-memory у long-running компонентов. C FFI имел
классический GC-safety баг.

Библиотека прошла основательный аудит и готова для production-
приложения. Оставшиеся отложенные пункты (P3 warnings, P4
структура) — косметика без функционального влияния.
