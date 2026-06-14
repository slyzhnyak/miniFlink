# Формальная верификация ядра miniFlink: реалистичная оценка

Обсуждение возможности доказать корректность ядра miniFlink в Coq
(или другой proof assistant). Краткий ответ: **сложно, и зависит
от того что значит "доказать корректность"**. Этот документ — не
план действий, а трезвая оценка возможностей и стоимости.

## Что значит «корректность»

Сам термин неоднозначен. Возможные интерпретации:

1. **Type safety.** OCaml-компилятор уже даёт (no segfaults, no
   type confusion). Доказывать в Coq — переоткрытие закрытой
   двери.
2. **Корректность отдельных операторов.** `dedup` действительно
   дедуплицирует, `window_agg_keyed` правильно собирает значения.
3. **Семантические свойства потоков.** Watermark monotonicity,
   late events handling, retract idempotency.
4. **End-to-end корректность пайплайнов.** Конкретный пайплайн
   (например, ex07 connectivity_alerts) удовлетворяет своей
   спецификации.
5. **Конкурентная корректность.** Параллельный режим даёт **тот же**
   результат что и sequential при правильном partitioning.

Сложность растёт от (1) к (5) на порядки.

## Технические препятствия в нашем коде

### 1. Mutable state везде

Наш код использует Hashtbl, Queue, mutable record fields, Atomic.t.
Это **императивная** реализация в значительной мере. Для рассуждения
в Coq нужен один из:

- **Monadic encoding** — переписать в state monad с pure
  представлением state как параметра. Полная переработка
  реализации.
- **Separation logic** через CFML (Charguéraud) или Iris.
  Позволяет рассуждать про **существующий** императивный код, но
  доказательства громоздкие.

### 2. Pull-based Stream API через side effects

```ocaml
type 'a Stream.t = unit -> 'a option
```

Каждый вызов **изменяет** внутреннее состояние (продвигает позицию,
обновляет state в Hashtbl). Чтобы рассуждать формально, нужна
модель как state transformer: `Stream.t = State -> (Option a × State)`.

Существующие императивные реализации операторов (`Pipe.dedup`,
`window_agg_keyed`, `Trigger.of_stream`) держат state в closure через
mutable таблицы. Извлечь чистый state explicit'но — много работы.

### 3. OCaml 5 multicore concurrency

`Domain.spawn`, atomic operations, OCaml 5 memory model. Для
параллельных операторов (`fan_out`, `Parallel.run_parallel_simple`)
нужен **concurrent separation logic** (Iris).

Существенная проблема: **OCaml 5 memory model** — не до конца
формализована академически. Никто формально не верифицировал
OCaml 5 multicore library из коробки. Это **новая территория**.

### 4. RocksDB FFI

`state_backend_rocksdb` использует C-биндинги. Доказать корректность
через FFI невозможно без **формальной модели самой RocksDB**.
RocksDB — log-structured merge tree с background compaction,
большая С++-кодовая база. Никто её формально не верифицировал.

Это значит: всё что использует state_backend становится
**unverifiable** на текущей технологии. Можно **доказать** что
библиотечный код корректно вызывает API state_backend, но не
саму корректность storage.

### 5. OCaml сам не verification-friendly

Coq → OCaml **extraction** работает (CompCert делает это для своих
доказательств). Но это **обратное** направление: мы написали
сначала в OCaml. Чтобы доказывать про существующий OCaml код:

- **CFML** (Charguéraud, INRIA) — характеристические формулы для
  OCaml. Позволяет рассуждать про императивный OCaml. Но: в
  основном для **sequential** OCaml, не работает с OCaml 5
  multicore. Активной поддержки мало.
- **Iris-OCaml** — экспериментальный, не production-ready.

Альтернатива — переписать ядро в Coq, extract'ить в OCaml. Минусы:
- Теряем richness OCaml (например, GADTs в полной форме)
- Extracted OCaml код **читаемо хуже** чем рукописный
- Performance трудно контролируем
- Не работает с native int / float примитивами без обвязок

## Что было бы realistic

Три уровня формализации, в порядке возрастания амбиций.

### Уровень 0: Property-based testing (уже частично есть)

**Что.** Тесты в `test/test_trigger.ml`, `test/test_item.ml`,
etc. — уже фиксируют конкретные сценарии. **QCheck** позволил бы
делать generative tests: «для любого валидного stream s, для любого
trigger spec, …».

**Пример свойств:**

```ocaml
(* dedup idempotence *)
let prop_dedup_idempotent =
  QCheck.Test.make
    ~name:"Pipe.dedup is idempotent"
    QCheck.(triq events)
    (fun s ->
       let s1 = s |> Pipe.dedup ~rule:... |> Stream.to_list in
       let s2 = s |> Pipe.dedup ~rule:... |> Pipe.dedup ~rule:...
                 |> Stream.to_list in
       s1 = s2)

(* watermark monotonicity *)
let prop_watermark_monotonic =
  QCheck.Test.make
    ~name:"event_time produces monotonic watermarks"
    QCheck.(triq events)
    (fun s ->
       let watermarks =
         s |> Pipe.event_time ~lateness:(seconds 1)
           |> Stream.to_list
           |> List.filter_map (function
                | Mf_event.Watermark t -> Some t | _ -> None) in
       List.for_all2 (<=) watermarks (List.tl watermarks))

(* triggers: count of recoveries ≤ count of problems *)
let prop_trigger_balance =
  QCheck.Test.make
    ~name:"Trigger emits ≤ recoveries than problems"
    QCheck.(triq (pair spec events))
    (fun (spec, evs) ->
       let alerts = evs |> Trigger.of_stream spec |> Stream.to_list in
       let problems = count_problems alerts in
       let recoveries = count_recoveries alerts in
       recoveries <= problems)
```

**Стоимость.** Небольшая — несколько дней на полный suite. QCheck
уже в OCaml ecosystem.

**Что даёт.** Не доказательство, но **очень сильное** свидетельство.
QCheck находит баги которые в unit-тестах не покрыты.

**Что не даёт.** Гарантии для **всех** возможных входов. Только для
тех которые сгенерировались.

### Уровень 1: Coq-спецификация ядра (отдельно от реализации)

**Что.** Формальный документ:

```coq
(* Mf_event как inductive type *)
Inductive mf_event (A : Type) : Type :=
  | Data : A -> Time -> mf_event A
  | Retract : A -> Time -> mf_event A
  | Watermark : Time -> mf_event A.

(* Stream как coinductive — потенциально бесконечный *)
CoInductive stream (A : Type) : Type :=
  | SNil : stream A
  | SCons : A -> stream A -> stream A.

(* Операторы как чистые функции *)
CoFixpoint dedup
  {A K : Type}
  (key : A -> K)
  (decide : A -> A -> bool)
  (seen : list A)
  (s : stream A) : stream A := ...

(* Свойство: dedup idempotence *)
Theorem dedup_idempotent :
  forall A K (key : A -> K) (decide : A -> A -> bool) (s : stream A),
    bisim (dedup key decide [] (dedup key decide [] s))
          (dedup key decide [] s).
Proof. ... Qed.

(* Свойство: watermark monotonicity для event_time *)
Theorem event_time_monotonic_wm :
  forall A (lateness : Time) (s : stream (mf_event A)),
    monotonic_watermarks (event_time lateness s).
Proof. ... Qed.

(* Свойство: retract корректность *)
Theorem materialize_retract :
  forall A (key : A -> K) (v : A) (t : Time),
    materialize key (cons (Data v t) (cons (Retract v t) nil)) = empty.
Proof. ... Qed.
```

**Что покрыть в спецификации:**

- Базовая семантика `Mf_event.t` и `Stream.t`
- Операторы: `map`, `filter`, `flat_map`, `dedup`, `union`,
  `event_time`, `window_agg_keyed`, `process_keyed`, `materialize`
- Trigger semantics: 4-state machine, Pending/Problem/Pending_ok/Ok
- silence_age semantics

**Доказательства ~15-20 ключевых лемм:**

1. Operator composition (algebraic laws):
   - `map f ∘ map g ≡ map (f ∘ g)`
   - `filter p ∘ filter q ≡ filter (λx. p x ∧ q x)`
   - `dedup ∘ dedup ≡ dedup`
2. Event-time correctness:
   - Watermarks monotonic
   - Late events trigger retract for closed windows in allowed_lateness
3. Trigger correctness:
   - Trigger emits Problem only after problem_for elapsed
   - Trigger.combine = parallel composition (no cross-interference)
4. Retract idempotence:
   - `materialize (Data v ++ Retract v) ≡ empty`
   - Multiple retracts of same value collapse correctly

**Объём.** Примерно **3-5K строк Coq**, постепенно растущих по мере
формализации новых операторов.

**Сроки.** **3-6 месяцев работы** одного человека с **опытом Coq**
(не новичка — для новичка Coq learning curve добавит ещё 6-12
месяцев).

**Что даёт.**
- Формальная спецификация — что должно происходить
- Доказательство ключевых свойств
- Основа для тестирования OCaml-реализации против спеки
- Документация на уровне точности недостижимой иначе

**Что не даёт.**
- Гарантию что **наш OCaml-код** корректен. Это **отдельный** документ.

### Уровень 1.5 (гибрид): Spec → property-tests генерируются

**Что.** Coq-спецификация + автогенерация property tests для
OCaml-реализации. Любой обнаруженный divergence — баг в OCaml.

**Стоимость.** Coq Level 1 + инструмент для генерации тестов из
Coq в OCaml (потребуется писать).

**Польза.** Связь между спекой и реализацией. **Не строгое
доказательство**, но **систематическое** тестирование.

### Уровень 2: Полная end-to-end верификация OCaml-кода

**Что.** Доказать что наш OCaml-код **реализует** Coq-спецификацию.
Через CFML или переписывание в Coq + extraction.

**Объём (оценка).** Для нашего ~5K строк OCaml ожидаемо **15-30K
строк Coq**.

**Сроки.** **2-3 года работы одного эксперта-уровня PhD**.

**Сравнения:**
- **CompCert** — 10+ лет, ~100K строк Coq, формально верифицированный
  C-компилятор. Делал Xavier Leroy + большая команда.
- **seL4** — ~10 человеко-лет на ~10K строк C-кода микроядра. NICTA.
- **CertiKOS** — формально верифицированное микроядро, академический
  проект ~5+ лет.
- **Iris** — concurrent separation logic, активный академический
  проект ~10 лет на момент написания.

**Что мешает у нас:**
- OCaml 5 multicore semantics не формализована
- RocksDB FFI неверифицируем
- Mutable state везде

**Реалистично?** Нет, не для нашей библиотеки одним человеком.
Это уровень академического проекта на несколько лет.

## Что я бы рекомендовал реально

Прагматично, в порядке возрастания цены:

1. **[выполнимо за неделю-месяц] QCheck свойства для ключевых
   операторов.** dedup idempotence, watermark monotonicity, trigger
   balance, retract correctness. Не доказательство, но **сильное**
   свидетельство. Огромный ROI.

2. **[выполнимо за 3-6 месяцев] Coq-спецификация ядра как
   отдельный документ.** Не связанная с OCaml-кодом. Даёт точную
   спеку + algebra законов. **PhD-style работа** но в разумных
   сроках.

3. **[heroic, не делать] Полная верификация OCaml-реализации.** ROI
   отрицательный.

## Аналоги в индустрии

Кто **формально верифицировал** stream processing:

**Никто** в production-смысле для всей библиотеки. Есть:
- Академические работы по **формализации семантики Flink** — спека,
  но не сам Flink-код.
- **Bloom** (UCBerkeley) — declarative distributed programming с
  формальной семантикой, но это исследовательский язык, не
  production.
- **DDlog** (VMware) — Datalog с временными аспектами, формально
  специфицирован.

Industrial stream-processing libraries (Flink, Kafka Streams, Goka,
Beam) — **никто** не имеет Coq-доказательства корректности.
Тестирование + property-based + статический анализ — стандарт.

**Это значит:** если мы делаем Coq-спецификацию ядра — мы **впереди
индустрии** в формальной строгости. Это **отличает** проект.

Если делаем полную верификацию — это **академический проект на
несколько лет**, не разработка.

## Итог

**Сложно — да.** Не невозможно, но требует калибровки целей.

**Реалистичные варианты:**

- **Property-based testing (QCheck)** — несколько дней. Высокий ROI.
- **Coq-спецификация ядра без связи с OCaml** — 3-6 месяцев одного
  эксперта. Даёт формальную модель и алгебраические доказательства.
- **Полная end-to-end верификация** — 2-3 года эксперта. Не делать
  для production library; разумно только как академический проект.

Если интерес есть — начинать с QCheck. Это **бесспорно полезно**.
Coq-спецификация — **разумная цель** на средне-срочный горизонт.
Полная верификация — **не для нашего случая**.

## Что добавить в TODO

Если решим в эту сторону двигаться:

- **[опц-средний] QCheck property-based tests для ключевых
  операторов.** Несколько дней работы. Покрыть dedup, watermarks,
  trigger semantics, retract correctness.

- **[опц-большой] Coq-спецификация ядра как отдельная
  формализация.** Без претензии связать с OCaml-кодом — отдельный
  документ. 3-6 месяцев.

Не добавляю в TODO README прямо сейчас — это **обсуждение**, не
коммитмент. Если решишь делать — обсудим план перед добавлением.
