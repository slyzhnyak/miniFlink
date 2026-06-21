# Покрытие семантики Update/Retract по операторам — июнь 2026 (раунд 4)

Раунд с акцентом на тестовое покрытие. Мотивирующий вопрос: почему
80+ тестов пропустили столько багов (раунды 2-3)?

## Почему тесты пропускали баги

Три слепые зоны, объясняющие пропуски:

1. **Только happy-path.** Баги раундов 2-3 (mutex-deadlock, fd-leak,
   GC-safety) живут в ИСКЛЮЧИТЕЛЬНЫХ путях — срабатывают когда
   пользовательский callback бросает, или при специфичном GC-тайминге.
   Happy-path тесты их не трогают по построению.

2. **Короткие прогоны.** Утечки памяти (kafka snapshots, temporal
   versions) видны только на ДОЛГИХ прогонах. Unit-тесты быстрые →
   неограниченный рост невидим.

3. **Только Data-события.** Самая коварная зона: операторы
   тестировались на потоках из одних Data. Update/Retract — особенно
   ASYMMETRIC (где предикат/агрегат по-разному реагирует на old/new)
   и LATE corrections — почти не покрывались. Эти баги дают МОЛЧА
   неверный результат, а не падение.

Раунд 4 закрывает зону #3 систематически.

## Найденные баги (зона #3)

### filter — несогласованная Update/Retract семантика
**pipe.ml** (commit 09ddad8)

filter применял предикат к new_value и пропускал/отбрасывал Update
целиком. При asymmetric Update это рассогласование:
- (p old=F, p new=T): эмитил Update{old→new}, но downstream никогда
  не видел old → коррекция несуществующего. Должно быть Data(new).
- (p old=T, p new=F): молча отбрасывал → downstream думает old ещё
  там. Должно быть Retract(old).
- Retract отфильтрованного значения проходил как шум.

Тесты filter работали только на Data → пропустили. Исправлено
полным разбором (p old, p new).

### window_agg — noop-коррекции
**pipe.ml, window.ml** (commit 3ee6ac2)

window_fold эмитил Update на КАЖДОЕ late Data/Retract в fired-окно,
даже когда видимый результат не менялся (sum+0, median тем же
значением, max меньшим) → noop Update{r→r}. Шум для downstream
(keyed_join делал лишний snapshot). Исправлено подавлением после
finish (сравнение результата, не аккумулятора — median acc меняется
при неизменном результате).

## Проверено — корректно (тесты добавлены, багов нет)

### keyed_join (commit a6a37ba) — test_keyed_join_semantics.ml
Snapshot-оператор, ради чьей атомарности и вводился Update. 5
сценариев: Retract→None, stale Retract игнорируется, atomic Update
БЕЗ None-flicker (ключевая гарантия), multi-source merge, unseen
Update как появление. Все корректны.

### trigger (commit 8d7c52b) — test_trigger_update_semantics.ml
Update реагирует на new_value через FSM problem/recovery. 3
сценария: коррекция вниз через порог → recovery, вверх → alert, в
той же зоне → без дубля. Все корректны.

### map / flat_map
map_value трансформирует ОБЕ половины Update симметрично; flat_map
zip'ует old/new результаты. Корректно (test_event_semantics_coverage
case 4, 5).

### dedup
Retract/Update проходят passthrough — осознанно (дедуп реагирует
только на свежие Data). Корректно.

### merge (раунд 2)
Все 4 варианта exhaustive, Update прозрачен. Корректно.

### sink / collect / fold_data — семантически защитимо
Терминальные потребители применяют f к new_value на Update,
игнорируют Retract. collect даёт [old; new] (лог значений, не
snapshot). Это граница системы — интерпретация на стороне
пользователя. Не баг, но стоит знать при использовании collect для
проверки финального состояния (используйте materialize/snapshot для
этого).

## Матрица: оператор × тип события

| Оператор      | Data | Watermark | Retract           | Update                    |
|---------------|------|-----------|-------------------|---------------------------|
| map (emap)    | ✓    | passthr   | трансформ value   | трансформ обе половины ✓  |
| filter        | ✓    | passthr   | passthr если p(v) | разбор (p old,p new) ✓ FIX|
| flat_map      | ✓    | passthr   | N retracts        | zip N updates ✓           |
| enrich        | ✓    | passthr   | обогащает value   | обогащает обе ✓           |
| dedup         | dedup| evict     | passthr           | passthr (always) ✓        |
| window_agg    | ✓    | fire/close| remove если retr  | atomic, noop-supp ✓ FIX   |
| window_fold   | ✓    | fire/close| remove(old)       | remove+add atomic ✓       |
| window (mat.) | ✓    | fire/close| remove_first      | replace, noop-supp ✓      |
| count_window  | ✓    | count-fire| drop (design)     | drop (design, doc)        |
| session_window| ✓    | gap-fire  | drop (design)     | best-effort add new (doc) |
| keyed_join    | ✓    | passthr   | clear if match ✓  | atomic, no flicker ✓      |
| trigger       | ✓    | timers    | ignore            | react on new_value ✓      |
| process_keyed | ✓    | timers    | (on_update opt)   | on_update callback ✓      |
| merge         | ✓    | min-active| passthr           | passthr ✓                 |
| table         | ✓    | passthr   | stop (doc)        | replace value ✓           |
| temporal      | ✓    | gate      | ignore (doc)      | new version ✓             |
| sink/collect  | ✓    | ignore    | ignore            | f(new_value)              |

FIX = исправлено в этом раунде. doc = документированное ограничение.

## Вывод

Систематическое покрытие Update/Retract/late-data нашло 2
семантических бага (filter, window_agg) — оба давали МОЛЧА неверный
результат, классическая слепая зона "только Data" тестов.
Остальные операторы проверены и корректны; добавлены тесты для
keyed_join и trigger, закрывающие пробелы покрытия на самых
важных для Update операторах.

Урок для будущих тестов: каждый оператор должен тестироваться на
ВСЕХ четырёх типах событий, с акцентом на asymmetric Update и late
corrections, а не только на happy-path Data.
