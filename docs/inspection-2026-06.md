# Инспекция кодовой базы miniFlink — июнь 2026

Свежий обзор перед написанием полноценного приложения. Цель —
чистая библиотека без legacy конструкций и недоделок.

## Резюме

Кодовая база в целом **здоровая**: чистая Stream-абстракция,
полное покрытие .mli для публичных модулей, нет deprecated stdlib
(`Pervasives`, старый `Stream`, `String.lowercase` без `_ascii`),
нет `Obj.magic`, нет настоящих TODO/FIXME/HACK. 81 тестовый файл,
10 примеров.

Но есть **реальные** места для чистки. Приоритизированы ниже.

---

## СТАТУС ЧИСТКИ (обновлено)

- **P1.1 — ЗАКРЫТО.** window_fold + materializing-окна (window,
  global_window, window_instrumented) обрабатывают Update/Retract
  на input. count_window и session_window — документированные
  ограничения (не недоделки). Тесты: test_window_fold_update_input,
  test_window_materializing_update.
- **P1.2 — ЗАКРЫТО.** Все 12 `assert false` в persistence-guards
  (window, item, process_fn, trigger) заменены на `invalid_arg`
  с понятными сообщениями. Остался 1 законный assert false в
  pipe.ml (доказуемо недостижим).
- **P2.1 — ЗАКРЫТО (вариант а).** Old-style persistence params
  удалены, остался только ?persistence bundle. ~60 строк glue
  убрано. Все consumers мигрированы.
- **P3.1, P3.2 — ОСОЗНАННО ОТЛОЖЕНО.** ~150 fragile-match +
  ~100 name-disambiguation, подавляющее большинство в тестах/бенчах
  где они полностью безопасны (тест намеренно матчит один паттерн;
  disambiguation работает через вывод типов). В lib/ — 78 warnings,
  в основном на полях Persistence_backend.t (be.set/get/keys/delete)
  и win_state/fold_state matches. Исправление всех ~250 — большая
  механическая работа с близкой к нулю пользой. Оставлены
  подавленными в dune.
- **P2.2, P4 — открыты, низкий приоритет.**

---



### P1.1 — Window операторы DROP Update на input (6 мест)

`lib/window.ml`, строки 165, 493, 590, 661, 770, 823.

Каждое — `| Some (Mf_event.Update _) -> pull ()` с комментарием
"Phase 1 fallback". Это **недоделка** из Phase 1: Window получает
Update на input и молча его теряет.

Самое важное — `window_fold` (строка 493): он уже имеет `?remove`
параметр (Phase 2.2), и Update{old, new} логически означает
"remove old, add new". Должен обрабатываться нативно когда remove
есть, как это уже сделано для Retract на input.

Влияние: если кто-то построит цепочку `window_agg → window_agg`
или `window_agg → ... → window_fold`, Update от первого окна
потеряется во втором. В текущем minePASS такой цепочки нет, но
это **дыра** в семантике.

Решение: window_fold обрабатывает Update на input аналогично
Retract — применяет remove(old) + add(new) к затронутым окнам.
Остальные 5 (materializing window, count windows) — либо
аналогично, либо явный документированный drop с обоснованием.

### P1.2 — `assert false` для guarded-but-unchecked путей

`lib/window.ml` строки 314, 319: `ser_acc`/`deser_acc` падают с
`assert false` если `serialize_acc=None`. Это guarded раньше по
коду (backend требует serialize), но `assert false` — плохой
сигнал: если инвариант нарушится, будет невнятный краш вместо
понятной ошибки.

Решение: заменить на `invalid_arg` с понятным сообщением, либо
структурно гарантировать через тип (сложнее).

---

## P2 — Legacy / дублирующиеся API

### P2.1 — Два способа задать persistence (old-style + bundle)

`lib/window.ml`, `lib/process_fn.ml`, `lib/item.ml`, `lib/trigger.ml`.

Каждый stateful оператор принимает persistence ДВУМЯ способами:
1. Old-style: отдельные `?backend ?backend_name ?serialize_state
   ?deserialize_state`
2. New: `?persistence` bundle (record)

В каждом операторе есть код нормализации (`old_style_any`) +
`invalid_arg` если смешали. Это ~15 строк дублированной логики
× 4 оператора = 60 строк legacy-glue.

Использование: old-style в 4 тестах, bundle в 1. Оба активны.

Решение (требует решения пользователя): либо
(а) deprecated old-style, мигрировать всё на bundle, удалить glue;
(б) оставить оба, но вынести нормализацию в общий хелпер
    (Persistence_backend.normalize) чтобы убрать дублирование.

Вариант (а) — чище, но breaking change для кода использующего
old-style. Вариант (б) — backward-compatible, убирает дублирование.

### P2.2 — Record-based API параллельно с классическими

- `Trigger.create` (13 параметров) + `Trigger.of_config` (record)
- `Pipe.process_keyed` (10 параметров) + `process_keyed_spec` (record)

Это НЕ обязательно проблема — record-based API даёт template
pattern (полезно). Но стоит решить: это два равноправных API или
один из них "предпочтительный"? Сейчас неясно из документации.

Решение: документировать рекомендацию (например "of_config для
шаблонов, create для одноразовых триггеров"), или выбрать один
canonical.

---

## P3 — Хрупкость компиляции (warnings подавлены, но код хрупкий)

### P3.1 — 163 Warning 4 (fragile-match)

Подавлены в dune (`-w ...-4...` нет, но они и не errors). Каждый —
pattern match который ОСТАНЕТСЯ exhaustive при добавлении
конструктора, но компилятор предупреждает что это случайность.

Большинство — в тестах (наш недавний Phase 1 patch добавил
exhaustive Update ветки в lib, но тесты используют сжатые
`| _ ->`). Это не баг, но fragile: если добавим 5-й вариант в
Mf_event.t, эти matches молча проглотят его.

Решение: пройти и сделать matches явными там где это дёшево.
Низкий приоритет — это гигиена, не баг.

### P3.2 — 103 Warning 40 + 24 Warning 42 (name disambiguation)

Код полагается на type-directed disambiguation полей записей и
конструкторов. Например `be.set` выбирается из
`Persistence_backend.t` по типу. Это работает, но хрупко: при
рефакторинге типов может молча выбрать не то поле.

В основном в `lib/item.ml`, `lib/runtime.ml`. Решение: явная
квалификация (`Persistence_backend.set be`) или локальные open с
явным аннотированием. Низкий приоритет.

---

## P4 — Структурные наблюдения (не баги)

### P4.1 — window.ml большой (835 строк)

Содержит: `window`, `window_fold`, `count_tumbling`, `count_sliding`,
`session_window`, `global_window`. Каждый — отдельный pull-loop с
своей persistence-логикой. Дублирование persistence-кода между
window_fold и другими.

Не критично, но если будет рост — стоит вынести общий
persistence-skeleton.

### P4.2 — variants/ split (v4/v5)

`channel` и `parallel` имеют v4 (Thread) и v5 (Domain) варианты,
выбираются по версии OCaml через dune copy-rules. Это разумно для
4.14↔5.x совместимости. Но v4/v5 имеют дублированную логику
(parallel_v4 220 строк, v5 111). Если v4 (OCaml 4.x) больше не
нужен — можно удалить. Зависит от target OCaml версии.

---

## Что НЕ является проблемой (проверено)

- Stream-абстракция: чистая lazy pull-based, без утечек
- Нет deprecated stdlib (Pervasives, old Stream, non-ascii String)
- Нет Obj.magic (только упоминание в комментарии что его НЕ нужно)
- Нет настоящих TODO/FIXME/HACK
- List.nth в median — на bounds-checked индексах (safe)
- List.assoc в restore — partial, но на corrupt JSON exception ОК
- Hashtbl.find в trigger — обёрнут в try/with
- invalid_arg для валидации аргументов — правильно
- Magic numbers 4096 — документированный Hashtbl sizing
- Все публичные модули имеют .mli

---

## Рекомендованный порядок чистки

1. **P1.1** — доделать Window Update handling (функциональная дыра)
2. **P1.2** — assert false → invalid_arg (быстро, улучшает диагностику)
3. **P2.1** — решить судьбу old-style persistence (нужно решение)
4. **P3.1** — fragile-match гигиена (механически, низкий риск)
5. **P2.2, P3.2, P4** — по желанию, низкий приоритет
