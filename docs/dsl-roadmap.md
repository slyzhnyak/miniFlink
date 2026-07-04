# Дорожная карта DSL (по ревью 2026-07)

Рабочий чеклист по итогам `docs/review-dsl-2026-07.md`. Здесь — только
статусы; мотивация, разбор ex07 и детали каждого пункта — в ревью.
Обновлять при закрытии пунктов (галочка + коммит/дата).

Цель-критерий всего плана: **ex07 переписывается композицией
комбинаторов, без единого ручного `match stream () with` — и становится
витриной DSL, а не обходом его дыр.**

---

## P1 — дёшево, закрывает видимые дыры (одна сессия)

- [x] **P1.1 / G-1** (готово, f05f518) `Pipe.map_ts : ('a -> Time.t -> 'b) -> 'a Mf_event.t Stream.t -> 'b Mf_event.t Stream.t`
      — map с доступом к timestamp, структура событий
      (Data/Retract/Update/Watermark) сохраняется библиотекой.
      Сразу же: переписать хвост `median_rssi` в ex07 на него
      (убирает 18-строчный ручной цикл, pipelines.ml:143-160).
- [x] **P1.2 / G-4** (готово, 155a8ca) `ctx.emit_retract : 'out -> unit` и
      `ctx.emit_update : old:'out -> 'out -> unit` в process_keyed ctx
      (Process_fn). Существующий `emit` не трогаем — обратная
      совместимость.
- [x] **P1.3 / E-4** (готово, 4c86172) Валидация Time/int-параметров на входе публичных
      конструкторов: `event_time ~lateness<0`, `dedup ~cooldown<0`,
      `keyed_join ~ttl<=0`, `Trigger.of_stream ~ttl<=0`,
      `run_exactly_once ~workers<=0 / ~capacity<=0 / ~checkpoint_every<=0`.
      `invalid_arg` с именем параметра, как уже сделано в окнах.
- [x] **P1.4 / E-5** (готово, c4f8c1c) Doc-предупреждения «⚠ бесконечный поток = вечный
      цикл» на все дренящие потребители: `collect`, `Stream.to_list`,
      `materialize`, `fold_data`, `count_data`.
- [x] **P1.5 / E-1** (готово, eafb7d1) Warning «окно без watermark»: оконный оператор при
      N=10000 событий без единого Watermark логирует один раз
      `Log.warn "окно не получило ни одного watermark — забыт
      Pipe.event_time?"`. Главный тихий footgun.

## P2 — средний размер, главный выигрыш выразительности

- [x] **P2.1 / G-5** (готово, e411d19) `Pipe.co_process2` / `co_process3` — co-обработка
      разнотипных потоков на общем ключе: общий per-key state,
      `on_a`/`on_b`/`on_c`, emit_event, watermark по min (union внутри).
      Каркас = текущий ручной код gas_alerts, один раз и оттестированный.
- [x] **P2.2** (готово, e411d19) Переписать `gas_alerts` в ex07 на co_process3
      (~200 строк → ~25-40). Целевой эскиз — в ревью, Часть 1.4.
- [x] **P2.3 / G-3** (готово, eb418c6) `Pipe.single_timer` (или helper в Process_fn):
      «один логический таймер на ключ с переносом цели на ближайший
      дедлайн» — вынести из ex07 (Self_timer) в библиотеку.
- [x] **P2.4 / E-3** (готово, cd821d0) `?on_error:(`Dlq of Dlq_log.t | `Propagate)` для
      process_keyed и window_fold. Default `Propagate` (текущее
      поведение). Мотивация: после M-3 ядовитое событие в exactly-once =
      вечный цикл crash-recovery-crash.
- [x] **P2.5 / E-2** (готово, 2ab6156) Док-блок про min-семантику watermark в union +
      idle; опционально шорткат `union ~idle_timeout`.

## P3 — крупный дизайн, отдельное обсуждение перед реализацией

- [x] **P3.1 / G-2a** (готово, f7449dc) Условия на тишину first-class:
      `Trigger.on_silence ~threshold` (сейчас — только связка
      Item.silence_age + of_stream, 4 разных silence на ключ не
      выражаются).
- [x] **P3.2 / G-2b** (готово, f7449dc) Подавление между автоматами:
      `Trigger.suppress_when other_state` (motion/voltage молчат при
      No_packets). Цель: connectivity_alerts ex07 из ~200 строк слоёв в
      ~40 строк деклараций четырёх автоматов.
- [x] **P3.3 / G-6** (готово, d5f86f7 — закрыт co_process3 бесплатно) Reactive enrich (side input с re-emit активных
      выходов при обновлении справочника). Проверить после P2.1 —
      возможно, выражается через co_process бесплатно.
- [x] **P3.4 / E-6** (готово) Финал: пройтись по ex07 целиком — либо все обходы
      заменены комбинаторами, либо остатки помечены «ограничение DSL,
      см. roadmap» с номером пункта.

## Вне плана (зафиксированные решения)

- Pull-модель НЕ меняем (осознанный выбор, см. оговорку в конце ревью).
- M-6 (3x-пик снапшота) и M-2 (Obj в codec-реестре) — отложены с
  обоснованием, см. docs/review-2026-06-30-response.md.
- TSan-прогон C-1-класса гонок через CI (OCaml 5.2+tsan switch) — идея
  из обсуждения аудита; opam.ocaml.org не в локальном allowlist, путь —
  CI-job по образцу kafka-eos.
- Fuzzing (Crowbar/afl через CI) для парсеров/декодеров (Kafka payload,
  snapshot restore, Marshal) — обсуждалось, приоритет ниже roadmap DSL.

---

## Итог (все P1–P3 закрыты)

Все шесть разрывов выразительности (G-1…G-6) и шесть unhappy-path'ов
(E-1…E-6) из ревью закрыты. Добавленные в библиотеку операторы:

- `Pipe.map_ts` — map с доступом к timestamp (G-1)
- `ctx.emit_retract` / `emit_update` / `emit_event` — retract-семантика
  в keyed-логике (G-4)
- `Pipe.co_process2` / `co_process3` — co-обработка разнотипных потоков
  на общем ключе (G-5); попутно закрыл G-6 (reactive enrich) бесплатно
- `Pipe.Single_timer` — один логический таймер на ключ (G-3)
- `Pipe.on_silence` — детекция отсутствия как первичная операция (G-2a)
- `Pipe.suppress_while` — подавление как композиция (G-2b)
- `?on_error` в process_keyed / window_fold — ядовитое событие в DLQ,
  не крах пайплайна (E-3)
- валидация Time/int-параметров (E-4), warning об окне без watermark
  (E-1), doc-предупреждения о дренящих потребителях (E-5), документация
  union min-watermark + idle (E-2)

### ex07 как витрина (E-6)

pipelines.ml: 605 → 457 строк. Все три пайплайна — декларативные
композиции:
- `median_rssi` — dedup → flat_map → event_time → window_agg (медиана,
  top-2); хвост на `map_ts`;
- `connectivity_alerts` — три `on_silence` + `Trigger` (гистерезис) +
  `suppress_while` + SOS-импульс, вместо трёх ручных FSM;
- `gas_alerts` — `co_process3` с тремя доменными обработчиками, retract/
  update/reactive-enrich выражены прямо в DSL.

Не осталось ни ручных `match stream () with`, ни сентинелов
`min_int/max_int`, ни признательных комментариев «делаем руками».
Остаток pipelines.ml — доменные функции (RSSI→расстояние, интерполяция
позиции, разбор газов), которые кодом остаются в любом фреймворке.

Тест-покрытие: каждый оператор имеет юнит-тест; ex07 smoke проверяет все
шесть видов алертов + монотонность late-событий; ex07 lib проверяет
reactive enrich end-to-end. CI зелёный на OCaml 4.14 и 5.2.
