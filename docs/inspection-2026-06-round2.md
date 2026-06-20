# Глубокая инспекция miniFlink — июнь 2026 (раунд 2)

Второй, более глубокий заход. Фокус на том что первая инспекция
(docs/inspection-2026-06.md) прошла поверхностно: конкурентность,
управление ресурсами, корректность семантики, граничные случаи.

## Резюме раунда 2

Найдено и исправлено **4 реальных бага**, все в редко-используемых
но критичных путях (параллельный режим, fan_out, durable
checkpoints). Плюс несколько тонких мест проверено и признано
корректными или некритичными.

Главный вывод: ядро (Stream, операторы, watermark-логика) —
зрелое и аккуратное. Баги сконцентрированы в concurrency/IO
обвязке, где обработка исключений была неполной.

---

## ИСПРАВЛЕНО

### #1 — Deadlock: mutex не разблокируется при исключении в callback
**checkpoint_parallel.ml** (commit 34783d6)

Несколько mutex-регионов вызывали пользовательский код
(cs_persist, publish) между Mutex.lock/unlock без exception safety.
Если callback бросал (persist на полном диске, sink.publish с
ошибкой) — unlock пропускался, mutex оставался заблокированным
навсегда → deadlock всего checkpoint-механизма.

Fix: helper `with_mutex mu f` разблокирует на обоих путях (return
и exception). Применён к commit, submit_snapshot, mark_failed,
ts_commit, ts_flush и др. Lock ordering проверен (co_mu→cs_mu, без
обратного). Тест: test_mutex_unlock_safety.ml.

### #2 — Deadlock: тот же класс в fan_out
**fan_out.ml** (commit bcdd1e4)

`advance t` под mutex вызывает `t.source ()` (пользовательский
источник). Если источник бросит — unlock пропущен, mutex
заблокирован → deadlock ВСЕХ выходов fan_out (общий mutex).

Fix: loop обёрнут так, что исключение освобождает mutex и
пробрасывается. Нельзя было использовать with_mutex напрямую
из-за Condition.wait внутри loop. Тест:
test_fan_out_unlock_safety.ml.

### #3 — File descriptor leak при IO-ошибке
**checkpoint_parallel.ml** (commit a3b5e48)

durable_store.persist и load_durable открывали файлы и закрывали
только на success-пути. Если output_bytes/really_input бросали
(диск полон, битый файл) — close пропускался. persist вызывается
на КАЖДОМ checkpoint → накопление leaked fd до исчерпания лимита
и смерти процесса.

Fix: helpers with_out_channel/with_in_channel закрывают на обоих
путях (close в exception-ветке сам в try/with чтобы не маскировать
исходное исключение).

### #4 — Division by zero в публичном hash_key
**parallel_v4/v5.ml, checkpoint_parallel.ml** (commit 727c189)

`hash_key key n` вычисляет `... mod n`. При n=0 → опаковый
Division_by_zero. Публичный API (Parallel.hash_key). В норме n =
число воркеров (валидируется Config), но как публичная функция
должна явно падать на плохом входе.

Fix: guard `if n <= 0 then invalid_arg` во всех 3 копиях.
Документировано в parallel.mli.

---

## ПРОВЕРЕНО — корректно или некритично (не трогал)

### Concurrency
- **log.ml**: sink под mutex обёрнут в try/with — корректно.
- **metrics_log.ml**: регионы только арифметика/Array — не бросают.
- **dlq_log.ml**: user-код (Log.warn) ВНЕ mutex — корректно.
- **supervisor.ml**: присваивания под mutex, хороший memory-model
  комментарий — корректно.
- **channel_v4/v5.ml**: между lock/unlock только буфер-операции,
  никакого user-кода — корректно.

### Семантика
- **mf_event.ml watermark**: overflow `wm - min_int` уже явно
  обработан (первый wm эмитим всегда). Зрелый код.
- **merge.ml**: все 4 варианта Mf_event exhaustive, Update
  прозрачен. `else None` при progressed=false фактически
  недостижим (done_ ставится только при pull()=None с
  progressed=true). idle:Never даёт warning. Корректно.
- **keyed_join Update**: атомарный single snapshot для same-key,
  корректная декомпозиция cross-key, stale handling. Зрелый код.
- **agg.ml retract**: sum/count/mean retractable; min/max/first/
  last с remove=None и правильным обоснованием. median удаляет из
  списка. Корректный дизайн.

### Граничные случаи (некритичные, документированы как поведение)
- **agg median finish**: `[] -> None` guard, индексы в bounds для
  n>=1. Safe.
- **agg p² parabolic**: деления могут дать nan/inf при дублях
  маркеров, но это свойство самого P²-алгоритма (float, не
  крашит), есть fallback на линейную. Не баг.
- **agg mean over-retract**: retract больше чем add → n<0 →
  неверный результат (не краш). Некорректное использование
  (retract без add), edge case. Float, не крашит.
- **process_fn fire_due**: если on_timer бросит на середине списка
  due-таймеров, оставшиеся уже удалены из set → потеряны. Но при
  persistence они вернутся из последнего checkpoint (persist на
  watermark, ДО fire_due). Без persistence pipeline всё равно упал.
  Не критично при корректном использовании.
- **timestamp overflow** (stop+latency+lateness): теоретический, но
  max_int мс = ~292 млн лет. Нереалистично.

---

## Вывод

Библиотека прошла глубокий аудит. 4 реальных бага (все в
concurrency/IO путях) исправлены с регрессионными тестами.
Семантическое ядро подтверждено как корректное. Оставшиеся
edge cases — либо свойства алгоритмов, либо некорректное
использование API, не баги реализации.

Готова для построения приложения.
