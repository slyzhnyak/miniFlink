# Persistence для `trigger` — устарело

> **Этот документ описывал старую модель persistence** (параметр
> `?persistence`/`?backend` в операторе, ручная JSON-сериализация,
> формат backend-ключей). Эта модель заменена **ортогональной
> persistence**: оператор больше не принимает persistence-параметра,
> состояние хранится в `Managed_state`, а режим (ephemeral/durable) и
> сериализация задаются снаружи через `Runtime_context`.
>
> Актуальная документация: **[orthogonal-persistence.md](orthogonal-persistence.md)**.

## Что изменилось для `trigger`

- Параметр persistence убран из сигнатуры. Тот же вызов оператора
  работает с persistence и без неё.
- Persistence включается снаружи:
  `Runtime_context.with_context (Runtime_context.durable backend) (fun () -> ...)`.
- Сериализация по умолчанию — Marshal (никаких ручных сериализаторов);
  для schema evolution — codec-реестр в контексте.
- Состояние переживает рестарт: e2e-тест показывает, что
  `phase1 + phase2 = baseline` (включая pending-таймеры там, где они
  есть).

Детали и примеры — в [orthogonal-persistence.md](orthogonal-persistence.md).
