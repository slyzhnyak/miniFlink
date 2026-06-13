# ex08_triggers — триггерная система в стиле Zabbix

Демонстрация **декларативных триггеров** как ортогонального
дополнения к существующим пайплайнам. Использует общие модули
`Trigger` (`lib/trigger.ml`) и `Item` (`lib/item.ml`), переиспользует
`Mock_source` из ex07.

## Запуск

```bash
dune exec examples/ex08_triggers/ex08_triggers.exe
```

Вывод (на Mock_source.Default):

```
⚠ M6 [30000 мс]: Low voltage 3.30 В
⚠ M1 [30000 мс]: CO 130 ppm
⚠ M1 [270000 мс]: Low voltage 3.37 В
🆘 M1 [350000 мс]: CRITICAL voltage 3.20 В
```

## Структура

```
ex08_triggers/
  domain.ml          — собственный type alert с парами Problem+Recovery
  items.ml           — функции построения item-потоков (voltage, gas_co,
                       no_packets через silence_age)
  triggers.ml        — декларации триггеров (low_voltage, voltage_critical,
                       no_packets, gas_co_warning)
  ex08_triggers.ml   — топология: source → items → triggers → union → sink
  dune
```

## Ключевая идея

Чтобы добавить новый триггер, достаточно:

1. Дописать конструктор в `Domain.alert` (Problem + Recovery)
2. Добавить функцию извлечения item-потока в `Items` (если нужен новый item)
3. Дописать декларацию `Trigger.create ...` в `Triggers.ml`
4. Подключить в топологии — `Trigger.of_stream` или добавить в `Trigger.combine`

**Ничего больше не править**: ни источника, ни sink, ни существующие триггеры,
ни модули ex07. Это и есть ортогональность.

## Чем отличается от ex07

| | ex07 connectivity_alerts | ex08 triggers |
|---|---|---|
| Подход | FSM-машина в `process_keyed` | Декларативные триггеры |
| Добавить алерт | Править `Last_seen`, `Fsm`, `next_check`, `check_and_emit`, `Domain.alert` (5 точек) | Одна декларация в `triggers.ml` |
| Композиция | Один монолит на всю логику | `Trigger.combine [t1; t2; ...]` |
| Refresh внутри Problem | Поддерживается | Нет (по дизайну MVP) |
| Late events | Через `allowed_lateness` окон | Игнорируются (forward-in-time only) |
| Когда лучше | Сложная логика с переходами между состояниями (SOS, multi-condition) | Простые threshold + hysteresis + debounce |

Оба подхода **сосуществуют**: в реальном сервисе можно использовать FSM
там где нужны сложные переходы, и триггеры — для большинства простых
threshold-алертов.
