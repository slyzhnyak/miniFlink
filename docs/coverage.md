# Покрытие тестами (code coverage)

miniFlink измеряет покрытие через
[bisect_ppx](https://github.com/aantron/bisect_ppx). `lib/dune` уже
содержит `(instrumentation (backend bisect_ppx))`, поэтому покрытие
включается флагом при прогоне тестов — отдельной настройки не нужно.

## Быстрый старт

```sh
./scripts/coverage.sh          # сводка в консоль
./scripts/coverage.sh --html   # ещё и HTML-отчёт в _coverage/
```

Скрипт прогоняет весь набор тестов с инструментацией, собирает
`.coverage`-файлы и печатает сводку плюс худшие по покрытию модули.

## Текущее покрытие

Замер на полном наборе тестов (120 тест-файлов):

```
Coverage: 2639/3064 (86.13%)
```

Это выше порога 80%, который заложен как минимальный gate. По
ключевым модулям потоковой обработки:

| Модуль | Покрытие |
|---|---|
| `stream.ml` | 98.8% |
| `trigger.ml` | 89.9% |
| `merge.ml` | 88.9% |
| `checkpoint_parallel.ml` | 86.3% |
| `window.ml` | 84.5% |
| `temporal.ml` | 88.8% |
| `pipe.ml` | 83.6% |

Просадки сосредоточены в инфраструктурных/observability-модулях
(`metrics_log` ~60%, `runtime` ~77%, `health` ~74%, `config` ~76%) и в
обработке ошибочных веток мелких модулей, а не в ядре обработки
потоков. Это ориентир, куда добавлять тесты в первую очередь, если
поднимать общий порог.

## Установка bisect_ppx без opam

В окружении без opam (только apt + ocamlfind) bisect_ppx ставится из
исходников:

```sh
# зависимости
apt-get install -y libcmdliner-ocaml-dev   # ppxlib обычно уже есть

# сборка нужных частей (без js_of_ocaml/reason)
git clone --depth 1 --branch 2.8.3 \
  https://github.com/aantron/bisect_ppx.git
cd bisect_ppx
dune build src/runtime src/ppx src/report
dune install --prefix=/usr --libdir="$(ocamlfind printconf destdir)"

# проверка
ocamlfind list | grep bisect          # → bisect_ppx 2.8.3
which bisect-ppx-report                # → /usr/bin/bisect-ppx-report
```

Версия 2.8.3 совместима с OCaml 4.14. Сборка только `src/runtime
src/ppx src/report` пропускает необязательные js_of_ocaml/Reason-части,
которым нужны лишние зависимости.

Если ocamlfind не находит библиотеку при прогоне в проекте, добавьте
`OCAMLPATH`:

```sh
OCAMLPATH="$(ocamlfind printconf destdir)" ./scripts/coverage.sh
```
