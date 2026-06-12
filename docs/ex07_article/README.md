# ex07_article

Подробный разбор примера `ex07_location`: сервис локации и газовых
алертов для подземной шахты. Архитектура, design-rationale,
trade-offs.

Структура:

```
main.tex              ← главный документ, подключает разделы
listings_setup.tex    ← настройки подсветки OCaml
sections/
  01_problem.tex      ← задача и контекст
  02_inputs.tex       ← устройство входов
  03_pipelines.tex    ← три пайплайна
  04_retract.tex      ← retract-семантика
  05_performance.tex  ← производительность
  06_conclusions.tex  ← выводы
```

## Сборка

Нужен `pdflatex` (входит в texlive-full / MiKTeX) с поддержкой
кириллицы. На Debian:

```bash
sudo apt install texlive-lang-cyrillic texlive-latex-extra
```

Затем:

```bash
cd docs/ex07_article
pdflatex main && pdflatex main   # дважды для оглавления
```

На выходе `main.pdf`.

## Статус

Статья пишется частями. Текущее состояние — см. `git log -- .`
