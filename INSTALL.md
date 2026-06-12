# Установка и сборка на Debian Stable с OCaml 5

Инструкция от чистой системы Debian 12 (bookworm) до работающего
бенчмарка `bench_parallel.exe` на множестве доменов OCaml 5.

> **Что вы получите**: компилируемый проект на OCaml 5.1+ с настоящей
> параллельностью через `Domain.spawn`. Все 50 тест-сюит проходят,
> примеры запускаются, бенчмарк `bench_parallel.exe` показывает
> масштабирование по ядрам без GIL.

> **Чего НЕ получите автоматически**: ускорения hot path в одиночных
> пайплайнах. Параллельность включается только когда вы используете
> `Pipe.fan_out` или `Pipe.supervisor` явно (см. `ex06_topology.ml`).
> Одиночный `bench/bench_ex07.exe` остаётся single-threaded — это by
> design (главный потребитель ресурсов — окна и group_by, они sequential).

---

## Шаг 1: Системные зависимости

Debian Stable (bookworm) ставит OCaml 4.14 по умолчанию. Чтобы получить
OCaml 5, нужен **opam** (менеджер компиляторов и пакетов), а компилятор
ставится через него. Системный пакет `ocaml` использовать **не нужно**.

```bash
# Базовая сборка C-кода и системные библиотеки
sudo apt update
sudo apt install -y \
  build-essential m4 \
  pkg-config \
  curl unzip git \
  librocksdb-dev \
  zlib1g-dev libbz2-dev libsnappy-dev liblz4-dev libzstd-dev
```

Заметки:
- `build-essential`, `m4` — нужны для сборки OCaml-компилятора и stub'ов
- `librocksdb-dev` — C-биндинг для персистентного state backend
- `zlib`/`bz2`/`snappy`/`lz4`/`zstd` — транзитивные зависимости RocksDB
- `pkg-config` — opam использует для поиска C-библиотек

---

## Шаг 2: Установить opam

```bash
# Бинарь opam от upstream — версия в Debian Stable старая
sudo bash -c "sh <(curl -fsSL https://raw.githubusercontent.com/ocaml/opam/master/shell/install.sh)"

# Первая инициализация (без выбора компилятора — мы поставим его явно)
opam init --bare --disable-sandboxing -a -y
eval $(opam env)
```

Флаги:
- `--bare` — не создавать switch по умолчанию, мы создадим OCaml 5 явно
- `--disable-sandboxing` — пропускает bubblewrap (на Debian Stable он
  иногда не работает с user namespaces). Если предпочитаете sandbox,
  поставьте `bubblewrap` и уберите этот флаг.

---

## Шаг 3: Создать OCaml 5 switch

```bash
# OCaml 5.1.1 — стабильная версия с эффектами и параллельными доменами
opam switch create miniflink-5 ocaml-base-compiler.5.1.1

# Активировать switch в текущей оболочке
eval $(opam env --switch=miniflink-5)

# Проверить версию
ocaml -version    # должно показать 5.1.1
```

Сборка компилятора занимает ~5-10 минут. На многоядерной машине
ускорится с `OPAMJOBS=$(nproc)`:

```bash
OPAMJOBS=$(nproc) opam switch create miniflink-5 ocaml-base-compiler.5.1.1
```

---

## Шаг 4: Установить OCaml-зависимости

```bash
opam install -y \
  dune \
  yojson \
  ppx_deriving_yojson \
  qcheck \
  odoc
```

Эти библиотеки определены в `dune-project` / `miniflink.opam`. Если в
будущем добавятся новые depends, `opam install . --deps-only -t` подтянет
их автоматически (см. ниже альтернативный путь).

---

## Шаг 5: Клонировать репо

```bash
git clone https://github.com/slyzhnyak/miniFlink.git
cd miniFlink
```

Альтернативный, более идиоматичный путь установки зависимостей через
`miniflink.opam`:

```bash
# В корне репо
opam install . --deps-only -t -y
```

Этот вариант надёжнее на будущее — opam подтягивает ровно то что
требует `miniflink.opam`, не нужно держать список depends в двух местах.

---

## Шаг 6: Сборка

```bash
dune build
```

Если всё прошло — `_build/default/` содержит скомпилированные
артефакты. Если что-то не собралось — частые причины внизу.

---

## Шаг 7: Запуск тестов

```bash
dune test
```

Должны пройти все 50 сюит. Полный прогон — ~30-60 секунд.

---

## Шаг 8: Бенчмарк с параллельностью

```bash
# Базовый бенчмарк — single-threaded throughput по операторам
dune exec bench/bench.exe

# Параллельный бенчмарк — масштабирование по доменам OCaml 5
dune exec bench/bench_parallel.exe
```

`bench_parallel.exe` тестирует `Pipe.fan_out` с 1, 2, 4, 8 рабочими
доменами на одной и той же нагрузке.

> **Замечание о цифрах.** На OCaml 4.14 в README указаны базовые
> числа (см. раздел «Производительность»): single-thread 210K ev/s,
> 8 воркеров под GIL — ~600K ev/s (potolok ~3x из-за GIL). На OCaml 5
> с `Domain.spawn` ожидается более линейное масштабирование, но
> конкретных замеров на бенчмарк-машине у репозитория пока нет.
>
> Запустите бенчмарк сами и сравните — это и будет первый честный
> замер OCaml 5 для этого проекта. Если хотите внести в README,
> укажите процессор и количество ядер.

---

## Что НЕ ускорится автоматически

Если вы запустите `bench/bench_ex07.exe` на OCaml 5, throughput
останется примерно как на OCaml 4 — ~19K ev/s на полной шахте. Это
**ожидаемо**: ex07 это single-pipeline workflow, бенчмарк не использует
`fan_out`. Для параллельной обработки нужно явно структурировать
пайплайн через `fan_out`/`supervisor`/`merge_partitioned` — пример в
`examples/ex06_topology.ml`.

В роадмапе есть открытый пункт «migrate runtime to Domain.spawn» —
сейчас `lib/checkpoint_parallel.ml`, `lib/fan_out.ml` используют
условную компиляцию (Thread на v4, Domain на v5) через `parallel.ml`
переключатель. Что-то работает, что-то ещё нужно проверять под нагрузкой.

---

## Типичные проблемы

### `Error: Library "qcheck" not found.`

Установили зависимости без флага `-t` (with-test). `qcheck` в `.opam`
помечен как тестовая зависимость, без `-t` opam её пропускает.
Решение:

```bash
opam install . --deps-only -t -y
# или явно
opam install -y qcheck
```

### `Error: Library "rocksdb" not found`

Не установлен `librocksdb-dev` или версия слишком старая (нужна 6.20+).
На Debian Stable должна быть 7.8 — проверьте `dpkg -l librocksdb-dev`.

### `opam: bwrap: setting up uid map: Permission denied`

Sandbox bubblewrap не работает в этой среде. Решение:

```bash
opam init --reinit --disable-sandboxing -y
```

### Компилятор ставится долго (>15 минут)

Бутстрап OCaml на медленных машинах долгий. Решения:
- использовать `ocaml-system` если у системного OCaml 5+ (на Debian
  bookworm — увы, 4.14)
- использовать готовый switch с `ocaml-variants.5.1.1+options` если нужны
  специфические флаги

### `ld: cannot find -lrocksdb`

Проверьте `pkg-config --libs rocksdb`. Если пусто — система не видит
библиотеку. На Debian:

```bash
dpkg -L librocksdb-dev | grep '\.so'
# должна быть /usr/lib/x86_64-linux-gnu/librocksdb.so
```

### Тест зависает или падает с deadlock

На OCaml 5 некоторые код-пути в `lib/parallel.ml`, использующие
`Mutex.lock` без `Domain.spawn`, могут вести себя иначе. Если попали в
такой — это **баг библиотеки**, нужен issue на GitHub с воспроизведением.
В качестве temporary workaround — пересобрать на switch 4.14:

```bash
opam switch create miniflink-4 ocaml-base-compiler.4.14.1
eval $(opam env --switch=miniflink-4)
opam install . --deps-only -t -y
dune clean && dune build
```

---

## Заметка про reproducibility

Если планируете долго работать с проектом, **зафиксируйте версии**
через `opam lock`:

```bash
opam lock . -o miniflink.opam.locked
```

Это сохранит точные версии всех transitive deps. Восстановить:

```bash
opam install miniflink.opam.locked
```

Так гарантированно соберётся через год, даже если в opam-repo что-то
обновилось.
