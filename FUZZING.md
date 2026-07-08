# Fuzzing и TSan — как прогнать

Тесты надёжности, требующие toolchain, которого нет в CI-песочнице
(crowbar/afl, OCaml tsan-switch). Написаны в аудит 2026-07 для двух
систематически непроверенных классов: **паники на битом внешнем входе**
и **data races**.

## Быстрый старт (одна команда)

```sh
./scripts/reliability.sh
```

Скрипт сам определяет, что установлено, прогоняет доступное (sanity
всегда; afl-fuzzing если есть crowbar+afl; TSan если switch с
ocaml-option-tsan) и честно сообщает про пропущенное. Флаги:
`--quick` (только sanity), `--fuzz-secs N` (длительность afl).

Ниже — что каждый тест делает и как запускать вручную.

Здесь три исполняемых:

| Файл | Что проверяет | Toolchain |
|---|---|---|
| `test/fuzz_decoders.ml` | декодеры не бросают на мусоре (QCheck-режим) | ничего сверх обычного |
| `fuzz/fuzz_crowbar.ml` | то же под afl + **сырой Marshal restore** | `crowbar`, `afl` |
| `test/tsan_parallel.ml` | гонки в параллельном exactly-once | OCaml + tsan |

Быстрый sanity (без спец-toolchain, уже проходит):

```sh
dune exec test/fuzz_decoders.exe     # 15000 QCheck-случаев
dune exec test/tsan_parallel.exe     # 5 конфигураций contention
```

## 1. Fuzzing декодеров (afl через Crowbar)

```sh
opam install crowbar
dune build --profile fuzz fuzz/fuzz_crowbar.exe

# быстрый Crowbar-режим (без afl, случайная генерация):
dune exec --profile fuzz fuzz/fuzz_crowbar.exe

# настоящий afl-fuzzing (afl-инструментация уже в профиле fuzz —
# отдельный afl-switch НЕ нужен):
opam install afl-persistent
sudo apt install afl++               # чтобы afl-fuzz был в PATH
mkdir -p fuzz_in fuzz_out
printf '\x00\x01hello' > fuzz_in/seed
afl-fuzz -i fuzz_in -o fuzz_out -- _build/fuzz/fuzz/fuzz_crowbar.exe @@
```

Три таргета по возрастанию опасности:

1. **`schema.decode never raises`** — контракт: любой байт-мусор →
   `Ok`/`Error`, никогда исключение. Ожидание: чисто.
2. **`schema round-trip`** — `encode >> decode` тождественно. Чисто.
3. **`state_backend restore on raw bytes`** — ⚠ **вот главная цель.**

### ✅ ИСПРАВЛЕНО (N-9): `Marshal.from_bytes` в restore обёрнут рамкой

`State_backend_memory.restore` (и `_rocksdb`, и `runtime_context.of_bytes`,
и `checkpoint_parallel`) декодируют снапшот через
`Marshal.from_bytes b 0` на **сырых байтах без валидации**. Публичная
сигнатура — `val restore : t -> bytes -> unit`.

`Marshal.from_bytes` на недоверенном/повреждённом вводе в OCaml **не
безопасен**: он может не просто бросить `Failure`, а **segfault или
повредить heap** (документированное свойство OCaml — Marshal доверяет
формату). То есть если байты снапшота пришли с повреждённого диска или
из недоверенного источника, restore — дыра.

Таргет 3 существует, чтобы это **подтвердить экспериментально**: если
afl найдёт вход, валящий процесс (SIGSEGV, а не пойманный `Failure`) —
это доказательство, что restore нужно перевести на safe-декодер:
length-prefixed формат + версия + проверка границ, вместо голого
Marshal. До этого момента контракт restore — «вход ДОЛЖЕН быть нашим же
snapshot»; на недоверенном вводе поведение неопределено.

Если afl НЕ найдёт крэш за разумное время — это тоже полезно: повышает
уверенность, что на практике повреждённый чекпоинт чаще даёт `Failure`
(ловится), чем segfault. Но гарантии Marshal не даёт в любом случае.

## 2. TSan (гонки в exactly-once)

Нужен OCaml 5.x со switch, собранным с ThreadSanitizer:

```sh
opam switch create miniflink-tsan --packages=ocaml.5.2.0,ocaml-option-tsan --assume-depexts   # версия ВНУТРИ --packages
# или: opam install ocaml-variants.5.2.0+options ocaml-option-tsan

dune build --profile tsan test/tsan_parallel.exe
TSAN_OPTIONS="halt_on_error=1 second_deadlock_stack=1" \
  ./_build/tsan/test/tsan_parallel.exe
```

(Профиль `tsan` объявлен в `dune-workspace`. Главное — switch должен
быть собран с tsan-рантаймом; иначе соберите обычным `dune build` на
tsan-switch и запускайте бинарь напрямую.)

Тест гоняет `run_exactly_once` в 5 конфигурациях с высокой contention
(до 16 воркеров, барьер на каждом событии, общий счётчик под mutex).
Нагружает ровно пути, где мы чинили гонки: `failed[]` (C-1, Atomic),
`dlq count` (M-1, mutex), `crash_epoch` (M-3), `supervisor all_crits`
(H-2, mutex).

Ожидание: **0 предупреждений TSan**. Каждое предупреждение = гонка;
TSan напечатает оба стека доступа и различающий их вход. Тест заодно
проверяет корректность (ровно N выходов = exactly-once), так что
логическая регрессия тоже упадёт.

## Что делать с находками

- **fuzz-крэш в таргетах 1–2** → баг в `schema_default`, чинить декодер.
- **fuzz-крэш в таргете 3 (Marshal)** → ожидаемо; приоритезировать
  замену Marshal на safe-декодер в state-backend (см. выше).
- **TSan-warning** → гонка; стек укажет модуль. Чинить по образцу C-1
  (Atomic для флагов, Mutex для составного состояния).

Результаты стоит занести в `docs/known-issues.md`.
