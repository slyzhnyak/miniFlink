#!/usr/bin/env bash
# reliability.sh — прогнать тесты надёжности одной командой:
#   fuzzing декодеров (Crowbar/afl) + гонки (ThreadSanitizer).
#
# Скрипт САМ определяет, что доступно в окружении, и прогоняет то, что
# может, честно сообщая про пропущенное. Ничего не устанавливает без
# спроса и не притворяется, что прогнал то, чего не мог.
#
# Использование:
#   ./scripts/reliability.sh            # всё доступное
#   ./scripts/reliability.sh --quick    # только быстрый sanity (без afl-циклов)
#   ./scripts/reliability.sh --setup    # доустановить недостающее, потом прогнать
#   ./scripts/reliability.sh --fuzz-secs 120   # длительность afl-фаззинга (по умолч. 60)
#
# --setup ставит то, что можно безопасно в userspace (crowbar через opam,
# tsan-switch — компилирует компилятор, НЕСКОЛЬКО МИНУТ). sudo-вещи (apt
# install afl++, настройка core_pattern) НЕ выполняет за вас — только
# показывает команды, т.к. они требуют root и меняют систему.
#
# Что прогоняется:
#   1. быстрый sanity — всегда (обычный switch): fuzz_decoders (QCheck) +
#      tsan_parallel (корректность exactly-once);
#   2. afl-fuzzing декодеров — если установлены crowbar и afl-fuzz;
#   3. TSan — если текущий switch собран с ThreadSanitizer.
#
# Требования для полного прогона (скрипт подскажет, если чего-то нет):
#   opam install crowbar
#   afl (afl-fuzz в PATH)
#   switch с ocaml-option-tsan (Linux x86-64, OCaml >= 5.2)
set -uo pipefail

# ── аргументы ──
QUICK=0
FUZZ_SECS=60
SETUP=0
while [ $# -gt 0 ]; do
  case "$1" in
    --quick)      QUICK=1; shift ;;
    --setup)      SETUP=1; shift ;;
    --fuzz-secs)  FUZZ_SECS="${2:-60}"; shift 2 ;;
    -h|--help)
      sed -n '2,32p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "неизвестный аргумент: $1"; exit 2 ;;
  esac
done

cd "$(dirname "$0")/.." || exit 1

# ── цвета/маркеры (без цвета, если не терминал) ──
if [ -t 1 ]; then B=$'\033[1m'; G=$'\033[32m'; Y=$'\033[33m'; R=$'\033[31m'; N=$'\033[0m'
else B=''; G=''; Y=''; R=''; N=''; fi
ok()   { echo "  ${G}✓${N} $*"; }
warn() { echo "  ${Y}‣${N} $*"; }
err()  { echo "  ${R}✗${N} $*"; }
hdr()  { echo; echo "${B}== $* ==${N}"; }

FAILED=0

# ── 0. проверка toolchain ──
hdr "Окружение"
have() { command -v "$1" >/dev/null 2>&1; }
HAS_CROWBAR=0; HAS_AFL=0; HAS_TSAN=0
if have opam && opam list --installed 2>/dev/null | grep -q '^crowbar '; then HAS_CROWBAR=1; fi
if have afl-fuzz; then HAS_AFL=1; fi
# TSan: инструментация вшита в компилятор switch. Определяем по наличию
# опции в switch, либо по runtime_variant тестового бинаря позже.
if have opam && opam list --installed 2>/dev/null | grep -qE '^ocaml-option(s-only)?-tsan|tsan'; then HAS_TSAN=1; fi

[ "$HAS_CROWBAR" = 1 ] && ok "crowbar установлен" || warn "crowbar НЕ установлен (afl-fuzzing пропущу; opam install crowbar)"
if [ "$HAS_AFL" = 1 ]; then ok "afl-fuzz в PATH"
else warn "afl-fuzz НЕ найден → afl-fuzzing пропущу (будет быстрый Crowbar-режим)."
     echo "      установка: opam install afl-persistent && sudo apt install afl++"
     echo "      (afl-инструментация уже в профиле fuzz — отдельный switch НЕ нужен)"
fi
[ "$HAS_TSAN" = 1 ] && ok "switch с tsan-опцией" || warn "switch БЕЗ tsan (гонки не проверю; см. dune-workspace)"

# ── опциональный автосетап (--setup) ──
# Ставит то, что можно безопасно (opam-пакеты в userspace) и создаёт
# tsan-switch. sudo-вещи (apt, core_pattern) НЕ трогает автоматически —
# только показывает команды, т.к. они требуют root и меняют систему.
if [ "$SETUP" = 1 ]; then
  hdr "Автосетап (--setup): доустановка недостающего"
  if ! have opam; then
    err "opam не найден — установите opam вручную, потом повторите"
  else
    # crowbar — userspace, безопасно ставить
    if [ "$HAS_CROWBAR" = 0 ]; then
      echo "  opam install crowbar (для afl-fuzzing)..."
      if opam install -y crowbar >/tmp/rel_setup_cb.log 2>&1; then
        ok "crowbar установлен"; HAS_CROWBAR=1
      else warn "не удалось поставить crowbar (лог /tmp/rel_setup_cb.log); пропущу"; fi
    fi
    # tsan-switch — userspace, но ДОЛГО (собирает компилятор). Явно
    # предупреждаем и делаем, раз пользователь попросил --setup.
    if [ "$HAS_TSAN" = 0 ]; then
      if opam switch list 2>/dev/null | grep -q miniflink-tsan; then
        ok "switch miniflink-tsan уже существует — активирую"
        eval "$(opam env --switch=miniflink-tsan 2>/dev/null)" || true
        HAS_TSAN=1
      else
        echo "  создаю tsan-switch (компилирует OCaml 5.2.0 — это НЕСКОЛЬКО МИНУТ)..."
        if opam switch create miniflink-tsan \
             --packages=ocaml.5.2.0,ocaml-option-tsan -y >/tmp/rel_setup_tsan.log 2>&1; then
          eval "$(opam env --switch=miniflink-tsan)"
          echo "  opam install . --deps-only..."
          opam install . --deps-only -y >>/tmp/rel_setup_tsan.log 2>&1 || true
          ok "tsan-switch готов и активирован"
          HAS_TSAN=1
        else
          warn "не удалось создать tsan-switch (лог /tmp/rel_setup_tsan.log)."
          echo "      частая причина — устаревшие репозитории: opam update, потом повторите"
        fi
      fi
    fi
  fi
  # sudo-часть — НЕ делаем за пользователя, показываем:
  if [ "$HAS_AFL" = 0 ]; then
    warn "afl-fuzz требует системной установки (root) — выполните сами:"
    echo "      sudo apt install afl++      # положит afl-fuzz в PATH"
    have afl-fuzz && HAS_AFL=1
  fi
fi

# ── 1. быстрый sanity (всегда) ──
hdr "1. Быстрый sanity (без спец-инструментов)"

echo "  сборка..."
if ! dune build test/fuzz_decoders.exe test/tsan_parallel.exe 2>build.log; then
  err "сборка провалилась:"; cat build.log; rm -f build.log; exit 1
fi
rm -f build.log

echo "  fuzz_decoders (QCheck, 15000 случаев)..."
if dune exec test/fuzz_decoders.exe >/tmp/rel_fuzz.log 2>&1 \
   && grep -q "properties held" /tmp/rel_fuzz.log; then
  ok "декодеры: все свойства держатся (15000 случаев; лог /tmp/rel_fuzz.log)"
else
  err "fuzz_decoders упал или свойство нарушено:"; tail -8 /tmp/rel_fuzz.log; FAILED=1
fi

echo "  tsan_parallel (корректность exactly-once под нагрузкой)..."
if dune exec test/tsan_parallel.exe >/tmp/rel_tsan.log 2>&1; then
  ok "exactly-once корректен во всех конфигурациях"
  grep -q "TSan НЕ активен" /tmp/rel_tsan.log && \
    warn "это обычный switch — ГОНКИ здесь НЕ проверены (см. шаг 3)"
else
  err "tsan_parallel упал:"; tail -5 /tmp/rel_tsan.log; FAILED=1
fi

if [ "$QUICK" = 1 ]; then
  hdr "Готово (--quick)"
  [ "$FAILED" = 0 ] && ok "sanity пройден" || err "были падения"
  exit $FAILED
fi

# ── 2. afl-fuzzing декодеров ──
hdr "2. Fuzzing декодеров (afl + Crowbar)"
if [ "$HAS_CROWBAR" = 1 ]; then
  echo "  сборка fuzz-таргета..."
  if dune build --profile fuzz fuzz/fuzz_crowbar.exe 2>/tmp/rel_fuzzbuild.log; then
    FEXE="_build/fuzz/fuzz/fuzz_crowbar.exe"
    if [ "$HAS_AFL" = 1 ] && [ -x "$FEXE" ]; then
      mkdir -p fuzz_in fuzz_out
      [ -f fuzz_in/seed ] || printf '\x00\x01hello' > fuzz_in/seed
      echo "  запуск afl-fuzz на ${FUZZ_SECS}с (Ctrl-C прервёт)..."
      # AFL_ переменные обходят типичные блокеры старта afl++ без sudo:
      #  SKIP_CPUFREQ — не требовать performance-governor
      #  NO_AFFINITY — не привязываться к ядру
      #  I_DONT_CARE_ABOUT_MISSING_CRASHES — не требовать core_pattern=core
      #    (иначе afl++ откажется стартовать без sudo-настройки системы)
      AFL_SKIP_CPUFREQ=1 AFL_NO_AFFINITY=1 \
      AFL_I_DONT_CARE_ABOUT_MISSING_CRASHES=1 \
        timeout "${FUZZ_SECS}" afl-fuzz -i fuzz_in -o fuzz_out -- "$FEXE" @@ \
        >/tmp/rel_afl.log 2>&1
      AFL_RC=$?
      # afl++ мог отказаться стартовать (не из-за находки) — распознаём
      if grep -qiE "PROGRAM ABORT|no instrumentation|core_pattern|forkserver" /tmp/rel_afl.log \
         && ! find fuzz_out -path '*/crashes/id:*' 2>/dev/null | grep -q .; then
        warn "afl-fuzz не смог стартовать (не находка, а настройка окружения). Вывод:"
        grep -iE "PROGRAM ABORT|core_pattern|instrumentation|Tips|echo core" /tmp/rel_afl.log | head -6
        echo "      частый случай — core_pattern; разово поправляется:"
        echo "        echo core | sudo tee /proc/sys/kernel/core_pattern"
        echo "      (полный лог: /tmp/rel_afl.log)"
      else
        CRASHES=$(find fuzz_out -path '*/crashes/id:*' 2>/dev/null | grep -c . || echo 0)
        if [ "$CRASHES" -gt 0 ]; then
          err "afl нашёл $CRASHES крэш(ей)! входы в fuzz_out/*/crashes/ — это находка"
          FAILED=1
        else
          ok "afl отработал ${FUZZ_SECS}с, крэшей нет (fuzz_out/)"
        fi
      fi
    else
      # без afl — быстрый Crowbar-режим (случайная генерация)
      echo "  afl нет — быстрый Crowbar-режим..."
      if timeout 60 dune exec --profile fuzz fuzz/fuzz_crowbar.exe >/tmp/rel_cb.log 2>&1; then
        ok "Crowbar-прогон без падений (без afl-глубины)"
      else
        # Crowbar при находке выходит !=0
        err "Crowbar нашёл падение:"; tail -8 /tmp/rel_cb.log; FAILED=1
      fi
    fi
  else
    err "fuzz-таргет не собрался:"; tail -5 /tmp/rel_fuzzbuild.log; FAILED=1
  fi
else
  warn "пропуск: нет crowbar (opam install crowbar, затем перезапустите)"
fi

# ── 3. TSan (гонки) ──
hdr "3. Гонки (ThreadSanitizer)"
if [ "$HAS_TSAN" = 1 ]; then
  echo "  сборка под tsan-switch (инструментация в компиляторе)..."
  if dune build test/tsan_parallel.exe 2>/tmp/rel_tbuild.log; then
    echo "  запуск под TSan..."
    if TSAN_OPTIONS="halt_on_error=1 second_deadlock_stack=1" \
         ./_build/default/test/tsan_parallel.exe >/tmp/rel_tsanrun.log 2>&1; then
      if grep -q "TSan active" /tmp/rel_tsanrun.log; then
        ok "TSan активен, гонок не обнаружено"
      else
        warn "тест не увидел TSan-инструментацию — switch точно с ocaml-option-tsan?"
        warn "(проверьте: opam list | grep tsan; см. dune-workspace)"
      fi
    else
      err "TSan сообщил о проблеме (гонка или крэш) — вывод:"
      grep -A20 "WARNING: ThreadSanitizer" /tmp/rel_tsanrun.log | head -25 || tail -20 /tmp/rel_tsanrun.log
      FAILED=1
    fi
  else
    err "сборка под tsan не удалась:"; tail -5 /tmp/rel_tbuild.log; FAILED=1
  fi
else
  warn "пропуск: switch без tsan. Как включить:"
  echo "      opam switch create miniflink-tsan --packages=ocaml.5.2.0,ocaml-option-tsan"
  echo "      eval \$(opam env --switch=miniflink-tsan) && opam install . --deps-only"
  echo "      (если пакеты не найдены — сначала: opam update)"
  echo "      затем перезапустите этот скрипт"
fi

# ── итог ──
hdr "Итог"
if [ "$FAILED" = 0 ]; then
  ok "всё, что доступно в этом окружении, прошло"
  [ "$HAS_CROWBAR" = 0 ] || [ "$HAS_TSAN" = 0 ] && \
    warn "часть проверок пропущена (см. выше) — для полного прогона доустановите инструменты"
  exit 0
else
  err "были падения/находки — разберите вывод выше; детали в /tmp/rel_*.log"
  exit 1
fi
