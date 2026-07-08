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
# на каком switch мы сейчас? нужно нескольким секциям: fuzzing и TSan —
# разные switch'и, и собирать fuzz-таргет (тянет dune) на tsan-switch
# нельзя (dune пересоберётся под TSan и упадёт на GC-гонках рантайма).
CUR_SWITCH=$(command -v opam >/dev/null 2>&1 && opam switch show 2>/dev/null || echo "")
ON_TSAN_SWITCH=0
case "$CUR_SWITCH" in *tsan*) ON_TSAN_SWITCH=1 ;; esac
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
    # crowbar — только НЕ на tsan-switch (иначе dune ломается под TSan)
    if [ "$HAS_CROWBAR" = 0 ] && [ "$ON_TSAN_SWITCH" = 0 ]; then
      echo "  opam install crowbar (для afl-fuzzing) — вывод ниже:"
      if OPAMYES=1 opam install -y crowbar </dev/null 2>&1 | sed 's/^/    | /'; then
        ok "crowbar установлен"; HAS_CROWBAR=1
      else warn "не удалось поставить crowbar; пропущу"; fi
    elif [ "$HAS_CROWBAR" = 0 ] && [ "$ON_TSAN_SWITCH" = 1 ]; then
      warn "вы на tsan-switch ($CUR_SWITCH) — crowbar сюда НЕ ставлю."
      echo "      fuzzing (crowbar/afl) идёт на ОБЫЧНОМ switch, TSan — на этом."
      echo "      для fuzzing: opam switch <обычный> && opam install crowbar,"
      echo "      затем ./scripts/reliability.sh там."
    fi
    # tsan-switch — userspace, но ДОЛГО (собирает компилятор). Явно
    # предупреждаем и показываем прогресс НА ЭКРАН (не прячем в лог).
    if [ "$HAS_TSAN" = 0 ]; then
      if opam switch list 2>/dev/null | grep -q miniflink-tsan; then
        ok "switch miniflink-tsan уже существует — активирую"
        eval "$(opam env --switch=miniflink-tsan 2>/dev/null)" || true
        HAS_TSAN=1
      else
        # TSan-компилятору нужна системная библиотека libunwind-dev
        # (TSan использует libunwind для раскрутки стека). Проверяем
        # заранее — иначе сборка провалится на conf-unwind в середине.
        if ! pkg-config --exists libunwind 2>/dev/null \
           && ! ldconfig -p 2>/dev/null | grep -q libunwind; then
          warn "для TSan нужна системная библиотека libunwind-dev — её нет."
          echo "      поставьте её (нужен root), затем повторите --setup:"
          echo "        sudo apt install libunwind-dev"
          echo "      (без неё tsan-компилятор не соберётся — conf-unwind падает)"
        else
          echo "  создаю tsan-switch: компилирует OCaml 5.2.0 с нуля."
          echo "  ЭТО ДОЛГО — обычно 10-30 минут, зависит от машины."
          echo "  Прогресс opam идёт ниже (если строки движутся — всё идёт штатно):"
          echo "  ----------------------------------------------------------------"
          # НЕ используем --assume-depexts: libunwind-dev уже проверен выше.
          # </dev/null — страховка от вечного ожидания на неожиданном запросе.
          if OPAMYES=1 opam switch create miniflink-tsan \
               --packages=ocaml.5.2.0,ocaml-option-tsan \
               -y --no-install </dev/null 2>&1 \
               | sed 's/^/    | /'; then
            echo "  ----------------------------------------------------------------"
            eval "$(opam env --switch=miniflink-tsan)"
            echo "  opam install . --deps-only (зависимости проекта):"
            # На tsan-switch ЛЮБАЯ сборка инструментирована, включая сборку
            # инструментов (dune) и библиотек. GC OCaml даёт TSan-гонки
            # (minor_gc.c oldify_one) — это НЕ наш код. При СБОРКЕ говорим
            # TSan не ронять процесс из-за них (exitcode=0 report_bugs=0);
            # строгий режим включим только при ЗАПУСКЕ теста.
            TSAN_OPTIONS="halt_on_error=0 exitcode=0 report_bugs=0" \
              OPAMYES=1 opam install . --deps-only -y </dev/null 2>&1 \
              | sed 's/^/    | /' || true
            ok "tsan-switch готов и активирован"
            HAS_TSAN=1
          else
            echo "  ----------------------------------------------------------------"
            warn "не удалось создать tsan-switch (см. вывод выше)."
            echo "      если switch остался частичным — почистите:"
            echo "        opam switch remove miniflink-tsan"
            echo "      частая причина — устаревшие репозитории: opam update, потом повторите"
          fi
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
if [ "$ON_TSAN_SWITCH" = 1 ]; then
  warn "пропуск: вы на tsan-switch ($CUR_SWITCH). Fuzzing идёт на обычном"
  echo "      switch (сборка fuzz-таргета тянет dune, который под TSan"
  echo "      падает на GC-гонках). Для fuzzing переключитесь:"
  echo "        opam switch <обычный> && ./scripts/reliability.sh"
elif [ "$HAS_CROWBAR" = 1 ]; then
  echo "  сборка fuzz-таргета..."
  if dune build --profile fuzz fuzz/fuzz_crowbar.exe 2>/tmp/rel_fuzzbuild.log; then
    # путь к бинарю ищем через find, а не угадываем: имя profile-папки в
    # _build/ зависит от версии dune, из-за жёсткого пути afl-ветка
    # ошибочно пропускалась даже при установленном afl.
    FEXE=$(find _build -name fuzz_crowbar.exe -type f 2>/dev/null | head -1)
    if [ "$HAS_AFL" = 1 ] && [ -n "$FEXE" ] && [ -x "$FEXE" ]; then
      mkdir -p fuzz_in fuzz_out
      [ -f fuzz_in/seed ] || printf '\x00\x01hello' > fuzz_in/seed
      echo "  запуск afl-fuzz на ${FUZZ_SECS}с (бинарь: $FEXE; Ctrl-C прервёт)..."
      # AFL_ переменные обходят типичные блокеры старта afl++ без sudo:
      #  SKIP_CPUFREQ — не требовать performance-governor
      #  NO_AFFINITY — не привязываться к ядру
      #  I_DONT_CARE_ABOUT_MISSING_CRASHES — не требовать core_pattern=core
      #  SKIP_BIN_CHECK — не требовать afl-инструментацию бинаря. Наш
      #    бинарь инструментирован ТОЛЬКО если собран на afl-switch
      #    (ocaml-option-afl). Без него afl бы упал с "No instrumentation
      #    detected"; SKIP_BIN_CHECK разрешает dumb-mode (fuzzing без
      #    обратной связи по покрытию — слабее, но работает без 3-го switch).
      AFL_SKIP_CPUFREQ=1 AFL_NO_AFFINITY=1 \
      AFL_I_DONT_CARE_ABOUT_MISSING_CRASHES=1 AFL_SKIP_BIN_CHECK=1 \
        timeout "${FUZZ_SECS}" afl-fuzz -i fuzz_in -o fuzz_out -- "$FEXE" @@ \
        >/tmp/rel_afl.log 2>&1
      AFL_RC=$?
      # afl++ мог отказаться стартовать (не из-за находки) — распознаём
      if grep -qiE "PROGRAM ABORT|core_pattern.*abort|forkserver.*failed" /tmp/rel_afl.log \
         && ! find fuzz_out -path '*/crashes/id:*' 2>/dev/null | grep -q .; then
        warn "afl-fuzz не смог стартовать (настройка окружения, не находка). Причина:"
        grep -iE "PROGRAM ABORT|No instrumentation|core_pattern|Tips" /tmp/rel_afl.log | head -4
        if grep -qi "No instrumentation" /tmp/rel_afl.log; then
          echo "      бинарь без afl-инструментации. Для НАСТОЯЩЕГО afl нужен"
          echo "      afl-switch (весь код инструментируется), см. FUZZING.md:"
          echo "        opam switch create miniflink-afl --packages=ocaml.5.2.0,ocaml-option-afl"
          echo "      (dumb-mode должен был включиться через AFL_SKIP_BIN_CHECK —"
          echo "       если всё равно упал, проверьте версию afl: afl-fuzz -h)"
        elif grep -qi "core_pattern" /tmp/rel_afl.log; then
          echo "      core_pattern — разово: echo core | sudo tee /proc/sys/kernel/core_pattern"
        fi
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
  # СБОРКА: не ронять на GC-гонках рантайма (не наш код).
  if TSAN_OPTIONS="halt_on_error=0 exitcode=0 report_bugs=0" \
       dune build test/tsan_parallel.exe 2>/tmp/rel_tbuild.log; then
    echo "  запуск под TSan..."
    # ЗАПУСК: строго — тут гонки в exactly-once и есть предмет проверки.
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
