#!/usr/bin/env bash
# coverage.sh — измерить покрытие тестами (bisect_ppx).
#
# lib/dune уже содержит (instrumentation (backend bisect_ppx)), поэтому
# покрытие включается флагом --instrument-with при прогоне тестов.
# Скрипт собирает .coverage-файлы и печатает сводку + (опционально) HTML.
#
# Требуется установленный bisect_ppx (ocamlfind list | grep bisect).
# Если его нет, см. docs/coverage.md — там как поставить из исходников
# в окружении без opam.
#
# Использование:
#   ./scripts/coverage.sh            # сводка в консоль
#   ./scripts/coverage.sh --html     # ещё и HTML-отчёт в _coverage/
set -euo pipefail

COV_DIR="$(mktemp -d)"
HTML_OUT="_coverage"

cleanup() { rm -rf "$COV_DIR"; }
trap cleanup EXIT

echo "Прогон тестов с инструментацией покрытия..."
BISECT_FILE="$COV_DIR/bisect" \
  dune runtest --instrument-with bisect_ppx --force

n=$(ls "$COV_DIR"/*.coverage 2>/dev/null | wc -l)
echo "Собрано $n coverage-файлов."
echo

echo "=== Сводка покрытия ==="
bisect-ppx-report summary --coverage-path "$COV_DIR"
echo
echo "=== По модулям (худшие 12) ==="
bisect-ppx-report summary --per-file --coverage-path "$COV_DIR" \
  | grep -oE "[0-9.]+ %.*lib/[a-z_]+\.ml" | sort -n | head -12

if [ "${1:-}" = "--html" ]; then
  rm -rf "$HTML_OUT"
  bisect-ppx-report html --coverage-path "$COV_DIR" -o "$HTML_OUT"
  echo
  echo "HTML-отчёт: $HTML_OUT/index.html"
fi
