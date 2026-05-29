#!/bin/bash
# Генерация odoc HTML документации вручную (обходит ограничение
# dune odoc с wrapped=false библиотеками — рендерит все модули).
set -e

dune build 2>/dev/null   # убедимся что .cmt свежие

OBJS="_build/default/lib/.miniflink.objs/byte"
OUT="_doc_build"
HTML="docs/api"

rm -rf "$OUT" "$HTML"
mkdir -p "$OUT" "$HTML"

# 1. Скомпилировать каждый модуль в .odoc
#    Предпочитаем .cmti (из .mli) если есть, иначе .cmt
for cmt in "$OBJS"/*.cmt; do
  base=$(basename "$cmt" .cmt)
  # пропускаем dune-внутренние
  case "$base" in
    miniflink|dune__exe*|.*) continue ;;
  esac
  src="$cmt"
  [ -f "$OBJS/$base.cmti" ] && src="$OBJS/$base.cmti"
  odoc compile "$src" -I "$OBJS" -o "$OUT/$base.odoc" 2>/dev/null || true
done

# 2. Скомпилировать .mld страницы
for mld in lib/index.mld lib/tutorial.mld; do
  base=$(basename "$mld" .mld)
  odoc compile "$mld" -I "$OUT" -o "$OUT/page-$base.odoc" 2>/dev/null || true
done

# 3. Линковать все .odoc
for odoc in "$OUT"/*.odoc; do
  odoc link "$odoc" -I "$OUT" -o "${odoc%.odoc}.odocl" 2>/dev/null || true
done

# 4. Рендерить HTML
for odocl in "$OUT"/*.odocl; do
  odoc html-generate "$odocl" -o "$HTML" 2>/dev/null || true
done

# 5. Support-файлы (CSS/JS)
odoc support-files -o "$HTML" 2>/dev/null || true

echo "Сгенерировано HTML: $(find "$HTML" -name '*.html' | wc -l) файлов"
find "$HTML" -name "*.html" | sort
