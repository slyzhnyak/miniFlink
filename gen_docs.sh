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

# 6. PDF API-референс (склейка HTML-страниц через wkhtmltopdf).
#    odoc latex-generate завязан на внутренний odoc.sty, которого нет
#    в дистрибутиве — поэтому идём через HTML→PDF, это надёжнее.
if command -v wkhtmltopdf >/dev/null && command -v pdfunite >/dev/null; then
  PDFTMP=$(mktemp -d)
  ORDER="index Stream Time Mf_event Keyed Table Pipe Codec Domain Rules Channel Parallel Checkpoint_parallel Runtime Harness"
  i=0
  for m in $ORDER; do
    src="$HTML/$m/index.html"; [ "$m" = "index" ] && src="$HTML/index.html"
    [ -f "$src" ] || continue
    wkhtmltopdf --quiet --enable-local-file-access "$src" \
      "$(printf "%s/%03d.pdf" "$PDFTMP" "$i")" >/dev/null 2>&1 && i=$((i+1))
  done
  # остальные модули (production-слои)
  for d in "$HTML"/*/index.html; do
    base=$(basename "$(dirname "$d")")
    case " $ORDER " in *" $base "*) continue ;; esac
    wkhtmltopdf --quiet --enable-local-file-access "$d" \
      "$(printf "%s/%03d.pdf" "$PDFTMP" "$i")" >/dev/null 2>&1 && i=$((i+1))
  done
  pdfunite "$PDFTMP"/*.pdf docs/miniflink_api.pdf 2>/dev/null \
    && echo "Сгенерирован PDF: docs/miniflink_api.pdf ($(pdfinfo docs/miniflink_api.pdf 2>/dev/null | awk '/Pages/{print $2}') стр.)"
  rm -rf "$PDFTMP"
else
  echo "PDF пропущен (нужны wkhtmltopdf + pdfunite)"
fi
