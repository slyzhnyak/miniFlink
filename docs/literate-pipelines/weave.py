#!/usr/bin/env python3
"""weave.py — собрать грамотный документ из .ml с @-разметкой (вариант B2).

Источник истины — сам .ml (компилируется как есть). Этот скрипт читает
файл сверху вниз и порождает LaTeX, чередуя прозу из блоков-комментариев
@doc / @section и листинги реального кода между ними. Код не извлекается
и не переставляется — документ следует порядку файла.

Разметка живёт в комментариях (* ... *):
  @title <...>      — заголовок документа (один раз, в шапке файла)
  @subtitle <...>   — подзаголовок
  @section <...>    — заголовок раздела
  @doc              — далее до конца комментария идёт проза (Markdown-лайт:
                      [ident] -> \\texttt{ident})
Любой код вне таких комментариев попадает в листинг.
"""
import re, sys, html

def esc_tex(s):
    for a, b in [('\\', r'\textbackslash{}'), ('&', r'\&'), ('%', r'\%'),
                 ('#', r'\#'), ('_', r'\_'), ('$', r'\$'),
                 ('{', r'\{'), ('}', r'\}'), ('~', r'\textasciitilde{}'),
                 ('^', r'\textasciicircum{}')]:
        s = s.replace(a, b)
    return s

def prose_to_tex(text):
    # [ident] -> \texttt{...}; «...» оставляем как есть (babel поймёт)
    out = []
    for para in re.split(r'\n\s*\n', text.strip()):
        para = para.strip()
        if not para:
            continue
        # инлайн-код [x] -> \texttt{x} (экранируем содержимое)
        def repl(m):
            return r'\texttt{' + esc_tex(m.group(1)) + '}'
        para = re.sub(r'\[([^\]]+)\]', repl, para)
        out.append(para)
    return '\n\n'.join(out)

def main(path):
    with open(path, encoding='utf-8') as f:
        src = f.read()

    title = subtitle = None
    blocks = []          # ('section', txt) | ('doc', txt) | ('code', txt)

    # Разобьём на комментарии (* ... *) и код между ними, по порядку.
    pos = 0
    code_buf = []
    def flush_code():
        if code_buf:
            chunk = ''.join(code_buf).strip('\n')
            if chunk.strip():
                blocks.append(('code', chunk))
            code_buf.clear()

    for m in re.finditer(r'\(\*(.*?)\*\)', src, flags=re.DOTALL):
        # код перед этим комментарием
        code_buf.append(src[pos:m.start()])
        flush_code()
        body = m.group(1)
        # вытащим директивы
        for line in [body]:
            tm = re.search(r'@title\s+(.+)', body)
            if tm and title is None:
                title = tm.group(1).strip()
            sm = re.search(r'@subtitle\s+(.+)', body)
            if sm and subtitle is None:
                subtitle = sm.group(1).strip()
        for secm in re.finditer(r'@section\s+(.+)', body):
            blocks.append(('section', secm.group(1).strip()))
        # @doc ... до конца комментария (после возможных @section/@title строк)
        dm = re.search(r'@doc\b(.*)$', body, flags=re.DOTALL)
        if dm:
            doc = dm.group(1)
            # убрать строки-директивы, попавшие после (нет — @doc последний)
            blocks.append(('doc', doc))
        pos = m.end()
    code_buf.append(src[pos:])
    flush_code()

    # Сгенерируем LaTeX
    L = []
    L.append(r'\documentclass[11pt,a4paper]{article}')
    L.append(r'\usepackage[utf8]{inputenc}')
    L.append(r'\usepackage[T2A]{fontenc}')
    L.append(r'\usepackage[english,russian]{babel}')
    L.append(r'\usepackage[margin=1in]{geometry}')
    L.append(r'\usepackage{listings}\usepackage{xcolor}')
    # literate-маппинг кириллицы: listings + inputenc utf8 не умеют
    # многобайтовые символы в коде, поэтому переводим каждую букву в
    # её T2A-команду (иначе русские комментарии в коде ломают сборку).
    lit = []
    lower = "абвгдежзийклмнопрстуфхцчшщъыьэюяё"
    upper = "АБВГДЕЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯЁ"
    cmds_l = [r'\cyra', r'\cyrb', r'\cyrv', r'\cyrg', r'\cyrd', r'\cyre',
              r'\cyrzh', r'\cyrz', r'\cyri', r'\cyrishrt', r'\cyrk', r'\cyrl',
              r'\cyrm', r'\cyrn', r'\cyro', r'\cyrp', r'\cyrr', r'\cyrs',
              r'\cyrt', r'\cyru', r'\cyrf', r'\cyrh', r'\cyrc', r'\cyrch',
              r'\cyrsh', r'\cyrshch', r'\cyrhrdsn', r'\cyrery', r'\cyrsftsn',
              r'\cyrerev', r'\cyryu', r'\cyrya', r'\cyryo']
    cmds_u = [r'\CYRA', r'\CYRB', r'\CYRV', r'\CYRG', r'\CYRD', r'\CYRE',
              r'\CYRZH', r'\CYRZ', r'\CYRI', r'\CYRISHRT', r'\CYRK', r'\CYRL',
              r'\CYRM', r'\CYRN', r'\CYRO', r'\CYRP', r'\CYRR', r'\CYRS',
              r'\CYRT', r'\CYRU', r'\CYRF', r'\CYRH', r'\CYRC', r'\CYRCH',
              r'\CYRSH', r'\CYRSHCH', r'\CYRHRDSN', r'\CYRERY', r'\CYRSFTSN',
              r'\CYREREV', r'\CYRYU', r'\CYRYA', r'\CYRYO']
    for ch, cmd in list(zip(lower, cmds_l)) + list(zip(upper, cmds_u)):
        lit.append('{%s}{{%s}}1' % (ch, cmd))
    L.append(r'\lstset{basicstyle=\ttfamily\small,frame=single,'
             r'backgroundcolor=\color[rgb]{0.97,0.97,0.97},'
             r'breaklines=true,columns=fullflexible,extendedchars=true,'
             r'rulecolor=\color[rgb]{0.8,0.8,0.8},xleftmargin=1em,xrightmargin=1em,'
             r'literate=' + '%\n  ' + ' '.join(lit) + '}')
    L.append(r'\title{' + esc_tex(title or 'Документ') +
             (r'\\ \large ' + esc_tex(subtitle) if subtitle else '') + '}')
    L.append(r'\date{}')
    L.append(r'\begin{document}\maketitle')
    L.append(r'\tableofcontents\bigskip')
    for kind, txt in blocks:
        if kind == 'section':
            L.append(r'\section{' + esc_tex(txt) + '}')
        elif kind == 'doc':
            L.append(prose_to_tex(txt))
        elif kind == 'code':
            L.append(r'\begin{lstlisting}')
            L.append(txt)
            L.append(r'\end{lstlisting}')
    L.append(r'\end{document}')
    sys.stdout.write('\n'.join(L) + '\n')

if __name__ == '__main__':
    main(sys.argv[1])
