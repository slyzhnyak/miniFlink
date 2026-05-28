# ============================================================
# Makefile — miniFlink v3
# Автоматически выбирает реализации под OCaml 4 или 5
# ============================================================

# ── Определяем версию OCaml ──────────────────────────────────
OCAML_MAJOR := $(shell ocamlopt -version | cut -d. -f1)

ifeq ($(OCAML_MAJOR),5)
  OCAML_VER    := 5
  THREAD_FLAGS :=                        # Domain встроен в stdlib
  THREAD_LIBS  :=                        # не нужна threads.cmxa
  CHANNEL_IMPL := channel_v5.ml
  PARALLEL_IMPL:= parallel_v5.ml
  $(info → OCaml 5 detected: using Domain + Atomic)
else
  OCAML_VER    := 4
  THREAD_FLAGS := -I /usr/lib/ocaml/threads
  THREAD_LIBS  := threads.cmxa
  CHANNEL_IMPL := channel_v4.ml
  PARALLEL_IMPL:= parallel_v4.ml
  $(info → OCaml 4 detected: using Thread + Mutex)
endif

# ── Инструменты ──────────────────────────────────────────────
OCAMLFIND := ocamlfind ocamlopt
PKGS      := ppx_deriving_yojson,ppx_deriving_yojson.runtime,ppx_deriving.show,yojson
OCAMLOPT  := ocamlopt

FLAGS     := $(THREAD_FLAGS)
FIND_FLAGS:= $(FLAGS) -package $(PKGS)

# ── Ядро (не зависит от версии OCaml) ───────────────────────
CORE_ML := stream.ml time.ml mf_event.ml keyed.ml \
           table.ml pipe.ml codec.ml domain.ml rules.ml

CORE_CMX := stream.cmx time.cmx mf_event.cmx keyed.cmx \
            table.cmx pipe.cmx codec.cmx domain.cmx rules.cmx

# ── Выбираем реализации под версию ──────────────────────────
CHANNEL_CMX := channel.cmx
PARALLEL_CMX:= parallel.cmx

# ── Цели ─────────────────────────────────────────────────────
.PHONY: all clean check_version main bench bench_parallel

all: main bench bench_parallel

# Выбрать правильные реализации
channel.ml: $(CHANNEL_IMPL)
	cp $< $@

parallel.ml: $(PARALLEL_IMPL)
	cp $< $@

# ── Компиляция ядра ──────────────────────────────────────────
stream.cmx: stream.ml
	$(OCAMLOPT) -c stream.ml

time.cmx: time.ml
	$(OCAMLOPT) -c time.ml

mf_event.cmx: mf_event.ml stream.cmx time.cmx
	$(OCAMLOPT) -c mf_event.ml

keyed.cmx: keyed.ml
	$(OCAMLOPT) -c keyed.ml

table.cmx: table.ml stream.cmx mf_event.cmx
	$(OCAMLOPT) -c table.ml

pipe.cmx: pipe.ml stream.cmx mf_event.cmx keyed.cmx
	$(OCAMLOPT) -c pipe.ml

codec.cmx: codec.ml
	$(OCAMLFIND) $(FIND_FLAGS) -c codec.ml

domain.cmx: domain.ml codec.cmx keyed.cmx
	$(OCAMLFIND) $(FIND_FLAGS) -c domain.ml

rules.cmx: rules.ml domain.cmx pipe.cmx
	$(OCAMLFIND) $(FIND_FLAGS) -c rules.ml

# ── Параллельный слой ────────────────────────────────────────
channel.cmi: channel.mli stream.cmx
	$(OCAMLFIND) $(FIND_FLAGS) $(FLAGS) -c channel.mli

channel.cmx: channel.ml channel.cmi stream.cmx
	$(OCAMLFIND) $(FIND_FLAGS) $(FLAGS) -c channel.ml

parallel.cmi: parallel.mli channel.cmi mf_event.cmx
	$(OCAMLFIND) $(FIND_FLAGS) $(FLAGS) -c parallel.mli

parallel.cmx: parallel.ml parallel.cmi channel.cmx pipe.cmx mf_event.cmx
	$(OCAMLFIND) $(FIND_FLAGS) $(FLAGS) -c parallel.ml

# ── Приложения ───────────────────────────────────────────────
fixtures.cmx: fixtures.ml domain.cmx mf_event.cmx
	$(OCAMLFIND) $(FIND_FLAGS) -c fixtures.ml

main.cmx: main.ml $(CORE_CMX) fixtures.cmx
	$(OCAMLFIND) $(FIND_FLAGS) -c main.ml

bench.cmx: bench.ml $(CORE_CMX) fixtures.cmx
	$(OCAMLFIND) $(FIND_FLAGS) -c bench.ml

bench_parallel.cmx: bench_parallel.ml $(CORE_CMX) \
                    channel.cmx parallel.cmx
	$(OCAMLFIND) $(FIND_FLAGS) $(FLAGS) -c bench_parallel.ml

# ── Линковка ─────────────────────────────────────────────────
LINK := $(OCAMLFIND) $(FIND_FLAGS) -linkpkg \
        unix.cmxa $(THREAD_LIBS) \
        $(CORE_CMX)

main: main.cmx fixtures.cmx $(CORE_CMX) channel.ml parallel.ml
	$(LINK) fixtures.cmx main.cmx -o miniflink

bench: bench.cmx fixtures.cmx $(CORE_CMX)
	$(OCAMLFIND) $(FIND_FLAGS) -linkpkg \
	  unix.cmxa $(CORE_CMX) fixtures.cmx bench.cmx \
	  -o bench

bench_parallel: bench_parallel.cmx $(CORE_CMX) \
                channel.cmx parallel.cmx
	$(LINK) channel.cmx parallel.cmx bench_parallel.cmx \
	  -o bench_parallel

# ── Проверка версии ──────────────────────────────────────────
check_version:
	@echo "OCaml major version: $(OCAML_MAJOR)"
	@echo "Channel impl: $(CHANNEL_IMPL)"
	@echo "Parallel impl: $(PARALLEL_IMPL)"
	@ocamlopt -version

# ── Очистка ──────────────────────────────────────────────────
clean:
	rm -f *.cmx *.cmi *.o *.cmo
	rm -f miniflink bench bench_parallel
	rm -f channel.ml parallel.ml   # generated from v4/v5

# ── Помощь ───────────────────────────────────────────────────
help:
	@echo "Targets:"
	@echo "  all            — собрать всё"
	@echo "  main           — основное приложение"
	@echo "  bench          — single-thread benchmark"
	@echo "  bench_parallel — parallel benchmark"
	@echo "  check_version  — показать версию и выбранные impl"
	@echo "  clean          — удалить артефакты"
	@echo ""
	@echo "OCaml version: $(OCAML_MAJOR) → $(CHANNEL_IMPL), $(PARALLEL_IMPL)"
