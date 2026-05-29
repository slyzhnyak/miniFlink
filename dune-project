(lang dune 3.0)
(name miniflink)
(generate_opam_files false)

(package
 (name miniflink)
 (synopsis "Декларативная потоковая обработка на OCaml — учебный клон Apache Flink")
 (description
  "miniFlink реализует ключевые концепции Apache Flink — event time, watermarks, windowing, stateful operators, exactly-once, table/join, retractions — плюс production-слои (DLQ, graceful shutdown, Prometheus-метрики, RocksDB state backend). Параллелизм через Thread (OCaml 4) или Domain (OCaml 5). Декларативный pipeline через type class KEYED.")
 (depends ocaml dune yojson ppx_deriving_yojson qcheck))
