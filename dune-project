(lang dune 3.0)
(name miniflink)
(version 0.1.0)
(generate_opam_files true)

(source (github slyzhnyak/miniFlink))
(license MIT)
(authors "slyzhnyak")
(maintainers "slyzhnyak")

(package
 (name miniflink)
 (synopsis "Декларативный streaming DSL на OCaml — семантика Apache Flink на одном узле")
 (description
  "miniFlink реализует ключевые концепции Apache Flink — event time, watermarks, windowing (tumbling/sliding/count/session/global+triggers) с инкрементальной агрегацией, stateful operators, exactly-once (offset + 2PC sink + recovery + durable), table/join с TTL, union потоков, retractions — плюс production-слои (DLQ + retry, graceful shutdown, Prometheus-метрики, структурированные логи, health, config, RocksDB state backend). Параллелизм через Thread (OCaml 4) или Domain (OCaml 5). Декларативный pipeline через type class KEYED. Распределённость вне области проекта — это single-node движок.")
 (depends
  (ocaml (>= 4.14))
  dune
  (yojson (>= 1.7))
  ppx_deriving_yojson
  (qcheck :with-test)))
