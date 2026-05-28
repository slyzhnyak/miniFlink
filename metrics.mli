(* ============================================================
   Metrics.mli — observability интерфейс

   Три реализации:
   metrics_noop.ml    — заглушка (0 overhead)
   metrics_log.ml     — периодический вывод в stderr
   metrics_otel.ml    — OpenTelemetry / Prometheus экспорт
   ============================================================ *)

(** Счётчик — только растёт *)
type counter

(** Измеритель — может расти и падать *)
type gauge

(** Гистограмма для latency *)
type histogram

val counter   : name:string -> labels:(string * string) list -> counter
val gauge     : name:string -> labels:(string * string) list -> gauge
val histogram : name:string -> labels:(string * string) list -> histogram

val incr      : counter -> unit
val add       : counter -> int -> unit
val set_gauge : gauge -> float -> unit
val observe   : histogram -> float -> unit

(** Снапшот всех метрик в текстовом формате (Prometheus exposition) *)
val dump : unit -> string

(** Периодически писать метрики в лог (каждые interval_s секунд) *)
val start_reporter : interval_s:int -> unit
