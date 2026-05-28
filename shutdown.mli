(* ============================================================
   Shutdown.mli — graceful shutdown

   При SIGTERM:
   1. Остановить чтение из source
   2. Дочитать буферизованные события
   3. Сделать финальный checkpoint
   4. Flush sink (Kafka producer)
   5. Завершить процесс

   Реализации:
   shutdown_noop.ml    — игнорирует SIGTERM (подходит для тестов)
   shutdown_default.ml — регистрирует handler, корректно завершается
   ============================================================ *)

(** Зарегистрировать обработчик SIGTERM *)
val register : on_shutdown:(unit -> unit) -> unit

(** Проверить был ли получен сигнал завершения *)
val is_requested : unit -> bool

(** Заблокироваться до получения SIGTERM или явного вызова request() *)
val wait : unit -> unit

(** Явно запросить завершение (для тестов) *)
val request : unit -> unit
