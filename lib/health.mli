(** Health/readiness как {e структура}, не HTTP-сервер.

    Библиотека вычисляет статус здоровья (значением), а поднять
    [/health] endpoint и выбрать HTTP-стек — дело приложения. Так
    библиотека не тянет httpaf/dream на всех потребителей и не
    навязывает решение про сеть.

    Приложение периодически зовёт {!check} и сериализует результат
    ({!to_json}) в свой HTTP-ответ. *)

(** Готовность к работе. *)
type readiness =
  | Starting   (** ещё инициализируется *)
  | Ready      (** принимает и обрабатывает нагрузку *)
  | Draining   (** завершается (получен shutdown) *)
  | Unhealthy  (** есть проблема (см. [detail]) *)

(** Снимок состояния. *)
type status = {
  ready        : readiness;
  state_size   : int;          (** число ключей в стейте (грубая оценка нагрузки) *)
  watermark_lag_ms : int;      (** отставание watermark; большое = не успеваем *)
  max_queue_depth  : int;      (** макс. глубина очереди по воркерам *)
  detail       : string option;(** пояснение для Unhealthy/Draining *)
}

(** Источники данных для вычисления статуса. Приложение передаёт
    функции-замыкания на свои живые значения; health их опрашивает.
    Все опциональны — что не задано, считается нейтральным. *)
val check :
  ?readiness:(unit -> readiness) ->
  ?state_size:(unit -> int) ->
  ?watermark_lag_ms:(unit -> int) ->
  ?max_queue_depth:(unit -> int) ->
  unit -> status

(** Сериализовать статус в JSON-строку (для HTTP-ответа приложения). *)
val to_json : status -> string

(** Удобный предикат: считается ли статус «живым» для liveness-пробы
    (всё кроме [Unhealthy]). *)
val is_live : status -> bool
