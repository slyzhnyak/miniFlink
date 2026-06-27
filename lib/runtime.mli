(** Запуск конвейера: точка входа в исполнение.

    Связывает источник, пайплайн и sink, добавляя по режиму DLQ,
    метрики, graceful shutdown. Пользователь выбирает готовый
    {!config} ([noop]/[log_cfg]/[parallel]/[prod]) или собирает свой. *)

(** Режим работы. *)
type mode = Noop | Log | Parallel | Prod

(** Конфигурация запуска. *)
type config = {
  mode         : mode;
  parallelism  : int;       (** число воркеров (для Parallel/Prod) *)
  capacity     : int;       (** ёмкость входных каналов воркеров *)
  metrics_port : int;       (** порт Prometheus endpoint, 0 = выключено *)
}

(** Без DLQ/метрик/shutdown — для тестов и простых прогонов. *)
val noop : config

(** Метрики в stderr + graceful shutdown, один воркер. *)
val log_cfg : config

(** Параллельно (4 воркера), без HTTP-метрик. *)
val parallel : config

(** Прод: параллельно + Prometheus endpoint на :9090. *)
val prod : config

(** [run ?label cfg ~key_of ~source ~pipeline ~sink ()] исполняет
    [pipeline] над [source], отправляя результаты в [sink]. [key_of]
    шардирует поток по воркерам в параллельных режимах. Сопутствующее
    (DLQ, метрики, обработчик shutdown) включается согласно [cfg.mode].
    [?label] помечает метрики/логи этого пайплайна. *)
val run :
  ?label:string ->
  config ->
  key_of:('a -> string) ->
  source:'a Mf_event.t Stream.t ->
  pipeline:('a Mf_event.t Stream.t -> 'b Mf_event.t Stream.t) ->
  sink:('b -> unit) ->
  unit -> unit

(** [safe_decode ~send_dlq ~topic ~codec ~attempt raw] декодирует [raw]:
    при успехе [Some v], при ошибке отправляет запись в DLQ через
    [send_dlq] и возвращает [None] (битое сообщение не роняет пайплайн).
    Низкоуровневая деталь декодирования с DLQ; экспонирована для
    тестирования поведения «ошибка → DLQ, не падение». *)
val safe_decode :
  send_dlq:(Dlq_noop.entry -> unit) ->
  topic:string ->
  codec:(bytes -> ('a, string) result) ->
  attempt:int ->
  bytes -> 'a option
(** [make_stream_with_dlq cfg ~topic ~codec ~ts_of raw_source] —
    источник событий поверх сырого [raw_source], декодирующий каждый
    payload через [codec]. Битый payload отправляется в DLQ (по
    [cfg.mode]) и {b пропускается} — поток продолжается со следующего
    элемента, а не обрывается. Конец потока — только когда [raw_source]
    вернул [None]. Так одно битое сообщение не глушит весь источник. *)
val make_stream_with_dlq :
  config ->
  topic:string ->
  codec:(bytes -> ('a, string) result) ->
  ts_of:('a -> Time.t) ->
  (unit -> (string * bytes) option) ->
  'a Mf_event.t Stream.t

(** Мост из {!Config.t} (расширенная запись приложения: workers,
    checkpoint, state_dir...) в runtime-конфиг исполнения:
    [workers]→[parallelism], [capacity]→[capacity], метрики включаются
    если [metrics_interval_s > 0]. [?mode] задаёт режим (по умолчанию
    [Prod]) — {!Config.t} описывает {e ресурсы}, mode — {e режим}.
    Два типа дополняют друг друга, не конкурируют. *)
val of_config : ?mode:mode -> Config.t -> config
