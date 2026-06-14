(** Триггеры в стиле Zabbix: декларативные условия + debounce + hysteresis.

    Триггерная система — {b ортогональное} дополнение к существующим
    пайплайнам. Не зависит от ex07-домена, не правит библиотечный код,
    не использует привилегированных API. Триггер — это
    функция от потока item-обновлений к потоку доменных алертов;
    можно подключить, не подключать, использовать вместе с FSM,
    использовать вместо FSM — выбор пользователя.

    {1 Концепция}

    Триггер описывается декларативно через {!type:spec}:
    - имя (для observability)
    - условие — {!type:condition} с предикатами check/recovery (для
      гистерезиса)
    - debounce: {!problem_for} перед эмиссией Problem, {!recovery_for}
      перед эмиссией Recovery
    - severity (Zabbix-уровни)
    - функции [produce_alert] / [produce_recovery] — конструируют
      пользовательский тип алертов

    Триггер имеет четыре внутренних состояния per-key:
    {ul
    {- [Ok] — условие не выполняется}
    {- [Pending_problem] — условие выполняется, ждём дозревания debounce}
    {- [Problem] — Problem-алерт эмитирован}
    {- [Pending_ok] — recovery-условие выполняется, ждём дозревания}}

    Переходы Ok→Problem и Problem→Ok происходят {b с задержкой} на
    [problem_for] и [recovery_for] соответственно. На Problem→Ok
    эмитится [Mf_event.Retract] последнего Problem плюс [Mf_event.Data]
    Recovery — sink через [Pipe.materialize] свернёт корректно.

    {1 Пример}

    {[
      let low_voltage = Trigger.create
        ~name:"low_voltage"
        ~condition:(Trigger.less_than_with_hysteresis
                      ~problem:3.5 ~recovery:3.7)
        ~problem_for:(Time.minutes 2)
        ~severity:Warning
        ~produce_alert:(fun ~key ~value ~ts ->
          Low_voltage (key, value, ts))
        ~produce_recovery:(fun ~key ~ts ->
          Voltage_ok (key, ts))
        ()
      in
      packets
      |> Pipe.map (fun p -> (p.lamp, p.voltage))
      |> Trigger.of_stream low_voltage
      |> ...sink
    ]}
*)

(** {1 Severity (Zabbix-совместимые уровни)} *)

type severity =
  | Not_classified
  | Info
  | Warning
  | Average
  | High
  | Disaster

val severity_name : severity -> string
(** Текстовое имя для логов: ["NOT_CLASSIFIED"], ["INFO"], … *)

(** {1 Условие триггера}

    Два предиката: [check] — переход OK→PROBLEM, [recovery] — переход
    PROBLEM→OK. Для триггеров без гистерезиса [recovery = not @@ check].
    Для гистерезиса (типа voltage 3.5/3.7) разные пороги.

    {b Инвариант} (не проверяется компилятором, ответственность
    пользователя): для любого значения [v] не должно быть одновременно
    [check v = true] и [recovery v = true]. Если случилось — приоритет
    у [check] (триггер останется в PROBLEM, более консервативно для
    safety-сценариев).
*)

type 'v condition = {
  check    : 'v -> bool;
  recovery : 'v -> bool;
}

val greater_than : float -> float condition
(** [check: v > t], [recovery: v <= t]. Без гистерезиса. *)

val less_than : float -> float condition
(** [check: v < t], [recovery: v >= t]. Без гистерезиса. *)

val greater_than_with_hysteresis :
  problem:float -> recovery:float -> float condition
(** Гистерезис. Требует [problem > recovery]: вход в PROBLEM при
    [v > problem], выход при [v < recovery]. *)

val less_than_with_hysteresis :
  problem:float -> recovery:float -> float condition
(** Гистерезис. Требует [problem < recovery]: вход в PROBLEM при
    [v < problem], выход при [v > recovery]. *)

val is_some : 'v option condition
(** [check: v = Some _], [recovery: v = None]. *)

val is_none : 'v option condition
(** [check: v = None], [recovery: v = Some _]. *)

val custom :
  problem:('v -> bool) -> recovery:('v -> bool) -> 'v condition
(** Произвольные предикаты. Соблюдение инварианта на пользователе. *)

(** {1 Декларация триггера} *)

type ('key, 'v, 'alert) spec
(** Описание триггера. Параметры:
    - ['key] — тип ключа партиционирования (обычно [string])
    - ['v] — тип значения item'а
    - ['alert] — пользовательский тип доменных алертов *)

val create :
  name:string ->
  condition:'v condition ->
  ?problem_for:Time.t ->
  ?recovery_for:Time.t ->
  ?severity:severity ->
  produce_alert:(key:'key -> value:'v -> ts:Time.t -> 'alert) ->
  produce_recovery:(key:'key -> ts:Time.t -> 'alert) ->
  ?serialize_key:('key -> Yojson.Safe.t) ->
  ?deserialize_key:(Yojson.Safe.t -> 'key) ->
  ?serialize_value:('v -> Yojson.Safe.t) ->
  ?deserialize_value:(Yojson.Safe.t -> 'v) ->
  ?serialize_alert:('alert -> Yojson.Safe.t) ->
  ?deserialize_alert:(Yojson.Safe.t -> 'alert) ->
  unit -> ('key, 'v, 'alert) spec
(** Сконструировать spec.

    [problem_for] (default 0) — сколько времени условие должно
    держаться, прежде чем эмитнется Problem. Защита от
    кратковременных всплесков.

    [recovery_for] (default 0) — debounce перед выходом из Problem.
    Защита от дребезга вокруг порога.

    [severity] (default [Not_classified]) — уровень для observability.

    [produce_alert] / [produce_recovery] — конструируют пользовательский
    [alert]-тип. На Problem-переходе вызывается [produce_alert];
    на Recovery-переходе [Mf_event.retract] последнего Problem-alert'а
    плюс [Mf_event.data] от [produce_recovery].

    {b Опциональные сериализаторы} ([serialize_*]/[deserialize_*])
    необходимы, если триггер запускается с persistence (см.
    [?backend] параметр в [of_stream]). Без backend'а они
    игнорируются. Если backend подключён, а хотя бы один
    сериализатор отсутствует — [of_stream] выбросит
    [Invalid_argument]. *)

val name     : ('key, 'v, 'alert) spec -> string
val severity : ('key, 'v, 'alert) spec -> severity

(** {1 Основной оператор} *)

val of_stream :
  ?backend:State_backend.t ->
  ('key, 'v, 'alert) spec ->
  ('key * 'v) Mf_event.t Stream.t ->
  'alert Mf_event.t Stream.t
(** Применить триггер к потоку item-обновлений.

    Семантика:
    - На каждое [Data ((key, value), ts)] обновляет per-key state
      и (возможно) эмитит [Data alert] / [Retract alert].
    - [Retract] в upstream игнорируются (триггер реагирует только
      на свежие значения).
    - [Watermark] прокидывается в downstream; перед прокидкой
      обрабатываются все накопленные таймеры дебаунса с [t ≤ wm].

    Эмиссии:
    - Ok→Problem: [Data (produce_alert ...)]
    - Problem→Ok: [Retract (last_emitted_problem)] +
                  [Data (produce_recovery ...)]
    - Pending-переходы (внутри debounce) ничего не эмитят.

    Эмитируемые события идут в event-time порядке (определяется
    upstream watermark'ами + моментами срабатывания таймеров).

    {b [?backend]} — если передан, триггер сохраняет и восстанавливает
    своё состояние (state machine per key + last_event_ts + pending
    timers) через [State_backend]. На старте читает все ключи с
    префиксом ["trigger:{name}:"] и восстанавливает state. На каждое
    изменение state-машины пишет обратно в backend.

    Требует чтобы в [spec] были заполнены все [serialize_*] и
    [deserialize_*] поля. Иначе [Invalid_argument].

    Без backend'а — поведение как раньше (state в памяти, теряется
    при завершении процесса). *)

val combine :
  ?backend:State_backend.t ->
  ('key, 'v, 'alert) spec list ->
  ('key * 'v) Mf_event.t Stream.t ->
  'alert Mf_event.t Stream.t
(** Запустить несколько триггеров параллельно на одном item-потоке.

    Каждый триггер видит все обновления, эмитит свои Problem/Recovery
    независимо. Все триггеры в списке должны иметь одинаковый
    ['alert] (естественное ограничение для слияния в один поток).

    Под капотом — независимые per-(trigger, key) state-таблицы;
    триггеры не интерферируют.

    [?backend], если передан, разделяется всеми триггерами в списке.
    Они различаются по [name] и не интерферируют в backend'е по
    ключам. *)
