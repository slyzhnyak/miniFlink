(** Декларации триггеров для ex08.

    Главное в этом файле — каждый триггер это {b значение}, не код в
    каком-то общем автомате. Чтобы добавить новый триггер, достаточно
    дописать в этот файл ещё одну декларацию вида [Trigger.create
    ...]. Ничего больше нигде не править: ни источника, ни sink,
    ни остальные триггеры. Это и есть «ортогональность» которую
    требовала задача.

    Каждый триггер сразу несёт produce_alert/produce_recovery
    callback'и, конструирующие пользовательский {!Domain.alert}.
    Компилятор проверяет что конструктор существует и принимает
    правильные аргументы — опечатки в именах теперь невозможны. *)

open Miniflink
open Domain

(** Низкое напряжение — Warning с гистерезисом 3.5/3.7 и debounce
    2 минуты. Классический пример из ex07-FSM, теперь декларативно. *)
let low_voltage =
  Trigger.create
    ~name:"low_voltage"
    ~condition:(Trigger.less_than_with_hysteresis
                  ~problem:3.5 ~recovery:3.7)
    ~problem_for:(Time.minutes 2)
    ~severity:Trigger.Warning
    ~produce_alert:(fun ~key ~value ~ts ->
      Low_voltage { lamp = key; voltage = value; ts })
    ~produce_recovery:(fun ~key ~ts ->
      Voltage_ok { lamp = key; ts })
    ()

(** Критическое напряжение — Disaster при < 3.0 В. На один и тот же
    item.voltage_item подвешены ДВА триггера ({!low_voltage} и
    этот). Это естественно: они независимы. Никаких ``выберите один''
    компромиссов. *)
let voltage_critical =
  Trigger.create
    ~name:"voltage_critical"
    ~condition:(Trigger.less_than 3.0)
    ~problem_for:(Time.minutes 1)
    ~severity:Trigger.Disaster
    ~produce_alert:(fun ~key ~value ~ts ->
      Voltage_critical { lamp = key; voltage = value; ts })
    ~produce_recovery:(fun ~key ~ts ->
      Voltage_recovered { lamp = key; ts })
    ()

(** Нет пакетов — Warning после 2 минут тишины. Использует
    Item.silence_age для эмиссии «возраста тишины» (в Items.no_packets_item
    он уже сконвертирован в float), Trigger.greater_than для пороговой
    проверки. *)
let no_packets =
  Trigger.create
    ~name:"no_packets"
    ~condition:(Trigger.greater_than (float_of_int (Time.minutes 2)))
    ~severity:Trigger.Warning
    ~produce_alert:(fun ~key ~value ~ts ->
      No_packets { lamp = key; silence_ms = int_of_float value; ts })
    ~produce_recovery:(fun ~key ~ts ->
      Packets_resumed { lamp = key; ts })
    ()

(** Газ CO — Warning после 50 ppm с гистерезисом 40 ppm. *)
let gas_co_warning =
  Trigger.create
    ~name:"gas_co_warning"
    ~condition:(Trigger.greater_than_with_hysteresis
                  ~problem:50.0 ~recovery:40.0)
    ~severity:Trigger.Warning
    ~produce_alert:(fun ~key ~value ~ts ->
      Gas_co_warning { lamp = key; ppm = value; ts })
    ~produce_recovery:(fun ~key ~ts ->
      Gas_co_safe { lamp = key; ts })
    ()

(** Все триггеры одного семейства собраны в списки.
    Использование с {!Trigger.combine}: один stream на все
    voltage-триггеры. *)
let voltage_all = [low_voltage; voltage_critical]
