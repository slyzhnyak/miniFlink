(** Декларация эвакуационного триггера.

    Multi-condition trigger через [Trigger.custom]: predicate
    смотрит на все три измерения combined-record одновременно.
    Hysteresis на каждом измерении — recovery срабатывает когда
    {b любое} условие отпустилось ниже своего hysteresis-порога.

    Это и есть точка где наш подход показывает выразительность по
    сравнению с inline-DSL Zabbix: predicate — это произвольная
    OCaml-функция, компилятор проверяет все типы. *)

open Miniflink
open Domain

(* Пороги. Реальная критическая ситуация - CO > 50, voltage < 3.5,
   avg_rssi < -75 (слабая связь). Hysteresis-пороги выбраны с
   запасом, чтобы избежать flapping у границ. *)
let co_problem      = 50.    (* ppm *)
let co_recovery     = 45.    (* hysteresis *)
let v_problem       = 3.5    (* V *)
let v_recovery      = 3.7    (* hysteresis *)
let rssi_problem    = -75.   (* dBm *)
let rssi_recovery   = -70.   (* hysteresis *)

(** Predicate problem: все три условия выполнены {b одновременно}, и
    данные по всем трём измерениям {b есть}. Это последний пункт
    критичен: до того как первое значение CO пришло (например, газ
    у шахтёра не измеряется), мы не должны выдавать алерт от
    init-значения. *)
let is_problem (c : combined) =
  c.has_co && c.has_voltage && c.has_rssi
  && c.co_ppm   > co_problem
  && c.voltage  < v_problem
  && c.avg_rssi < rssi_problem

(** Predicate recovery: {b любое} условие отпустилось до своего
    hysteresis-порога. Это даёт правильную семантику — ситуация
    улучшилась если хоть одна из угроз снизилась. *)
let is_recovery (c : combined) =
  c.co_ppm   <= co_recovery
  || c.voltage  >= v_recovery
  || c.avg_rssi >= rssi_recovery

(** Сам триггер. *)
let evacuation =
  Trigger.create
    ~name:"evacuation_critical"
    ~condition:(Trigger.custom ~problem:is_problem ~recovery:is_recovery)
    ~problem_for:(Time.minutes 1)
    ~recovery_for:(Time.seconds 30)
    ~severity:Trigger.Disaster
    ~produce_alert:(fun ~key ~value ~ts ->
      Evacuation_critical {
        lamp    = key;
        co      = value.co_ppm;
        voltage = value.voltage;
        rssi    = value.avg_rssi;
        ts;
      })
    ~produce_recovery:(fun ~key ~ts ->
      Evacuation_cleared { lamp = key; ts })
    ()
