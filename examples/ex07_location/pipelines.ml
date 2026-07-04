(** Пайплайны примера 7: чистые stream-преобразования над {!Domain.packet}.

    Никакого I/O — ни источника, ни sink. Это позволяет:
    - переиспользовать пайплайны в бенчмарках (другой источник, тот же
      граф вычислений);
    - тестировать пайплайны изолированно (передать `Stream.of_list ...`);
    - подменять моки реальными Kafka-коннекторами без изменения этого
      модуля.

    Три пайплайна:
    - {!median_rssi}: локация шахтёра по медиане RSSI в скользящем окне;
    - {!connectivity_alerts}: декларативная композиция детекторов
      (on_silence × 3 + Trigger с гистерезисом + suppress_while + SOS);
    - {!gas_alerts}: газовые алерты через co_process3 с обогащением
      координатами (retract/update-семантика). *)

open Miniflink
open Time
open Domain

module ByLamp = Keyed.Make (struct type t = packet let key p = p.lamp end)

(** {1 Расчёт позиции шахтёра по RSSI} *)

(** Расстояние от шахтёра до маяка по RSSI. Инверсия формулы из
    [Mock_source.Large_mine]: RSSI = -40 - 20·log10(d).  Минимум 1м чтобы
    избежать log(0). *)
let distance_from_rssi (rssi : float) : float =
  10. ** ((-. rssi -. 40.) /. 20.)

(** Приближённая позиция шахтёра по двум сильнейшим маякам через
    линейную интерполяцию.

    Алгоритм: известны (x_a, y_a, d_a) и (x_b, y_b, d_b) — координаты
    двух маяков и расстояния до них (из RSSI). Точку на отрезке
    пропорционально расстояниям: t = d_a / (d_a + d_b),
    pos = (1-t)·A + t·B.

    {b Геометрия задачи.} Шахтный штрек — линия, биконы расставлены
    вдоль неё через равные интервалы, шахтёр находится в самом
    тоннеле между двумя соседними биконами. Линейная интерполяция —
    {e корректный} для такой геометрии метод, не упрощение. Трилатерация
    на 3+ маяков (стандартная техника для 2D-плоскости снаружи) здесь
    не работает: маяки лежат на одной прямой, окружности их покрытия
    симметричны относительно оси штрека, и нелинейная система либо
    вырождается либо неоднозначна.

    {b Источники неточности.} RSSI шумный (±4 dB → ~50% по расстоянию
    на одном пакете) — поэтому берём медиану в окне 60с. Глубина (h)
    берётся от первого маяка — шахтёр на одном горизонте со своими
    биконами.

    [find_beacon] — функция поиска координат маяка по ID. {!Domain.beacon_coords}
    для базового сценария, [Hashtbl.find_opt (Large_mine.beacons_table cfg)]
    для большой шахты. *)
let interpolate_position
    ~(find_beacon : string -> (float * float * int) option)
    (top2 : (string * float) list)
  : (float * float * int) option =
  match top2 with
  | (b_a, rssi_a) :: (b_b, rssi_b) :: _ ->
    (match find_beacon b_a, find_beacon b_b with
     | Some (xa, ya, ha), Some (xb, yb, _) ->
       let d_a = distance_from_rssi rssi_a in
       let d_b = distance_from_rssi rssi_b in
       let total = d_a +. d_b in
       if total = 0. then Some (xa, ya, ha)
       else
         let t = d_a /. total in
         let x = (1. -. t) *. xa +. t *. xb in
         let y = (1. -. t) *. ya +. t *. yb in
         Some (x, y, ha)
     | _ -> None)
  | _ -> None
  (* Меньше 2 маяков — позиции нет: одного для интерполяции мало. *)

(** Грубая позиция по ОДНОМУ ближайшему маяку — fallback для
    raw RSSI-пакета. Используется в [gas_alerts] чтобы получить
    позицию сразу после первого RSSI-пакета шахтёра, не дожидаясь
    закрытия RSSI-окна. Возвращает координаты самого маяка — шахтёр
    где-то на сфере радиуса d вокруг него. *)
let single_beacon_position
    ~(find_beacon : string -> (float * float * int) option)
    (readings : (string * float) list)
  : (float * float * int) option =
  match readings with
  | [] -> None
  | rs ->
    (* Сильнейший = максимум RSSI (RSSI отрицательный, -40 > -90). *)
    let (b, _) = List.fold_left
      (fun ((_, best) as acc) ((_, r) as cur) ->
         if r > best then cur else acc)
      (List.hd rs) (List.tl rs) in
    find_beacon b

(** {1 Локация: median RSSI в скользящем окне} *)

(** [median_rssi packets] — для каждого 5-секундного окна выдаёт топ-2
    маяков с самой сильной median RSSI для каждого шахтёра.

    Конвейер:
    + [Pipe.dedup] по [(lamp, ts)] до окна — at-least-once канал отдаёт
      дубль того же пакета; без dedup дубль исказил бы median;
    + [flat_map] разворачивает пакет в показания (шахтёр, маяк, RSSI);
    + [event_time] с [lateness:1с] вставляет watermark;
    + [window_agg_keyed] окно ключа [lamp] (sliding 60с/5с,
      [allowed_lateness:60с]) — одно показание попадает в 12 окон;
    + внутри окна {!Agg.group_by} по маяку (median RSSI на маяк) +
      пост-фильтр по [None] + {!Agg.top_k_by} 2.

    На выходе — поток {!location} событий по каждому закрытию окна
    (включая Retract от опоздавших пакетов; sink применяет их через
    [Pipe.materialize] чтобы получить финальную картину).

    [?find_beacon] — функция поиска координат маяка для вычисления
    [loc_position]. По умолчанию {!Domain.beacon_coords} (5 маяков для
    базового сценария). Для большой шахты передавайте
    [Hashtbl.find_opt (Large_mine.beacons_table cfg)]. *)
let median_rssi
    ?(find_beacon = Domain.beacon_coords)
    (source : packet Mf_event.t Stream.t)
    : location Mf_event.t Stream.t =
  source
  |> Pipe.dedup (module ByLamp)
       ~rule:(fun p -> string_of_int p.ts)
       ~cooldown:(seconds 120)
  |> Pipe.flat_map (fun p ->
       List.map (fun (b, rssi) ->
         { r_lamp = p.lamp; r_beacon = b; r_rssi = rssi; r_ts = p.ts })
         p.readings)
  |> Pipe.event_time ~lateness:(seconds 1)
  |> Pipe.window_agg_keyed ~by:(fun r -> r.r_lamp)
       ~allowed_lateness:(seconds 60)
       (Pipe.sliding (seconds 60) (seconds 5))
       Agg.(
         group_by
           ~key:(fun r -> r.r_beacon)
           ~inner:(median (fun r -> r.r_rssi))
         |> map (fun per_beacon ->
              per_beacon
              |> List.filter_map (fun (b, med) ->
                   match med with Some m -> Some (b, m) | None -> None)
              |> Agg.run (top_k_by 2 ~by:snd)))
  (* window_agg_keyed эмитит (Data (lamp, top2)) с ts=конец окна.
     map_ts даёт доступ к ts (концу окна) прямо в конструкторе Location,
     сохраняя вид события (Data/Retract/Update) — раньше здесь был
     ручной разбор всех четырёх конструкторов (закрытый разрыв G-1). *)
  |> Pipe.map_ts (fun (lamp, top2) wend ->
       { loc_lamp = lamp; loc_wend = wend; loc_top2 = top2;
         loc_position = interpolate_position ~find_beacon top2 })

(** {1 Контроль состояния шахтёра: декларативная композиция} *)

(* ════════════════════════════════════════════════════════════════
   Четыре независимых подсистемы фонаря — связь, движение, батарея,
   SOS. Раньше это были три переплетённых ручных FSM (Last_seen /
   Single_timer / Fsm, ~150 строк) с подавлением, вшитым в состояния.

   Теперь — композиция первичных операторов (разрывы G-2a/G-2b):
     - Pipe.on_silence   — отсутствие (нет пакетов/показаний/движения);
     - Trigger           — напряжение с гистерезисом и debounce;
     - Pipe.suppress_while — подавление (motion/voltage молчат без связи);
     - map/filter        — импульсный SOS.
   Вся сборка — в connectivity_alerts ниже.
   ════════════════════════════════════════════════════════════════ *)
(** [connectivity_alerts packets] — поток алертов о состоянии шахтёра.

    Декларативная композиция пяти детекторов (разрывы G-2a/G-2b закрыты):

    - три {!Pipe.on_silence} для отсутствия (нет пакетов / показаний /
      движения) — absence как первичная операция, а не ручной FSM;
    - {!Trigger} с гистерезисом и debounce для напряжения — та самая
      механика, что жила в проекте, но раньше не композировалась;
    - импульсный SOS через map+filter по переходу кнопки.

    Подавление (motion и voltage молчат, пока «нет пакетов» — связи нет,
    сигналить нечем) выражено {!Pipe.suppress_while}, а не переплетением
    состояний. No_packets-поток используется дважды: как выход и как
    controller подавления — поэтому источник и no_packets-поток
    разветвляются через {!Stream.split}.

    Источник размечается {!Pipe.event_time}: absence и Trigger работают
    по event-time, watermark двигает детекцию тишины. *)
let connectivity_alerts (source : packet Mf_event.t Stream.t)
    : alert Mf_event.t Stream.t =
  let timed = source |> Pipe.event_time ~lateness:(seconds 1) in
  (* источник нужен нескольким детекторам + no_packets — дважды
     (выход и controller подавления). Разветвляем. *)
  let srcs = Stream.split 6 timed in
  let s_no_packets  = List.nth srcs 0 in
  let s_no_readings = List.nth srcs 1 in
  let s_no_motion   = List.nth srcs 2 in
  let s_voltage     = List.nth srcs 3 in
  let s_sos         = List.nth srcs 4 in
  let s_np_ctrl     = List.nth srcs 5 in

  (* ── Absence-детекторы ──────────────────────────────────── *)
  let no_packets =
    s_no_packets
    |> Pipe.on_silence (module ByLamp)
         ~within:no_packets_threshold
         ~has:(fun _ -> true)          (* любой пакет = контакт *)
         ~on_silent:(fun k ~last ~ts:_ -> No_packets (k, last)) in

  let no_readings =
    s_no_readings
    |> Pipe.on_silence (module ByLamp)
         ~within:no_readings_threshold
         ~has:(fun p -> p.readings <> [])   (* пакет с маяками *)
         ~on_silent:(fun k ~last ~ts:_ -> No_readings (k, last)) in

  let no_motion =
    s_no_motion
    |> Pipe.on_silence (module ByLamp)
         ~within:no_motion_threshold
         ~has:(fun p -> p.moving)           (* пакет с движением *)
         ~on_silent:(fun k ~last ~ts:_ -> No_motion (k, last)) in

  (* ── Voltage: Trigger с гистерезисом + debounce ─────────── *)
  let voltage =
    let low_voltage = Trigger.create
      ~name:"low_voltage"
      ~condition:(Trigger.less_than_with_hysteresis
                    ~problem:low_voltage_threshold
                    ~recovery:voltage_ok_threshold)
      ~problem_for:voltage_debounce
      ~severity:Trigger.Warning
      ~produce_alert:(fun ~key ~value ~ts:_ -> Low_voltage (key, value))
      ~produce_recovery:(fun ~key ~ts:_ -> Voltage_ok key)
      () in
    s_voltage
    |> Pipe.map (fun p -> (p.lamp, p.voltage))
    |> Trigger.of_stream low_voltage in

  (* ── SOS: импульс на переходе кнопки false→true ─────────── *)
  let sos =
    s_sos
    |> Pipe.filter (fun p -> p.sos)
    |> Pipe.map_ts (fun p ts -> Sos (p.lamp, ts))
    |> Pipe.dedup (module Keyed.Make (struct
         type t = alert
         let key = function
           | Sos (k, _) -> k | _ -> "" end))
         ~rule:(function Sos (k, t) -> k ^ string_of_int t | _ -> "")
         ~cooldown:0 in

  (* ── Подавление: motion и voltage молчат, пока «нет пакетов» ──
     No_packets-controller включает подавление ([`On]) на Data-алерте
     No_packets и снимает ([`Off]) на его Retract (recovery контакта). *)
  let np_alerts_ctrl =
    s_np_ctrl
    |> Pipe.on_silence (module ByLamp)
         ~within:no_packets_threshold
         ~has:(fun _ -> true)
         ~on_silent:(fun k ~last ~ts:_ -> No_packets (k, last))
         ~on_resumed:(fun k ~ts:_ -> No_packets (k, None)) in
  let np_gate = function
    | No_packets (k, Some _) -> `On k    (* тишина наступила — подавляем *)
    | No_packets (k, None)   -> `Off k   (* контакт возобновился — снимаем *)
    | _ -> `Ignore in
  let np_key = function No_packets (k, _) -> Some k | _ -> None in
  let alert_key = function
    | No_packets (k, _) | No_readings (k, _) | No_motion (k, _)
    | Low_voltage (k, _) | Voltage_ok k | Sos (k, _) -> k in

  let suppress s =
    Pipe.suppress_while
      ~controller_key:np_key ~gate:np_gate ~suppressed_key:alert_key
      np_alerts_ctrl s in

  (* ── Слияние всех детекторов ────────────────────────────── *)
  Mf_event.union no_packets
    (Mf_event.union no_readings
       (Mf_event.union (suppress no_motion)
          (Mf_event.union (suppress voltage) sos)))


(** {1 Газовые алерты с обогащением координатами} *)

(* ════════════════════════════════════════════════════════════════
   Газовый пайплайн: объединяем три потока в один через union-type,
   обрабатываем в едином process_keyed.

   Дизайн почему НЕ window join:
   - Газовый алерт — safety, должен идти НЕМЕДЛЕННО (без ожидания пары)
   - Частоты разные (10с газ vs 5с окно RSSI) → window join даёт дубли
   - Координаты могут прийти ПОЗЖЕ первого газового алерта — и тогда
     алерт должен ОБНОВИТЬСЯ через retract+emit, не запоздать

   Делаем temporal-left-join через keyed state: gas — driver, position —
   side input. В state помним последнюю позицию + активные алерты.
   ════════════════════════════════════════════════════════════════ *)

(** State per шахтёра: последняя известная позиция + активные алерты
    (по одному на (gas) пока шахтёр в опасной зоне). *)
type gas_state = {
  mutable position : (float * float * int) option;
  active : (gas, active_alert) Hashtbl.t;
}
and active_alert = {
  aa_level    : gas_level;
  aa_ppm      : float;
  aa_position : (float * float * int) option;
}

let make_gas_state () = { position = None; active = Hashtbl.create 4 }

(** Извлечь (gas, ppm) пары из пакета — только те газы, что измерены. *)
let gases_in_packet (g : gas_packet) : (gas * float) list =
  let acc = ref [] in
  (match g.g_ch4 with Some v -> acc := (Gas_CH4, v) :: !acc | None -> ());
  (match g.g_h2  with Some v -> acc := (Gas_H2,  v) :: !acc | None -> ());
  (match g.g_co  with Some v -> acc := (Gas_CO,  v) :: !acc | None -> ());
  (match g.g_co2 with Some v -> acc := (Gas_CO2, v) :: !acc | None -> ());
  !acc

(** Существенно ли изменился ppm — порог 20% относительно старого. *)
let ppm_changed_significantly ~old_ppm ~new_ppm =
  if old_ppm = 0. then new_ppm > 0.
  else abs_float (new_ppm -. old_ppm) /. old_ppm > Domain.gas_ppm_significant_change

(** Обработка одного газа из пакета. Возвращает список эмиссий. *)
let process_one_gas
    (st : gas_state) (lamp : string) (ts : Time.t)
    (g : gas) (ppm : float)
  : gas_alert Mf_event.t list =
  let cur_level = Domain.gas_level_for g ppm in
  let prev = Hashtbl.find_opt st.active g in
  match cur_level, prev with
  | None, None ->
    (* в норме, не было алерта → ничего *)
    []
  | None, Some old ->
    (* был алерт, теперь норма → retract + Gas_resolved *)
    Hashtbl.remove st.active g;
    let retract_ev = Mf_event.retract
      (Gas_alert {
         ga_lamp = lamp; ga_ts = ts; ga_gas = g;
         ga_level = old.aa_level; ga_ppm = old.aa_ppm;
         ga_position = old.aa_position }) ts in
    let resolved_ev = Mf_event.data
      (Gas_resolved { gr_lamp = lamp; gr_ts = ts; gr_gas = g }) ts in
    [retract_ev; resolved_ev]
  | Some level, None ->
    (* новый алерт *)
    let aa = { aa_level = level; aa_ppm = ppm;
               aa_position = st.position } in
    Hashtbl.add st.active g aa;
    [ Mf_event.data
        (Gas_alert {
           ga_lamp = lamp; ga_ts = ts; ga_gas = g;
           ga_level = level; ga_ppm = ppm;
           ga_position = st.position }) ts ]
  | Some level, Some old ->
    (* был алерт. Refresh нужен если: смена уровня, заметное изменение
       ppm, или обновилась position (которую sink может показать). *)
    let level_changed = level <> old.aa_level in
    let ppm_changed = ppm_changed_significantly ~old_ppm:old.aa_ppm ~new_ppm:ppm in
    let position_changed = st.position <> old.aa_position in
    if not (level_changed || ppm_changed || position_changed) then []
    else begin
      let new_aa = { aa_level = level; aa_ppm = ppm;
                     aa_position = st.position } in
      Hashtbl.replace st.active g new_aa;
      (* Замена old→new одного и того же алерта — атомарный Update, а не
         пара Retract+Data: downstream видит коррекцию за один шаг, без
         промежуточного «алерт исчез» (None-flicker). *)
      let old_alert = Gas_alert {
        ga_lamp = lamp; ga_ts = ts; ga_gas = g;
        ga_level = old.aa_level; ga_ppm = old.aa_ppm;
        ga_position = old.aa_position } in
      let new_alert = Gas_alert {
        ga_lamp = lamp; ga_ts = ts; ga_gas = g;
        ga_level = level; ga_ppm = ppm;
        ga_position = st.position } in
      [ Mf_event.update old_alert new_alert ts ]
    end

(** При обновлении позиции — пройтись по активным алертам и для каждого
    выпустить refresh (retract старого с position_then + data с новой).
    Это и есть «когда координаты приходят позже, газовый алерт обновляется». *)
let refresh_alerts_for_new_position
    (st : gas_state) (lamp : string) (ts : Time.t)
  : gas_alert Mf_event.t list =
  let to_emit = ref [] in
  Hashtbl.iter (fun g old ->
    if st.position <> old.aa_position then begin
      (* Тот же алерт, но с обновлённой позицией — атомарный Update
         (замена), а не Retract+Data. *)
      let old_alert = Gas_alert {
        ga_lamp = lamp; ga_ts = ts; ga_gas = g;
        ga_level = old.aa_level; ga_ppm = old.aa_ppm;
        ga_position = old.aa_position } in
      let new_alert = Gas_alert {
        ga_lamp = lamp; ga_ts = ts; ga_gas = g;
        ga_level = old.aa_level; ga_ppm = old.aa_ppm;
        ga_position = st.position } in
      to_emit := Mf_event.update old_alert new_alert ts :: !to_emit;
      Hashtbl.replace st.active g { old with aa_position = st.position }
    end
  ) st.active;
  List.rev !to_emit

(** Основной газовый пайплайн.

    Три входа (позиция из окна, позиция из raw-пакета, показания газа)
    co-обрабатываются на общем per-lamp состоянии через
    {!Pipe.co_process3}. Каждый обработчик эмитит готовые события
    (Data/Retract/Update) через [ctx.emit_event] — retract-семантика
    (алерт отзывается при возврате к норме, обновляется атомарным Update
    при смене позиции) выражается прямо в DSL.

    Исторически этот пайплайн был написан вручную (union-тег + свой
    Hashtbl + Queue + pull-цикл), потому что ctx.emit умел только
    значения; разрывы G-4/G-5 закрыты (emit_event + co_process3).

    [?find_beacon] — для расчёта грубой позиции из raw-пакета
    (single-beacon fallback). Default — {!Domain.beacon_coords}. *)
let gas_alerts
    ?(find_beacon = Domain.beacon_coords)
    ~(rssi : packet Mf_event.t Stream.t)
    ~(locations : location Mf_event.t Stream.t)
    ~(gas : gas_packet Mf_event.t Stream.t)
    () : gas_alert Mf_event.t Stream.t =
  (* co_process3 объединяет три разнотипных входа на общем per-lamp
     состоянии (закрытый разрыв G-5). Раньше здесь были ~90 строк
     ручного каркаса: union-тег, свой Hashtbl состояний, Queue, pull-цикл
     по четырём конструкторам. Теперь — три доменных обработчика, каждый
     эмитит готовые события через ctx.emit_event (helper'ы возвращают
     gas_alert Mf_event.t list с Data/Retract/Update). *)
  let emit_all ctx evs = List.iter ctx.Pipe.emit_event evs in
  Pipe.co_process3
    ~init:make_gas_state
    ~key_a:(fun (l : location) -> l.loc_lamp)
    ~key_b:(fun (p : packet) -> p.lamp)
    ~key_c:(fun (g : gas_packet) -> g.g_lamp)
    (* A: позиция из окна медианы RSSI *)
    ~on_a:(fun ctx _lamp st loc ->
      match loc.loc_position with
      | None -> ()
      | Some _ as new_pos ->
        if new_pos <> st.position then begin
          st.position <- new_pos;
          emit_all ctx
            (refresh_alerts_for_new_position st loc.loc_lamp loc.loc_wend)
        end)
    (* B: позиция из одиночного маяка raw-пакета (только если позиции ещё нет) *)
    ~on_b:(fun ctx _lamp st pkt ->
      match st.position, single_beacon_position ~find_beacon pkt.readings with
      | None, (Some _ as new_pos) ->
        st.position <- new_pos;
        emit_all ctx
          (refresh_alerts_for_new_position st pkt.lamp pkt.ts)
      | _ -> ())
    (* C: показания газа → алерты/резолвы/апдейты *)
    ~on_c:(fun ctx _lamp st g ->
      gases_in_packet g
      |> List.iter (fun (gas_id, ppm) ->
           emit_all ctx (process_one_gas st g.g_lamp g.g_ts gas_id ppm)))
    locations rssi gas
