(** Пайплайны примера 7: чистые stream-преобразования над {!Domain.packet}.

    Никакого I/O — ни источника, ни sink. Это позволяет:
    - переиспользовать пайплайны в бенчмарках (другой источник, тот же
      граф вычислений);
    - тестировать пайплайны изолированно (передать `Stream.of_list ...`);
    - подменять моки реальными Kafka-коннекторами без изменения этого
      модуля.

    Два независимых пайплайна:
    - {!median_rssi}: локация шахтёра по медиане RSSI в скользящем окне;
    - {!connectivity_alerts}: четыре независимых FSM + импульсный SOS. *)

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

(** {1 Контроль состояния шахтёра: 3 слоя + 4 FSM} *)

(* ════════════════════════════════════════════════════════════════
   Четыре НЕЗАВИСИМЫХ подсистемы фонаря — связь, движение, батарея,
   SOS-кнопка. У шахтёра могут падать одновременно несколько; каждая
   требует своей реакции, поэтому НЕ один автомат с пятью состояниями
   (где одно подавит другое), а ТРИ независимых FSM плюс импульсное
   событие SOS.

   Структура трёх слоёв:

     Layer 1 — Last_seen:  ЧТО ИЗВЕСТНО (факты о шахтёре)
     Layer 2 — Self_timer: КОГДА ПРОВЕРИТЬ (один таймер на ключ,
                           переменная цель = ближайшая смена состояния)
     Layer 3 — Fsm:        ЧТО ДЕЛАТЬ (три чистые функции перехода)

   Зависимость подавления: motion и voltage смотрят на contact_state.
   Если No_packets, они возвращают «Ok» — нет смысла сигналить, мы
   просто не знаем.

   Гистерезис и debounce напряжения: три состояния (Ok / Suspect /
   Low_voltage); Suspect — внутренний, наружу не эмитим. Вход в
   Suspect при V<low; через debounce, если V всё ещё низко, переход
   в Low_voltage и эмиссия. Выход в Ok только при V>ok (гистерезис).

   Late пакеты не сдвигают last_* поля назад во времени (монотонность);
   только late, который реально новее уже зафиксированного, обновляет
   их вперёд.
   ════════════════════════════════════════════════════════════════ *)

(** ─── Layer 1: Last_seen — хранилище фактов ────────────────── *)
module Last_seen = struct
  type t = {
    mutable any           : Time.t option;
    mutable with_readings : Time.t option;
    mutable moving        : Time.t option;
    mutable voltage       : float option;
    mutable suspect_low_since : Time.t option;
    mutable sos           : bool;
  }

  let make () = {
    any = None; with_readings = None; moving = None;
    voltage = None; suspect_low_since = None; sos = false;
  }

  (** Зарегистрировать пришедший пакет. Возвращает [`Sos_pressed] если
      это переход кнопки false→true (импульс).

      {b Монотонность по времени}: поля last_* обновляются только если
      [p.ts > current] — late не сдвигает зафиксированные «свежие»
      факты назад. *)
  let record t ~(p : packet) : [ `Sos_pressed | `Quiet ] =
    let bumps_max old_v = match old_v with
      | None -> true
      | Some t' -> p.ts > t' in
    let was_freshest = bumps_max t.any in
    if was_freshest then t.any <- Some p.ts;
    if p.readings <> [] && bumps_max t.with_readings then
      t.with_readings <- Some p.ts;
    if p.moving && bumps_max t.moving then t.moving <- Some p.ts;
    if was_freshest || t.voltage = None then
      t.voltage <- Some p.voltage;
    if p.voltage < low_voltage_threshold && t.suspect_low_since = None then
      t.suspect_low_since <- Some p.ts
    else if p.voltage > voltage_ok_threshold then
      t.suspect_low_since <- None;
    let sos_pressed = p.sos && not t.sos in
    t.sos <- p.sos;
    if sos_pressed then `Sos_pressed else `Quiet
end

(** ─── Layer 2: Single_timer — один логический таймер на ключ ────
    Вынесен в библиотеку (Pipe.Single_timer, разрыв G-3 закрыт). *)
module Self_timer = Pipe.Single_timer

(** ─── Layer 3: Fsm — три чистые функции перехода ───────────── *)
module Fsm = struct
  type contact_state = Contact_ok | C_no_readings | C_no_packets
  type motion_state  = Motion_ok  | M_no_motion
  type voltage_state = V_ok       | V_suspect | V_low

  let contact_state ~now (s : Last_seen.t) : contact_state =
    let exhausted ~since ~threshold = match since with
      | None -> true | Some t -> now - t >= threshold in
    if exhausted ~since:s.any ~threshold:no_packets_threshold then C_no_packets
    else if exhausted ~since:s.with_readings ~threshold:no_readings_threshold then C_no_readings
    else Contact_ok

  let motion_state ~now (s : Last_seen.t) : motion_state =
    match contact_state ~now s with
    | C_no_packets -> Motion_ok
    | _ ->
      match s.moving with
      | None -> M_no_motion
      | Some t when now - t >= no_motion_threshold -> M_no_motion
      | Some _ -> Motion_ok

  let voltage_state ~now (s : Last_seen.t) : voltage_state =
    match contact_state ~now s with
    | C_no_packets -> V_ok
    | _ ->
      match s.voltage, s.suspect_low_since with
      | None, _ -> V_ok
      | Some v, _ when v > voltage_ok_threshold -> V_ok
      | Some _, Some since when now - since >= voltage_debounce -> V_low
      | Some _, Some _ -> V_suspect
      | Some _, None -> V_ok

  (** Ближайший момент, когда состояние какой-либо подсистемы может
      измениться. Минимум из 4 порогов; max_int если ничего не запланировано. *)
  let next_check (s : Last_seen.t) : Time.t =
    let after since threshold = (Option.value since ~default:0) + threshold in
    let contact_check =
      min (after s.any no_packets_threshold)
          (after s.with_readings no_readings_threshold) in
    let motion_check = after s.moving no_motion_threshold in
    let voltage_check = match s.suspect_low_since with
      | Some since -> since + voltage_debounce
      | None -> max_int in
    min contact_check (min motion_check voltage_check)
end

(** ─── Клей: process_keyed связывает три слоя ────────────────── *)
type per_lamp = {
  seen          : Last_seen.t;
  timer         : Self_timer.t;
  mutable last_contact : Fsm.contact_state;
  mutable last_motion  : Fsm.motion_state;
  mutable last_voltage : Fsm.voltage_state;
  mutable max_now      : Time.t;
}

let make_state () = {
  seen         = Last_seen.make ();
  timer        = Self_timer.make ();
  last_contact = Fsm.Contact_ok;
  last_motion  = Fsm.Motion_ok;
  last_voltage = Fsm.V_ok;
  max_now      = min_int;
}

(** Проверить все три FSM, эмитнуть на переходе. Не пересчитывает FSM
    «в прошлое» относительно уже виденного [max_now]. *)
let check_and_emit ctx key st ~now =
  if now <= st.max_now then ()
  else begin
    st.max_now <- now;
    let c = Fsm.contact_state ~now st.seen in
    if c <> st.last_contact then begin
      st.last_contact <- c;
      match c with
      | Fsm.C_no_packets  -> ctx.Pipe.emit (No_packets  (key, st.seen.any))
      | Fsm.C_no_readings -> ctx.Pipe.emit (No_readings (key, st.seen.with_readings))
      | Fsm.Contact_ok    -> ()
    end;
    let m = Fsm.motion_state ~now st.seen in
    if m <> st.last_motion then begin
      st.last_motion <- m;
      match m with
      | Fsm.M_no_motion -> ctx.Pipe.emit (No_motion (key, st.seen.moving))
      | Fsm.Motion_ok   -> ()
    end;
    let v = Fsm.voltage_state ~now st.seen in
    if v <> st.last_voltage then begin
      st.last_voltage <- v;
      match v, st.seen.voltage with
      | Fsm.V_low, Some volts -> ctx.Pipe.emit (Low_voltage (key, volts))
      | Fsm.V_ok, _           -> ctx.Pipe.emit (Voltage_ok key)
      | Fsm.V_suspect, _ | Fsm.V_low, None -> ()
    end
  end

(** [connectivity_alerts packets] — поток алертов о состоянии шахтёра.

    Один [process_keyed] на каждого шахтёра, три независимых FSM плюс
    SOS-импульс. Использует pull-modэль таймеров miniflink: при тишине
    источника нужен [Pipe.event_time] перед оператором, чтобы watermark
    двигался. *)
let connectivity_alerts (source : packet Mf_event.t Stream.t)
    : alert Mf_event.t Stream.t =
  source
  |> Pipe.event_time ~lateness:(seconds 1)
  |> Pipe.process_keyed (module ByLamp)
       ~init:make_state
       ~on_event:(fun ctx key st p ->
         (match Last_seen.record st.seen ~p with
          | `Sos_pressed -> ctx.Pipe.emit (Sos (key, p.ts))
          | `Quiet -> ());
         check_and_emit ctx key st ~now:p.ts;
         Self_timer.reschedule st.timer ctx ~target:(Fsm.next_check st.seen))
       ~on_timer:(fun ctx key st t _kind ->
         Self_timer.consumed st.timer;
         check_and_emit ctx key st ~now:t;
         let next = Fsm.next_check st.seen in
         if next > t && next < max_int then
           Self_timer.reschedule st.timer ctx ~target:next)


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
