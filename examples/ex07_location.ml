(* ============================================================
   Пример 7 — локация шахтёра по RSSI маяков связи.

   Каждые 15 секунд фонарь шахтёра шлёт пакет: ID фонаря + до пяти
   самых слышимых маяков с их RSSI. Маяки слышны только в пределах
   своего горизонта (один уровень шахты). Сервис делает две вещи:

   ЛОКАЦИЯ (скользящее окно минута / шаг 5 секунд):
     1. flat_map: пакет → показания (шахтёр, маяк, RSSI);
     2. window_agg_keyed по «шахтёр|маяк» → median RSSI по каждой паре
        в окне минуты (median гасит шум радиоканала);
        allowed_lateness 30с — опоздавший пакет вызывает РЕТРАКТ и
        пересчёт уже закрытых окон;
     3. для КАЖДОГО окна каждого шахтёра — два маяка с сильнейшим
        median RSSI (вход для трилатерации);
     4. обогащение координатами и горизонтом из справочника узлов.

     Особенность скользящего окна 60s/5s: одно событие попадает в 12
     окон, поэтому локация обновляется каждые 5 секунд, хотя пакеты
     приходят раз в 15.

   ДВА АЛЕРТА (process_keyed + event-time таймеры):
     A. «нет пакетов от фонаря >2 мин»  — heartbeat по факту приёма
        пакета (сбрасывается ЛЮБЫМ пакетом, включая пустой);
     B. «не слышит ни одного маяка >5 мин» — heartbeat по показаниям
        (сбрасывается только пакетом с показаниями).

   Граничные случаи в данных:
     - M3 слышит маяки только до t=20с, дальше шлёт ПУСТЫЕ пакеты:
       алерт B сработает (нет показаний 5 мин), алерт A — нет;
     - M4 слышит только ОДИН маяк → честное предупреждение, для
       трилатерации этого мало;
     - M5 шлёт пакеты до t=100с, потом ЗАМОЛКАЕТ: сработает алерт A
       (нет пакетов >2 мин);
     - один пакет M1 приходит С ОПОЗДАНИЕМ ~25с → ретракции
       пересчитывают закрытые окна (счётчик в выводе).

   Замечание о ключе: пара шахтёр×маяк — склейка "lamp|beacon";
   известная ловушка коллизии (см. роадмап «Композитные ключи»)
   здесь безопасна: ID без разделителя.
   ============================================================ *)

open Miniflink
open Time

(* ── Модель ───────────────────────────────────────────────── *)

type packet = {
  lamp     : string;                  (* ID фонаря шахтёра *)
  readings : (string * float) list;   (* до 5: (ID маяка, RSSI dBm) *)
  voltage  : float;                   (* напряжение фонаря, вольты *)
  moving   : bool;                    (* было ли движение с прошлого пакета *)
  sos      : bool;                    (* нажата ли кнопка SOS *)
  ts       : Time.t;
}

type reading = { r_lamp : string; r_beacon : string; r_rssi : float; r_ts : Time.t }

(* справочник маяков: ID → (x, y, горизонт) *)
let beacons = [
  "B1", (120.0,  40.0, -160);
  "B2", (180.0,  40.0, -160);
  "B3", (240.0,  95.0, -240);
  "B4", (300.0,  95.0, -240);
  "B5", (360.0, 150.0, -240);
] |> List.to_seq |> Hashtbl.of_seq

let beacon_horizon b =
  match Hashtbl.find_opt beacons b with Some (_,_,h) -> Some h | None -> None

(* ── Пороги диагностики фонаря ────────────────────────────── *)
let no_readings_threshold = minutes 2   (* нет пакетов вообще *)
let silence_threshold     = minutes 5   (* пакеты есть, маяков не слышит *)
let no_motion_threshold   = minutes 10  (* фонарь не двигался *)

(* Гистерезис напряжения: вход в Suspect при V<low; алерт Low_voltage
   только если состояние «низкое» подтверждено временем debounce;
   выход в Voltage_ok только при V>ok (выше входа — нет дребезга на
   границе, и точно ловит факт замены/зарядки батареи). *)
let low_voltage_threshold = 3.5         (* вход в подозрение *)
let voltage_ok_threshold  = 3.7         (* выход в норму (гистерезис) *)
let voltage_debounce      = minutes 2   (* сколько держать подозрение *)

(* ── Входные пакеты (~6 минут, шаг 15с) ────────────────────── *)
(* M1 (горизонт -160м): слышит свой уровень B1/B2.
   M2 (-240м): слышит B3/B4/B5 (свой уровень).
   M3 (-160м): слышит B1 до t=20с, дальше пустые пакеты (фонарь жив,
               эфир пуст — алерт B сработает).
   M4 (-240м): всегда слышит только B5 (один маяк).
   M5 (-160м): шлёт пакеты до t=100с, потом замолкает (алерт A).

   Сценарии диагностики:
   - M1: батарея садится — V плавно падает с 4.0 до 3.3В к концу;
         двигается всегда; SOS нет → ожидается Low_voltage с debounce.
   - M2: двигается до t=120с, потом стоит на месте → No_motion ~t=720с.
         НО симуляция кончается раньше (360с), специально занижу до 2 мин.
   - M3: всё хорошо по бортовым датчикам (V=4.0, двигается), но
         маяки не слышит после t=20с.
   - M4: нажимает SOS на t=120с (один импульс) → Sos.
   - M5: исчезает на t=100с → No_packets (по бортовым датчикам ничего
         не успеваем сказать). *)
let noise i k = float_of_int ((i * 7 + k * 13) mod 9) -. 4.0   (* −4..+4 dB *)

let steps = 25                              (* t = 0, 15, ..., 360с *)
let dt    = seconds 15

(* Снизим пороги движения, чтобы успеть сработать в 6-минутной симуляции *)
let _ = no_motion_threshold   (* объявлен выше как 10 мин для прода *)
let demo_no_motion_threshold = minutes 2   (* в примере: 2 мин не двигается → алерт *)

(* напряжение M1 падает быстро (батарея садится): 4.0 → 3.2 В за симуляцию.
   К i=15 (t=225с) V≈3.5В — войдёт в Suspect; debounce 2 мин → алерт ≈t=345с *)
let m1_voltage i = 4.0 -. 0.8 *. float_of_int i /. float_of_int (steps - 1)

let packets_for lamp i =
  let t = i * dt in
  let default = { lamp; ts = t; readings = [];
                  voltage = 4.0; moving = true; sos = false } in
  match lamp with
  | "M1" -> Some { default with
      readings = [ "B1", -45. +. noise i 1; "B2", -52. +. noise i 2 ];
      voltage  = m1_voltage i }
  | "M2" -> Some { default with
      readings = [ "B3", -48. +. noise i 6; "B4", -50. +. noise i 7;
                   "B5", -72. +. noise i 9 ];
      moving   = (t < seconds 120) }              (* стоит после t=120 *)
  | "M3" ->
    Some { default with
      readings = if i * 15 <= 20
                 then [ "B1", -60. +. noise i 11 ]
                 else [] }                         (* пустые пакеты *)
  | "M4" -> Some { default with
      readings = [ "B5", -55. +. noise i 12 ];
      sos      = (t = seconds 120) }              (* SOS один раз *)
  | "M5" ->
    if i * 15 <= 100
    then Some { default with readings = [ "B2", -50. +. noise i 13 ] }
    else None                                     (* фонарь замолк *)
  | _ -> None

let packets =
  let base =
    List.init steps (fun i ->
      List.filter_map (fun l -> packets_for l i) ["M1"; "M2"; "M3"; "M4"; "M5"])
    |> List.concat in
  (* опоздавший пакет M1 c ts=180с, вставленный после ts≈225с *)
  let late = { lamp = "M1"; ts = seconds 180;
               readings = [ "B1", -44.; "B2", -53. ];
               voltage = m1_voltage 12; moving = true; sos = false } in
  let before, after = List.partition (fun p -> p.ts <= seconds 225) base in
  before @ [late] @ after

(* ── median RSSI по шахтёр×маяк, скользящее окно 60s/5s ───── *)

(* Пайплайн локации (декларативный):
   окно по ШАХТЁРУ → внутри окна group_by бикон → median RSSI на бикон
   → top_k_by 2 по медиане. Результат на окно: (шахтёр, top-2 биконов
   с их медианами). Замена ручному Hashtbl-Hashtbl-сорт-filteri. *)
let median_rssi source =
  source
  |> Pipe.flat_map (fun p ->
       List.map (fun (b, rssi) ->
         { r_lamp = p.lamp; r_beacon = b; r_rssi = rssi; r_ts = p.ts })
         p.readings)
  |> Pipe.event_time ~lateness:(seconds 1)
  |> Pipe.window_agg_keyed ~by:(fun r -> r.r_lamp)
       ~allowed_lateness:(seconds 30)
       (Pipe.sliding (seconds 60) (seconds 5))
       Agg.(
         group_by
           ~key:(fun r -> r.r_beacon)
           ~inner:(median (fun r -> r.r_rssi))
         |> map (fun per_beacon ->
              (* отфильтровать биконы где медиана None (пусто) и взять top-2.
                 top_k_by — простой пост-фильтр над списком из group_by *)
              per_beacon
              |> List.filter_map (fun (b, med) ->
                   match med with Some m -> Some (b, m) | None -> None)
              |> Agg.run (top_k_by 2 ~by:snd)))

(* ════════════════════════════════════════════════════════════════
   КОНТРОЛЬ СОСТОЯНИЯ ШАХТЁРА — три явных слоя, четыре сигнала

   У шахтёра несколько НЕЗАВИСИМЫХ подсистем — связь, движение,
   батарея, SOS-кнопка. Они могут падать одновременно и каждая
   требует своей реакции, поэтому это НЕ один автомат с пятью
   состояниями (где одно подавит другое), а ТРИ независимых FSM
   плюс импульсное событие SOS.

   Структура остаётся той же:

     Слой 1. Last_seen — ЧТО ИЗВЕСТНО (хранилище фактов о шахтёре)
     Слой 2. Self_timer — КОГДА ПРОВЕРИТЬ (один таймер с переменной
              целью на минимум из всех возможных смен состояния)
     Слой 3. Fsm        — ЧТО ДЕЛАТЬ (ТРИ чистые функции перехода —
              по одной на подсистему)

   ЗАВИСИМОСТЬ ПОДАВЛЕНИЯ. motion_state и voltage_state смотрят на
   Last_seen.any: если пакетов нет давно, эти функции возвращают
   соответствующий «Ok» — про motion/voltage мы просто ничего не знаем,
   и сигналить нельзя. Это не подавление эмиссии — это корректная
   семантика самого автомата (правильное «не знаю»).

   ГИСТЕРЕЗИС И DEBOUNCE НАПРЯЖЕНИЯ. У voltage три состояния —
   Voltage_ok, Suspect (внутреннее, наружу не эмитим), Low_voltage.
   Вход в Suspect при V<low; через debounce, если V всё ещё низко,
   переход в Low_voltage и эмиссия алерта. Выход обратно в Voltage_ok
   ТОЛЬКО при V>ok (гистерезис) — это и устраняет дребезг на границе,
   и явно ловит факт замены/зарядки батареи.

   suspect_low_since фиксируется при ПЕРВОМ падении и не сбрасывается
   промежуточными пакетами с V<low: 2 минуты отсчитываются от первого
   момента, когда стало плохо.

   SOS — это НЕ FSM-состояние, а импульс. Эмитится при переходе
   last_sos: false→true. Зажатая кнопка — один алерт; отпустил и
   нажал снова — два события.
   ════════════════════════════════════════════════════════════════ *)

module ByLamp = Keyed.Make (struct type t = packet let key p = p.lamp end)

type alert =
  | No_packets   of string * Time.t option   (* пакетов нет >2 мин *)
  | No_readings  of string * Time.t option   (* пакеты идут, маяков нет >5 мин *)
  | No_motion    of string * Time.t option   (* фонарь не двигался *)
  | Low_voltage  of string * float           (* напряжение подтверждённо ниже порога *)
  | Voltage_ok   of string                   (* напряжение вернулось в норму *)
  | Sos          of string * Time.t          (* нажата кнопка SOS *)

(* ─── Слой 1: Last_seen ───────────────────────────────────────────
   ХРАНИЛИЩЕ ФАКТОВ. Времена последних событий каждого типа и
   последнее наблюдаемое напряжение. Единственная мутабельная
   структура, владеющая «временной памятью» о шахтёре. *)
module Last_seen = struct
  type t = {
    mutable any           : Time.t option;   (* любой пакет *)
    mutable with_readings : Time.t option;   (* пакет с показаниями маяков *)
    mutable moving        : Time.t option;   (* пакет с moving=true *)
    mutable voltage       : float option;    (* последнее наблюдаемое напряжение *)
    mutable suspect_low_since : Time.t option;
      (* когда V ВПЕРВЫЕ упало ниже low_voltage_threshold (и пока не
         поднималось выше voltage_ok_threshold). None — нет подозрения. *)
    mutable sos           : bool;            (* для детекции импульса false→true *)
  }

  let make () = {
    any = None; with_readings = None; moving = None;
    voltage = None; suspect_low_since = None; sos = false;
  }

  (** Зарегистрировать пришедший пакет. Возвращает [`Sos_pressed] если
      это переход кнопки false→true (импульс) — клей сразу эмитит SOS. *)
  let record t ~p : [ `Sos_pressed | `Quiet ] =
    t.any <- Some p.ts;
    if p.readings <> [] then t.with_readings <- Some p.ts;
    if p.moving        then t.moving        <- Some p.ts;
    t.voltage <- Some p.voltage;
    (* Гистерезис: V<low входит в подозрение (если ещё не было);
       V>ok выходит из подозрения. Между low и ok не меняем. *)
    if p.voltage < low_voltage_threshold && t.suspect_low_since = None then
      t.suspect_low_since <- Some p.ts
    else if p.voltage > voltage_ok_threshold then
      t.suspect_low_since <- None;
    let sos_pressed = p.sos && not t.sos in
    t.sos <- p.sos;
    if sos_pressed then `Sos_pressed else `Quiet
end

(* ─── Слой 2: Self_timer ──────────────────────────────────────────
   ТАЙМЕР С ПЕРЕМЕННОЙ ЦЕЛЬЮ. Один логический таймер на шахтёра.
   Цель — минимум из всех возможных моментов смены состояния (см.
   Fsm.next_check). Без этой обёртки сырой API set/cancel +
   идентичность таймера (key, time) — источник тонких ошибок. *)
module Self_timer = struct
  type t = { mutable target : Time.t option }

  let make () = { target = None }

  (** Поставить event-таймер на [target], сняв предыдущий если был и
      указывал на другое время. Идемпотентно по [target]. *)
  let reschedule t (ctx : alert Pipe.ctx) ~target =
    (match t.target with
     | Some old when old <> target -> ctx.cancel_event_timer old
     | _ -> ());
    t.target <- Some target;
    ctx.set_event_timer target

  (** Отметить, что таймер сработал и больше не активен. *)
  let consumed t = t.target <- None
end

(* ─── Слой 3: Fsm ────────────────────────────────────────────────
   ТРИ ЧИСТЫХ АВТОМАТА — по одному на подсистему. Каждый — функция
   фактов и времени; никаких ref, никаких эмиссий. Тестируются
   изолированно, переходы видны явно. *)
module Fsm = struct
  type contact_state = Contact_ok | C_no_readings | C_no_packets
  type motion_state  = Motion_ok  | M_no_motion
  type voltage_state = V_ok       | V_suspect | V_low

  (** Связь. Приоритет «нет пакетов» > «нет показаний» — оправдан:
      если пакетов нет, про показания мы знать не можем. *)
  let contact_state ~now (s : Last_seen.t) : contact_state =
    let exhausted ~since ~threshold = match since with
      | None -> true | Some t -> now - t >= threshold in
    if exhausted ~since:s.any ~threshold:no_readings_threshold then C_no_packets
    else if exhausted ~since:s.with_readings ~threshold:silence_threshold then C_no_readings
    else Contact_ok

  (** Движение. ЗАВИСИТ от наличия пакетов: если связь потеряна
      (No_packets), про движение не знаем — возвращаем Motion_ok как
      «не сигналим». *)
  let motion_state ~now (s : Last_seen.t) : motion_state =
    match contact_state ~now s with
    | C_no_packets -> Motion_ok
    | _ ->
      match s.moving with
      | None -> M_no_motion         (* не двигался ни разу *)
      | Some t when now - t >= demo_no_motion_threshold -> M_no_motion
      | Some _ -> Motion_ok

  (** Напряжение. Та же зависимость: при потере связи — V_ok как
      «не знаем». Гистерезис + debounce уже в Last_seen.suspect_low_since,
      здесь только проверяем условия. *)
  let voltage_state ~now (s : Last_seen.t) : voltage_state =
    match contact_state ~now s with
    | C_no_packets -> V_ok
    | _ ->
      match s.voltage, s.suspect_low_since with
      | None, _ -> V_ok                                (* напряжение ещё не наблюдалось *)
      | Some v, _ when v > voltage_ok_threshold -> V_ok
      | Some _, Some since when now - since >= voltage_debounce -> V_low
      | Some _, Some _ -> V_suspect
      | Some _, None -> V_ok                           (* между порогами без истории *)

  (** Когда автомат сможет в следующий раз поменять состояние — для
      планирования таймера. Минимум из всех возможных моментов: четыре
      порога для contact (2), motion (1), voltage debounce (1). *)
  let next_check (s : Last_seen.t) : Time.t =
    let after since threshold = (Option.value since ~default:0) + threshold in
    let contact_check =
      min (after s.any no_readings_threshold)
          (after s.with_readings silence_threshold) in
    let motion_check = after s.moving demo_no_motion_threshold in
    let voltage_check = match s.suspect_low_since with
      | Some since -> since + voltage_debounce
      | None -> max_int in              (* нет подозрения — таймер не нужен *)
    min contact_check (min motion_check voltage_check)
end

(* ─── Клей: process_keyed связывает три слоя ─────────────────────
   on_event: обновили факты, эмитим SOS если был переход false→true,
             перепланировали таймер.
   on_timer: спросили КАЖДЫЙ из трёх автоматов; если состояние
             конкретной подсистемы изменилось — эмитим её алерт.
             Перепланировали таймер на следующую возможную смену. *)

type per_lamp = {
  seen          : Last_seen.t;
  timer         : Self_timer.t;
  mutable last_contact : Fsm.contact_state;
  mutable last_motion  : Fsm.motion_state;
  mutable last_voltage : Fsm.voltage_state;
}

let make_state () = {
  seen         = Last_seen.make ();
  timer        = Self_timer.make ();
  last_contact = Fsm.Contact_ok;
  last_motion  = Fsm.Motion_ok;
  last_voltage = Fsm.V_ok;
}

(** Сравнить три FSM с прошлыми состояниями, эмитнуть алерты на
    переходах. Voltage_ok эмитим как явное «вернулось в норму».
    V_suspect — внутренний, не эмитим. *)
let check_and_emit ctx key st ~now =
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

let connectivity_alerts source =
  source
  |> Pipe.event_time ~lateness:(seconds 1)
  |> Pipe.process_keyed (module ByLamp)
       ~init:make_state
       ~on_event:(fun ctx key st p ->
         (match Last_seen.record st.seen ~p with
          | `Sos_pressed -> ctx.Pipe.emit (Sos (key, p.ts))
          | `Quiet -> ());
         (* Любой подходящий пакет может вернуть подсистему в норму —
            проверяем все три FSM на «возврат», эмитим если изменилось *)
         check_and_emit ctx key st ~now:p.ts;
         Self_timer.reschedule st.timer ctx ~target:(Fsm.next_check st.seen))
       ~on_timer:(fun ctx key st t _kind ->
         Self_timer.consumed st.timer;
         check_and_emit ctx key st ~now:t;
         (* Перепланировать на следующую возможную смену состояния *)
         let next = Fsm.next_check st.seen in
         if next > t && next < max_int then
           Self_timer.reschedule st.timer ctx ~target:next)

(* ── Сборка сервиса ───────────────────────────────────────── *)

let () =
  Printf.printf "=== Локация шахтёров по маякам ===\n\n";

  (* алерты связи *)
  Printf.printf "Контроль состояния шахтёра:\n";
  let alerts =
    Mf_event.of_list ~ts:(fun p -> p.ts) packets
    |> connectivity_alerts
    |> Pipe.collect in
  (match alerts with
   | [] -> Printf.printf "  все фонари в норме\n"
   | _ -> List.iter (function
       | No_packets (lamp, Some t) ->
         Printf.printf "  ⚠ %s: НЕТ ПАКЕТОВ >%d мин (последний t=%dс)\n"
           lamp (no_readings_threshold/60000) (t/1000)
       | No_packets (lamp, None) ->
         Printf.printf "  ⚠ %s: пакетов не было ни разу\n" lamp
       | No_readings (lamp, Some t) ->
         Printf.printf "  ⚠ %s: пакеты идут, но не слышит маяки >%d мин (последние показания t=%dс)\n"
           lamp (silence_threshold/60000) (t/1000)
       | No_readings (lamp, None) ->
         Printf.printf "  ⚠ %s: не слышал маяки ни разу\n" lamp
       | No_motion (lamp, Some t) ->
         Printf.printf "  ⚠ %s: НЕ ДВИЖЕТСЯ >%d мин (последнее движение t=%dс)\n"
           lamp (demo_no_motion_threshold/60000) (t/1000)
       | No_motion (lamp, None) ->
         Printf.printf "  ⚠ %s: не двигался ни разу\n" lamp
       | Low_voltage (lamp, v) ->
         Printf.printf "  ⚠ %s: НИЗКОЕ НАПРЯЖЕНИЕ %.2fВ (порог %.2fВ, подтверждено %d мин)\n"
           lamp v low_voltage_threshold (voltage_debounce/60000)
       | Voltage_ok lamp ->
         Printf.printf "  ✓ %s: напряжение восстановилось\n" lamp
       | Sos (lamp, t) ->
         Printf.printf "  🆘 %s: НАЖАТА КНОПКА SOS (t=%dс)\n" lamp (t/1000)) alerts);

  (* окна median + подсчёт ретракций *)
  let win_events =
    Mf_event.of_list ~ts:(fun p -> p.ts) packets
    |> median_rssi
    |> Stream.to_list in
  let retracts = List.length (List.filter
    (function Mf_event.Retract _ -> true | _ -> false) win_events) in
  if retracts > 0 then
    Printf.printf "\nОпоздавшие пакеты: ретракций (пересчётов окон): %d\n" retracts;

  (* финальные значения окон (после ретрактов) — теперь окно сразу
     даёт top-2 биконов с их медианами для каждого шахтёра. *)
  let medians =
    Pipe.materialize ~by:(fun (lamp, _) wend -> (lamp, wend))
      (Stream.of_list win_events) in

  (* группировка по шахтёру — для удобства вывода (последние 3 окна) *)
  let by_lamp = Hashtbl.create 8 in
  List.iter (fun ((lamp, wend), (_, top2)) ->
    let xs = try Hashtbl.find by_lamp lamp with Not_found -> [] in
    Hashtbl.replace by_lamp lamp ((wend, top2) :: xs)) medians;

  Printf.printf "\nЛокация (top-2 маяков по median RSSI, последние 3 окна каждого фонаря):\n\n";
  ["M1"; "M2"; "M3"; "M4"; "M5"] |> List.iter (fun lamp ->
    match Hashtbl.find_opt by_lamp lamp with
    | None | Some [] ->
      Printf.printf "%s: нет окон с показаниями\n\n" lamp
    | Some wins ->
      let last3 = List.sort (fun (a,_) (b,_) -> compare b a) wins
                  |> List.filteri (fun i _ -> i < 3)
                  |> List.rev in
      Printf.printf "%s:\n" lamp;
      List.iter (fun (wend, top2) ->
        Printf.printf "  окно→%3dс: " (wend / 1000);
        (match top2 with
         | [] -> Printf.printf "—\n"
         | xs ->
           List.iteri (fun i (beacon, med) ->
             match Hashtbl.find_opt beacons beacon with
             | Some (x, y, h) ->
               Printf.printf "%s%s %.1f dBm (x=%.0f y=%.0f h=%dм)"
                 (if i > 0 then "; " else "") beacon med x y h
             | None -> Printf.printf "%s%s %.1f dBm"
                 (if i > 0 then "; " else "") beacon med) xs;
           if List.length xs < 2 then
             Printf.printf "  ⚠ слышен 1 маяк — для трилатерации мало";
           print_newline ())) last3;
      print_newline ())
