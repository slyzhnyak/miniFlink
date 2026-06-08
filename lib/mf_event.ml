(** Событие потока: данные, watermark или отмена.

    Все три конструктора текут по одному потоку и обрабатываются
    операторами через pattern matching — единообразно и с проверкой
    полноты компилятором. *)

(** Событие со значением ['a]. *)
type 'a t =
  | Data      of 'a * Time.t  (** значение + его event-time *)
  | Watermark of Time.t       (** граница: события раньше уже пришли *)
  | Retract   of 'a * Time.t  (** отмена ранее выданного результата *)

let data    v ts = Data    (v, ts)
let retract v ts = Retract (v, ts)
let wm        ts = Watermark ts

let value  = function Data (v,_) | Retract (v,_) -> Some v | _ -> None
let ts     = function Data (_,t) | Retract (_,t) | Watermark t -> t
let is_data = function Data _ -> true | _ -> false

let map_value f = function
  | Data    (v,t) -> Data    (f v, t)
  | Retract (v,t) -> Retract (f v, t)
  | Watermark _ as w -> w

(* ── Watermark стратегия ──────────────────────────────────────
   wm = max_seen - latency, но эмитим не на каждый максимум, а:
   - когда watermark продвинулся минимум на ~interval (по event-time),
     чтобы не плодить watermark на каждое событие;
   - при idle (источник вернул None, но поток не закрыт): эмитим
     текущий watermark, чтобы окна закрылись и не висели.

   Обратная совместимость: with_watermarks ~latency работает как
   и раньше (interval=0 → watermark на каждый продвинувшийся максимум).
   ──────────────────────────────────────────────────────────── *)

(** [with_watermarks_ext ~latency ?interval src] вставляет watermark-маркеры
    в поток. Watermark = [max_seen - latency]: все события с меньшим
    timestamp считаются прибывшими.

    - [~latency] — допуск на опоздание (out-of-orderness).
    - [~interval] — минимальный сдвиг watermark перед новой эмиссией
      (по event-time). При [interval=0] watermark эмитится на каждый
      новый максимум; больший [interval] уменьшает их число.

    На конце потока эмитится финальный watermark [= max_seen], чтобы
    закрыть все хвостовые окна. *)
let with_watermarks_ext ~latency ?(interval = 0) (src : 'a t Stream.t)
    : 'a t Stream.t =
  let max_seen   = ref min_int in
  let last_wm    = ref min_int in   (* последний эмитнутый watermark *)
  let pending    = Queue.create () in
  let maybe_emit () =
    let wm = !max_seen - latency in
    (* эмитим если продвинулись хотя бы на interval от прошлого wm.
       Первый watermark (last_wm = min_int) эмитим всегда — избегаем
       переполнения wm - min_int. *)
    let advanced =
      if !last_wm = min_int then true
      else wm > !last_wm && wm - !last_wm >= interval
    in
    if wm > !last_wm && advanced then begin
      last_wm := wm;
      Queue.push (Watermark wm) pending
    end
  in
  fun () ->
    if not (Queue.is_empty pending) then Some (Queue.pop pending) else
    match src () with
    | None ->
      (* Поток закончился: эмитим финальный watermark = max_seen,
         чтобы закрыть все оставшиеся окна (включая последнее,
         которое latency могла оставить открытым). *)
      let final_wm = !max_seen in
      if final_wm > !last_wm && !max_seen > min_int then begin
        last_wm := final_wm;
        Some (Watermark final_wm)
      end else None
    | Some (Watermark _ | Retract _ as ev) -> Some ev
    | Some (Data (_, t) as ev) ->
      if t > !max_seen then begin
        max_seen := t;
        maybe_emit ()
      end;
      Some ev

(* Совместимость: старая сигнатура (watermark на каждый новый максимум) *)
(** [with_watermarks ~latency src] — то же что
    {!with_watermarks_ext} с [~interval:0] (watermark на каждый новый
    максимум). Обратно совместимая форма. *)
let with_watermarks ~latency src = with_watermarks_ext ~latency ~interval:0 src

(** [of_list ~ts xs] — поток Data-событий из списка значений, event-time
    каждого берётся из [ts]. Убирает повторяющееся
    [Stream.of_list (List.map (fun v -> data v (ts v)) xs)]. *)
let of_list ~ts xs =
  Stream.of_list (List.map (fun v -> Data (v, ts v)) xs)

(** Idle watermark: продвигает watermark по {e wall-clock} когда источник
    замолчал, чтобы окна не висели открытыми в тишине.

    Работает с {e неблокирующим} источником [poll : unit -> 'a t option option]:
    - [None]            — поток окончен;
    - [Some None]       — данных пока нет (тишина);
    - [Some (Some ev)]  — событие.

    Когда тишина длится дольше [~idle_ms] (реального времени, через [~now_ms]),
    эмитится watermark, продвинутый на [~idle_advance] от последнего
    watermark. Это держится {e вне} чистого event-time ядра — таймер живёт
    здесь, в слое источника, операторы не трогаются.

    [~now_ms] и [~sleep_ms] вынесены параметрами (источник реального времени
    и пауза между опросами), чтобы тесты могли подать управляемые часы и
    no-op паузу вместо настоящих. Результат — обычный {!Stream.t}: тишина
    обрабатывается внутри (опрос в цикле), наружу выходит либо событие,
    либо watermark, либо конец. *)
let with_idle_watermarks
    ~latency ~idle_ms ~idle_advance
    ~(now_ms : unit -> int)
    ~(sleep_ms : int -> unit)
    (poll : unit -> 'a t option option)
    : 'a t Stream.t =
  let max_seen = ref min_int in
  let last_wm  = ref min_int in
  let last_activity_wall = ref (now_ms ()) in
  fun () ->
    let rec loop () =
      match poll () with
      | None ->
        (* конец потока: финальный watermark = max_seen, потом None *)
        if !max_seen > !last_wm && !max_seen > min_int then begin
          last_wm := !max_seen; Some (Watermark !max_seen)
        end else None
      | Some (Some (Data (_, t) as ev)) ->
        last_activity_wall := now_ms ();
        if t > !max_seen then max_seen := t;
        Some ev
      | Some (Some ev) ->            (* watermark/retract прозрачно *)
        last_activity_wall := now_ms ();
        Some ev
      | Some None ->
        (* тишина *)
        let now = now_ms () in
        if now - !last_activity_wall >= idle_ms && !max_seen > min_int then begin
          (* база для idle-watermark: максимум из последнего wm и
             известного event-time, иначе при last_wm=min_int получили бы
             бессмысленное min_int + advance *)
          let base = if !last_wm = min_int then !max_seen else !last_wm in
          let idle_wm = base + idle_advance in
          last_wm := idle_wm;
          last_activity_wall := now;   (* следующий idle-wm через idle_ms *)
          Some (Watermark idle_wm)
        end else begin
          (* ещё не созрело — подождём и опросим снова (внутри одного next) *)
          sleep_ms 1;
          loop ()
        end
    in
    loop ()

(* ── Union: слияние двух потоков по event-time ────────────── *)

(* Объединяет два потока событий, упорядочивая по event-time (merge).
   Оба входа должны быть упорядочены по времени (как после источника
   с watermarks). Тонкость с watermarks: watermark объединённого потока
   = МИНИМУМ watermarks обоих входов — нельзя гарантировать что время T
   прошло, пока медленный вход это не подтвердил. Это сохраняет
   корректность окон ниже по течению. *)
let union (a : 'v t Stream.t) (b : 'v t Stream.t) : 'v t Stream.t =
  (* по одному «заглянутому» элементу из каждого входа *)
  let pa = ref None and pb = ref None in
  let a_done = ref false and b_done = ref false in
  (* последние watermarks каждого входа (для вычисления min) *)
  let wm_a = ref min_int and wm_b = ref min_int in
  let emitted_wm = ref min_int in
  let pull_a () =
    if !a_done then () else
    match !pa with Some _ -> () | None ->
      (match a () with None -> a_done := true | Some ev -> pa := Some ev) in
  let pull_b () =
    if !b_done then () else
    match !pb with Some _ -> () | None ->
      (match b () with None -> b_done := true | Some ev -> pb := Some ev) in
  fun () ->
    let rec step () =
      pull_a (); pull_b ();
      match !pa, !pb with
      (* watermarks потребляем отдельно: обновляем границу входа и
         эмитим объединённый wm = min, только если он продвинулся *)
      | Some (Watermark w), _ ->
        pa := None; wm_a := w;
        let m = min !wm_a !wm_b in
        if !b_done then (emitted_wm := w; Some (Watermark w))
        else if m > !emitted_wm then (emitted_wm := m; Some (Watermark m))
        else step ()
      | _, Some (Watermark w) ->
        pb := None; wm_b := w;
        let m = min !wm_a !wm_b in
        if !a_done then (emitted_wm := w; Some (Watermark w))
        else if m > !emitted_wm then (emitted_wm := m; Some (Watermark m))
        else step ()
      (* данные с обеих сторон — эмитим меньший по времени *)
      | Some ea, Some eb ->
        if ts ea <= ts eb then (pa := None; Some ea)
        else (pb := None; Some eb)
      | Some ea, None when !b_done -> pa := None; Some ea
      | None, Some eb when !a_done -> pb := None; Some eb
      | None, None when !a_done && !b_done -> None
      | _ -> step ()   (* один пуст, другой ещё нет — докрутить *)
    in step ()

let pp_ts t = Printf.sprintf "%d.%03ds" (t/1000) (t mod 1000)
