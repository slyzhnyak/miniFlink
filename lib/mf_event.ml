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

let pp_ts t = Printf.sprintf "%d.%03ds" (t/1000) (t mod 1000)
