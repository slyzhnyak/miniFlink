type 'a t =
  | Data      of 'a * Time.t
  | Watermark of Time.t
  | Retract   of 'a * Time.t

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
let with_watermarks ~latency src = with_watermarks_ext ~latency ~interval:0 src

let pp_ts t = Printf.sprintf "%d.%03ds" (t/1000) (t mod 1000)
