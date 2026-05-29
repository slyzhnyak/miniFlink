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

(* Watermark стратегия: wm = max_seen - latency *)
let with_watermarks ~latency (src : 'a t Stream.t) : 'a t Stream.t =
  let max_seen = ref min_int in
  let pending  = Queue.create () in
  fun () ->
    if not (Queue.is_empty pending) then Some (Queue.pop pending) else
    match src () with
    | None -> None
    | Some (Watermark _ | Retract _ as ev) -> Some ev
    | Some (Data (_, t) as ev) ->
      if t > !max_seen then begin
        max_seen := t;
        Queue.push (Watermark (t - latency)) pending
      end;
      Some ev

let pp_ts t = Printf.sprintf "%d.%03ds" (t/1000) (t mod 1000)
