open Miniflink
open QCheck
(* Fuzzing водяных знаков (п.5 ревью): хаотичные потоки со случайными
   (в т.ч. убывающими) event-time, проверяем инварианты движка:
     1. выходной watermark МОНОТОНЕН (даже при backward на входе)
     2. watermark не обгоняет max event-time входа
     3. ни одно событие не теряется (окно + side output = вход)
   Property-генерация ловит крайние случаи, которые детерминированные
   тесты пропускают. *)

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1

let gen_event = Gen.(map2 (fun v t -> Mf_event.data v t) small_int (int_range 0 1000))
let gen_chaotic = Gen.(list_size (int_range 1 80) gen_event)
let arb_stream = make ~print:(fun evs -> Printf.sprintf "[%d events]" (List.length evs)) gen_chaotic

let extract_wms s =
  Stream.to_list s |> List.filter_map (function Mf_event.Watermark w -> Some w | _ -> None)

let rec monotone = function a :: (b :: _ as r) -> a <= b && monotone r | _ -> true

let prop_monotone =
  Test.make ~count:500 ~name:"watermarks monotone under chaotic event-time"
    arb_stream (fun events ->
      Stream.of_list events |> Mf_event.with_watermarks ~latency:5
      |> extract_wms |> monotone)

let prop_not_exceed =
  Test.make ~count:500 ~name:"watermark never exceeds max event-time"
    arb_stream (fun events ->
      if events = [] then true else
      let max_ts = List.fold_left (fun m -> function
        | Mf_event.Data (_, t) -> max m t | _ -> m) min_int events in
      Stream.of_list events |> Mf_event.with_watermarks ~latency:5
      |> extract_wms |> List.for_all (fun w -> w <= max_ts))

module K = Keyed.Make (struct type t = int * int let key (k,_) = string_of_int (k mod 4) end)

let prop_no_loss =
  Test.make ~count:300 ~name:"no event lost under chaotic watermarks (window+late)"
    arb_stream (fun events ->
      if events = [] then true else
      let tagged = List.map (function
        | Mf_event.Data (v, t) -> Mf_event.data (v, t) t
        | Mf_event.Watermark w -> Mf_event.wm w
        | Mf_event.Retract (v,t) -> Mf_event.retract (v,t) t
        | Mf_event.Update { old; new_value = v; ts = t } ->
          Mf_event.update (old, t) (v, t) t) events in
      let total_in = List.length
        (List.filter (function Mf_event.Data _ -> true | _ -> false) tagged) in
      let late = ref 0 in
      let in_windows =
        Stream.of_list tagged |> Mf_event.with_watermarks ~latency:5
        |> Pipe.window (module K) ~allowed_lateness:50 ~on_late:(fun _ -> incr late)
             (Pipe.tumbling 20)
        |> Stream.to_list
        |> List.fold_left (fun acc -> function
           | Mf_event.Data ((_, vs), _) -> acc + List.length vs
           | Mf_event.Retract ((_, vs), _) -> acc - List.length vs
           | Mf_event.Update { old = (_, old_vs); new_value = (_, new_vs); _ } ->
             acc - List.length old_vs + List.length new_vs
           | Mf_event.Watermark _ -> acc) 0 in
      in_windows + !late = total_in)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Watermark fuzzing (property-based)\n";
  Printf.printf "==========================================\n";
  let ok = QCheck_base_runner.run_tests ~verbose:false
    [prop_monotone; prop_not_exceed; prop_no_loss] in
  if ok = 0 then pass "all watermark properties hold"
  else fail "watermark property violated"
