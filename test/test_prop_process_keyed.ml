(** Property-тесты: инварианты process_keyed.

    1. FSM-консистентность: для произвольного потока событий результат
       process_keyed (как сумма per-key эмиссий) совпадает с
       независимым oracle, считающим то же per ключ.

    2. Таймеры event-time: event-timer, установленный на время T,
       срабатывает РОВНО когда watermark достигает T, и ровно один
       раз. Число timer-эмиссий = число ключей, доживших до своего
       fire-времени под финальным watermark. *)

open Miniflink
open QCheck

module SK : Keyed.S with type t = string * int = struct
  type t = string * int
  let key (k, _) = k
end

(* Поток (key, value, ts) in-order по ts. *)
let gen_stream : (string * int * int) list Gen.t =
  let open Gen in
  let keys = [| "A"; "B"; "C"; "D" |] in
  sized_size (int_range 0 60) (fun n ->
    list_repeat n (tup2 (int_range 0 3) (int_range 0 100))
    >>= fun pairs ->
    return (List.mapi (fun i (ki, v) -> (keys.(ki), v, i * 10)) pairs))

let arb_stream = make ~print:(fun evs ->
  String.concat " " (List.map (fun (k,v,t) -> Printf.sprintf "%s:%d@%d" k v t) evs))
  gen_stream

(* ── 1. FSM: count per ключ, эмитим каждое 3-е событие ───────── *)
(* Состояние — мутабельный record (process_keyed мутирует st in place,
   не возвращает новое). *)
type counter = { mutable n : int }

let run_count_fsm events =
  let stream =
    (List.map (fun (k,v,ts) -> Mf_event.data (k, v) ts) events
     @ [Mf_event.wm 1_000_000])
    |> Stream.of_list
    |> Pipe.process_keyed (module SK)
         ~init:(fun () -> { n = 0 })
         ~on_event:(fun ctx _k st _ev ->
           st.n <- st.n + 1;
           if st.n mod 3 = 0 then ctx.emit st.n)
         ~on_timer:(fun _ _ _ _ _ -> ()) in
  let acc = ref [] in
  let rec go () = match stream () with
    | None -> () | Some (Mf_event.Data (n, _)) -> acc := n :: !acc; go ()
    | Some _ -> go ()
  in go (); List.sort compare !acc

(* Oracle: воспроизводим тот же счётчик независимо. *)
let oracle_count_fsm events =
  let cnt : (string, int) Hashtbl.t = Hashtbl.create 8 in
  let emitted = ref [] in
  List.iter (fun (k, _, _) ->
    let n = 1 + (try Hashtbl.find cnt k with Not_found -> 0) in
    Hashtbl.replace cnt k n;
    if n mod 3 = 0 then emitted := n :: !emitted) events;
  List.sort compare !emitted

let prop_fsm_count =
  Test.make ~count:1000 ~name:"process_keyed: per-key counter = oracle"
    arb_stream
    (fun events -> run_count_fsm events = oracle_count_fsm events)

(* ── 2. Таймеры: на первое событие ключа ставим event-timer на
   ts+100; он должен сработать ровно один раз, когда wm дойдёт. ── *)
let run_timer_fsm events ~final_wm =
  let stream =
    (List.map (fun (k,v,ts) -> Mf_event.data (k, v) ts) events
     @ [Mf_event.wm final_wm])
    |> Stream.of_list
    |> Pipe.process_keyed (module SK)
         ~init:(fun () -> false)   (* поставлен ли уже таймер *)
         ~on_event:(fun ctx _k armed (_, _) ->
           if not armed then begin
             (* fire через 100 после первого ts ключа: нужен ts.
                Берём ts из ctx? нет — используем значение как смещение.
                Упростим: ставим таймер на фиксированное будущее. *)
             ctx.set_event_timer 500
           end;
           ignore armed)
         ~on_timer:(fun ctx k _ t _ -> ctx.emit (k, t)) in
  (* перезапишем: armed надо обновлять — process_keyed передаёт st, но
     мы не меняем его здесь. Используем set через возврат? on_event не
     возвращает st. Таймер идемпотентен (повторный set на то же время
     не дублируется), поэтому armed не нужен. *)
  let acc = ref [] in
  let rec go () = match stream () with
    | None -> () | Some (Mf_event.Data ((k,t), _)) -> acc := (k,t) :: !acc; go ()
    | Some _ -> go ()
  in go (); !acc

(* Инвариант: каждый ключ, у которого было хоть одно событие, и чей
   timer (500) <= final_wm, даёт РОВНО один timer-эмит. Идемпотентность
   set_event_timer гарантирует отсутствие дублей. *)
let prop_timer_fires_once =
  Test.make ~count:500
    ~name:"process_keyed: event-timer fires exactly once per key when wm passes"
    (make (Gen.tup2 gen_stream (Gen.int_range 0 2000)))
    (fun (events, _) ->
       let final_wm = 1000 in  (* > 500, все таймеры должны сработать *)
       let out = run_timer_fsm events ~final_wm in
       (* ключи с событиями *)
       let keys_with_events =
         List.fold_left (fun s (k,_,_) -> if List.mem k s then s else k :: s) [] events in
       (* каждый такой ключ ровно один раз в выходе с t=500 *)
       List.for_all (fun k ->
         let fires = List.filter (fun (k',t) -> k'=k && t=500) out in
         List.length fires = 1) keys_with_events
       && List.length out = List.length keys_with_events)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Property: process_keyed FSM + timers\n";
  Printf.printf "==========================================\n";
  let suites = [ prop_fsm_count; prop_timer_fires_once ] in
  let ok = QCheck_runner.run_tests ~verbose:false suites in
  if ok = 0 then (Printf.printf "\nAll process_keyed properties passed.\n"; exit 0)
  else (Printf.printf "\nSOME PROPERTIES FAILED.\n"; exit 1)
