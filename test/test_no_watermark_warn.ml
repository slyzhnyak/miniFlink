(** E-1 / P1.5: раннее предупреждение о забытом [event_time].

    Окна закрываются только по watermark. Без [Pipe.event_time] на
    бесконечном потоке окна копятся вечно, ноль результатов, ноль
    сигналов — худший тихий отказ. Раньше warning был только на
    end-of-stream (на бесконечном потоке недостижим). Теперь окно
    считает Data-события и при пороге (10000) без единого watermark
    логирует один раз В ПРОЦЕССЕ.

    Проверяем через перехват Log.set_sink:
    - поток из >10000 Data без watermark → warning появляется;
    - поток с watermark → warning НЕ появляется;
    - warning логируется РОВНО один раз (не на каждое событие). *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

module ByKV = Keyed.Make (struct type t = string * int let key (k,_) = k end)

(* перехват warning'ов с текстом про watermark *)
let count_wm_warnings f =
  let n = ref 0 in
  Log.set_sink (fun ev ->
    if ev.Log.level = Log.Warning
    && (try ignore (Str.search_forward (Str.regexp "watermark") ev.Log.message 0); true
        with Not_found -> false)
    then incr n);
  (try f () with _ -> ());
  Log.set_sink (fun _ -> ());  (* сброс *)
  !n

(* прогнать окно на потоке; drain до конца *)
let drain stream =
  let rec loop () = match stream () with None -> () | Some _ -> loop () in
  loop ()

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  E-1: no-watermark warning\n";
  Printf.printf "==========================================\n";

  (* поток из N Data-событий БЕЗ watermark *)
  let data_only n =
    let i = ref 0 in
    fun () ->
      if !i >= n then None
      else (incr i; Some (Mf_event.data ("k", !i) !i)) in

  (* поток из N Data С watermark каждые 100 событий *)
  let data_with_wm n =
    let i = ref 0 in
    fun () ->
      if !i > n then None
      else begin
        let v = !i in incr i;
        if v mod 100 = 0 then Some (Mf_event.wm v)
        else Some (Mf_event.data ("k", v) v)
      end in

  Printf.printf "\n-- поток без watermark (>10000 событий) → warning\n";
  let warns = count_wm_warnings (fun () ->
    data_only 11000
    |> Pipe.window (module ByKV) (Pipe.tumbling 50)
    |> drain) in
  check "warning про watermark появился" (warns >= 1);
  check "warning ровно один раз (не на каждое событие)" (warns = 1);

  Printf.printf "\n-- поток С watermark → без warning\n";
  let warns2 = count_wm_warnings (fun () ->
    data_with_wm 11000
    |> Pipe.window (module ByKV) (Pipe.tumbling 50)
    |> drain) in
  check "warning про watermark НЕ появился" (warns2 = 0);

  Printf.printf "\nE-1 no-watermark warning test passed.\n"
