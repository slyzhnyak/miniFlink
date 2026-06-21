(** Таргетный бенч для путей, изменённых в раундах 4-5:
    - efilt (filter с полным разбором Update/Retract)
    - window_agg noop-suppression
    - group_by на Map с late data (реальные corrections)

    Цель: убедиться что изменённые горячие пути не деградировали и
    корректно обрабатывают Update/Retract под объёмом. *)

open Miniflink

let time label f =
  let g0 = Gc.stat () in
  let t0 = Unix.gettimeofday () in
  let r = f () in
  let t1 = Unix.gettimeofday () in
  let g1 = Gc.stat () in
  let mb = (g1.minor_words +. g1.major_words -. g0.minor_words -. g0.major_words)
           *. float Sys.word_size /. 8. /. 1e6 in
  Printf.printf "  %-32s %.3fс  alloc ~%.0fМБ  (%s)\n"
    label (t1 -. t0) mb r

(* ── 1. efilt под объёмом, со смешанными Update/Retract ── *)
let bench_filter n =
  let events = List.init n (fun i ->
    match i mod 4 with
    | 0 -> Mf_event.data (i mod 100) i
    | 1 -> Mf_event.update (i mod 100) ((i+50) mod 100) i  (* asymmetric *)
    | 2 -> Mf_event.retract (i mod 100) i
    | _ -> Mf_event.wm i) in
  fun () ->
    let cnt = ref 0 in
    events |> Stream.of_list |> Pipe.filter (fun v -> v > 50)
    |> Stream.iter (fun _ -> incr cnt);
    Printf.sprintf "%d passed" !cnt

(* ── 2. window_agg с group_by + late data (Map path + noop-supp) ── *)
let bench_group_by_late n =
  let module K = struct type t = int * string * float let key (k,_,_) = string_of_int k end in
  (* n окон, в каждом несколько групп, + late события *)
  let events = List.concat (List.init n (fun w ->
    let base = w * 1000 in
    [ Mf_event.data (w, "B1", float (w mod 50)) base;
      Mf_event.data (w, "B2", float (w mod 30)) (base+10);
      Mf_event.data (w, "B3", float (w mod 70)) (base+20);
      Mf_event.wm (base + 1500);                              (* fire окна *)
      Mf_event.data (w, "B4", float ((w*7) mod 90)) (base+30); (* late → correction *)
    ])) in
  fun () ->
    let u = ref 0 in
    events |> Stream.of_list
    |> Pipe.window_agg (module K) ~allowed_lateness:100000 (Pipe.tumbling 1000)
         Agg.(group_by ~key:(fun (_,b,_) -> b) ~inner:(median (fun (_,_,r) -> r)))
    |> Stream.iter (function Mf_event.Update _ -> incr u | _ -> ());
    Printf.sprintf "%d updates" !u

let () =
  let n = match Sys.argv with [| _; s |] -> int_of_string s | _ -> 500_000 in
  Printf.printf "=== Changed-paths bench (n=%d) ===\n" n;
  time "filter (mixed Update/Retract)" (bench_filter n);
  let w = n / 5 in
  time (Printf.sprintf "group_by+late (%d windows)" w) (bench_group_by_late w);
  Printf.printf "\n"
