open Miniflink
open Time
(* Сахар для ключа: Keyed.of_fun делает first-class Keyed.S из функции
   инлайн (работает со ВСЕМИ операторами), а window_keyed/window_agg_keyed
   принимают ~by напрямую. Ядро не тронуто — это обёртки. *)

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

type ev = { id : string; v : float; ts : int }

let sample = [
  { id="A"; v=10.; ts=seconds 1 };
  { id="B"; v=20.; ts=seconds 2 };
  { id="A"; v=30.; ts=seconds 3 };
]

(* of_fun: создать Keyed.S из функции, отдать в обычный Pipe.window *)
let test_of_fun_with_window () =
  Printf.printf "\n-- Keyed.of_fun works with any (module K) operator\n";
  let k = Keyed.of_fun (fun e -> e.id) in
  let out = Mf_event.of_list ~ts:(fun e -> e.ts) sample
    |> Pipe.event_time ~lateness:0
    |> Pipe.window_agg k (Pipe.tumbling (seconds 10)) Agg.count
    |> Stream.to_list
    |> List.filter_map (function Mf_event.Data ((id,n),_) -> Some (id,n) | _ -> None)
    |> List.sort compare in
  check "grouped by id via of_fun: A=2, B=1" (out = [("A",2); ("B",1)])

(* window_keyed ~by: ключ прямо в вызове, без module *)
let test_window_keyed () =
  Printf.printf "\n-- window_keyed ~by: key inline, no module\n";
  let out = Mf_event.of_list ~ts:(fun e -> e.ts) sample
    |> Pipe.event_time ~lateness:0
    |> Pipe.window_keyed ~by:(fun e -> e.id) (Pipe.tumbling (seconds 10))
    |> Stream.to_list
    |> List.filter_map (function Mf_event.Data ((id, evs),_) -> Some (id, List.length evs) | _ -> None)
    |> List.sort compare in
  check "window_keyed groups by id: A=2, B=1" (out = [("A",2); ("B",1)])

(* window_agg_keyed ~by *)
let test_window_agg_keyed () =
  Printf.printf "\n-- window_agg_keyed ~by\n";
  let out = Mf_event.of_list ~ts:(fun e -> e.ts) sample
    |> Pipe.event_time ~lateness:0
    |> Pipe.window_agg_keyed ~by:(fun e -> e.id)
         (Pipe.tumbling (seconds 10)) (Agg.sum (fun e -> e.v))
    |> Stream.to_list
    |> List.filter_map (function Mf_event.Data ((id,s),_) -> Some (id,s) | _ -> None)
    |> List.sort compare in
  check "window_agg_keyed sums by id: A=40, B=20" (out = [("A",40.); ("B",20.)])

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Keyed sugar (of_fun / *_keyed ~by)\n";
  Printf.printf "==========================================\n";
  test_of_fun_with_window ();
  test_window_keyed ();
  test_window_agg_keyed ();
  Printf.printf "\nKeyed sugar tests passed.\n"
