(** P1.1 / G-1: [Mf_event.map_ts] / [Pipe.map_ts] — map с доступом к ts,
    сохраняющий вид события.

    Проверяем, что f получает и значение, и время, и что все четыре
    конструктора обрабатываются: Data/Retract маппятся со своим ts,
    Update маппит оба значения с общим ts, Watermark проходит без
    изменений. *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  P1.1: map_ts\n";
  Printf.printf "==========================================\n";

  (* f делает из значения пару (значение, ts) — чтобы проверить, что ts доступен *)
  let f v t = (v, t) in

  Printf.printf "\n-- Mf_event.map_ts по конструкторам\n";
  (match Mf_event.map_ts f (Mf_event.data "a" 10) with
   | Mf_event.Data (("a", 10), 10) -> pass "Data несёт свой ts"
   | _ -> fail "Data");
  (match Mf_event.map_ts f (Mf_event.retract "b" 20) with
   | Mf_event.Retract (("b", 20), 20) -> pass "Retract несёт свой ts"
   | _ -> fail "Retract");
  (match Mf_event.map_ts f (Mf_event.update "old" "new" 30) with
   | Mf_event.Update { old = ("old", 30); new_value = ("new", 30); ts = 30 } ->
     pass "Update: оба значения с общим ts"
   | _ -> fail "Update");
  (match Mf_event.map_ts f (Mf_event.wm 40 : string Mf_event.t) with
   | Mf_event.Watermark 40 -> pass "Watermark проходит без изменений"
   | _ -> fail "Watermark");

  Printf.printf "\n-- Pipe.map_ts на потоке\n";
  let out =
    [ Mf_event.data "x" 1; Mf_event.wm 2; Mf_event.retract "x" 3 ]
    |> Stream.of_list
    |> Pipe.map_ts (fun v t -> Printf.sprintf "%s@%d" v t)
    |> Stream.to_list in
  check "поток: Data и Retract получили ts, Watermark цел"
    (match out with
     | [ Mf_event.Data ("x@1", 1);
         Mf_event.Watermark 2;
         Mf_event.Retract ("x@3", 3) ] -> true
     | _ -> false);

  Printf.printf "\nmap_ts tests passed.\n"
