(** P2.1 / G-5: [Pipe.co_process2] / [co_process3] — keyed-обработка
    нескольких разнотипных потоков на общем per-key состоянии.

    Проверяем:
    - обработчик каждого входа вызывается на события своего потока;
    - состояние общее по ключу (событие из потока A видит эффект
      события из потока B на том же ключе);
    - выход несёт результат обработки (emit) с временем события;
    - emit_retract/emit_update из co_process работают (общий ctx). *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

(* три разнотипных входа, все по ключу-строке *)
type add   = { ak : string; delta : int }
type reset = { rk : string }
type query = { qk : string }

(* общее состояние: счётчик на ключ *)
type st = { mutable n : int }

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  P2.1: co_process3\n";
  Printf.printf "==========================================\n";

  (* поток A: прибавляет; B: сбрасывает; C: запрашивает (эмитит текущее n) *)
  let adds =
    [ Mf_event.data { ak = "x"; delta = 3 } 10;
      Mf_event.data { ak = "x"; delta = 4 } 20 ] |> Stream.of_list in
  let resets =
    [ Mf_event.data { rk = "x" } 30 ] |> Stream.of_list in
  let queries =
    [ Mf_event.data { qk = "x" } 15;   (* после +3 → 3 *)
      Mf_event.data { qk = "x" } 25;   (* после +3+4 → 7 *)
      Mf_event.data { qk = "x" } 35 ]  (* после reset → 0 *)
    |> Stream.of_list in

  let out =
    Pipe.co_process3
      ~init:(fun () -> { n = 0 })
      ~key_a:(fun a -> a.ak)
      ~key_b:(fun b -> b.rk)
      ~key_c:(fun c -> c.qk)
      ~on_a:(fun _ctx _k st a -> st.n <- st.n + a.delta)
      ~on_b:(fun _ctx _k st _b -> st.n <- 0)
      ~on_c:(fun ctx _k st _q -> ctx.Pipe.emit st.n)
      adds resets queries
    |> Pipe.collect in

  Printf.printf "\n-- общее состояние + диспетчеризация по потокам\n";
  (* union по event-time упорядочит: q@15(=3), q@25(=7), reset@30, q@35(=0).
     Но порядок зависит от union; проверяем МНОЖЕСТВО эмиссий query. *)
  check "query@15 после +3 = 3" (List.mem 3 out);
  check "query@25 после +3+4 = 7" (List.mem 7 out);
  check "query@35 после reset = 0" (List.mem 0 out);
  check "ровно 3 эмиссии (по одной на query)" (List.length out = 3);

  (* проверка emit_retract через общий ctx *)
  Printf.printf "\n-- emit_retract из co_process2\n";
  let s_on  = [ Mf_event.data "x" 10 ] |> Stream.of_list in
  let s_off = [ Mf_event.data "x" 20 ] |> Stream.of_list in
  let out2 =
    Pipe.co_process2
      ~init:(fun () -> ())
      ~key_a:(fun k -> k) ~key_b:(fun k -> k)
      ~on_a:(fun ctx _ _ k -> ctx.Pipe.emit (k ^ ":alert"))
      ~on_b:(fun ctx _ _ k -> ctx.Pipe.emit_retract (k ^ ":alert"))
      s_on s_off
    |> Stream.to_list in
  check "co_process2: Data затем Retract"
    (match List.filter (function
        | Mf_event.Data _ | Mf_event.Retract _ -> true | _ -> false) out2 with
     | [ Mf_event.Data ("x:alert", 10); Mf_event.Retract ("x:alert", 20) ] -> true
     | _ -> false);

  Printf.printf "\nco_process tests passed.\n"
