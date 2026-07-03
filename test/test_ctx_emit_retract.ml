(** P1.2 / G-4: [ctx.emit_retract] и [ctx.emit_update] в process_keyed.

    Раньше ctx.emit умел эмитить только Data-значения, поэтому
    keyed-логика с retract-семантикой (обновление ранее выпущенных
    алертов) писалась в обход process_keyed (ex07 gas_alerts). Теперь
    контекст умеет эмитить Retract и Update.

    Проверяем: on_event, вызывающий emit_retract / emit_update,
    порождает в выходном потоке Retract / Update соответствующего вида,
    с временем события. *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

module ByStr = Keyed.Make (struct type t = string let key s = s end)

(* прогнать process_keyed на списке событий, собрать выходные события *)
let run ~on_event events =
  events
  |> Stream.of_list
  |> Pipe.process_keyed (module ByStr)
       ~init:(fun _ -> ())
       ~on_event
       ~on_timer:(fun _ _ _ _ _ -> ())
  |> Stream.to_list

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  P1.2: ctx.emit_retract / emit_update\n";
  Printf.printf "==========================================\n";

  (* on_event: на "d" эмитит Data, на "r" — Retract, на "u" — Update *)
  let on_event ctx key _st v =
    match v with
    | "d" -> ctx.Pipe.emit ("data:" ^ key)
    | "r" -> ctx.Pipe.emit_retract ("retract:" ^ key)
    | "u" -> ctx.Pipe.emit_update ~old:("old:" ^ key) ("new:" ^ key)
    | _ -> ()
  in
  let out = run ~on_event [
    Mf_event.data "d" 10;
    Mf_event.data "r" 20;
    Mf_event.data "u" 30;
  ] in
  (* фильтруем watermark'и, смотрим на порождённые события *)
  let interesting = List.filter (function
    | Mf_event.Watermark _ -> false | _ -> true) out in

  Printf.printf "\n-- emit порождает Data\n";
  check "Data с временем события"
    (List.exists (function
       | Mf_event.Data ("data:d", 10) -> true | _ -> false) interesting);

  Printf.printf "\n-- emit_retract порождает Retract\n";
  check "Retract с временем события"
    (List.exists (function
       | Mf_event.Retract ("retract:r", 20) -> true | _ -> false) interesting);

  Printf.printf "\n-- emit_update порождает Update\n";
  check "Update old→new с временем события"
    (List.exists (function
       | Mf_event.Update { old = "old:u"; new_value = "new:u"; ts = 30 } -> true
       | _ -> false) interesting);

  Printf.printf "\nemit_retract / emit_update tests passed.\n"
