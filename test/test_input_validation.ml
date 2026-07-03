(** E-4 / P1.3: валидация Time/int-параметров публичных конструкторов.

    Раньше отрицательный lateness/cooldown, нулевой ttl/workers и т.п.
    принимались молча и давали тихо-неверное поведение (напр.
    отрицательный lateness = watermark впереди событий = «всё late»).
    Теперь каждый публичный конструктор проверяет свои Time/int-параметры
    и бросает [Invalid_argument] с именем параметра. *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1

(* проверяет, что [f ()] бросает Invalid_argument *)
let raises_invalid name f =
  match f () with
  | exception Invalid_argument _ -> pass name
  | exception e ->
    Printf.printf "  FAIL %s: ожидался Invalid_argument, получено %s\n%!"
      name (Printexc.to_string e); exit 1
  | _ -> fail (name ^ ": не бросил Invalid_argument")

(* и что корректный вход НЕ бросает *)
let ok name f =
  match f () with
  | exception e ->
    Printf.printf "  FAIL %s: неожиданное исключение %s\n%!"
      name (Printexc.to_string e); exit 1
  | _ -> pass name

module ByStr = Keyed.Make (struct type t = string let key s = s end)

let empty () : string Mf_event.t Stream.t = Stream.empty

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  E-4: input validation\n";
  Printf.printf "==========================================\n";

  Printf.printf "\n-- отрицательные/нулевые параметры отвергаются\n";
  raises_invalid "event_time lateness<0"
    (fun () -> Pipe.event_time ~lateness:(-5) (empty ()));
  raises_invalid "dedup cooldown<0"
    (fun () -> Pipe.dedup (module ByStr) ~rule:(fun s -> s)
        ~cooldown:(-1) (empty ()));
  raises_invalid "keyed_join ttl<=0"
    (fun () -> let _s : _ Stream.t = Pipe.keyed_join (module ByStr) ~ttl:0 [empty ()] in ());
  raises_invalid "window latency<0"
    (fun () -> Pipe.window (module ByStr) ~latency:(-1)
        (Pipe.tumbling 10) (empty ()));
  raises_invalid "window allowed_lateness<0"
    (fun () -> Pipe.window (module ByStr) ~allowed_lateness:(-1)
        (Pipe.tumbling 10) (empty ()));

  Printf.printf "\n-- run_exactly_once параметры\n";
  let dummy_store () = Checkpoint_parallel.make_store () in
  raises_invalid "workers<=0"
    (fun () -> Checkpoint_parallel.run_exactly_once
        ~workers:0 ~capacity:16 ~checkpoint_every:4
        ~key_of:(fun s -> s) ~make_state:State_backend_memory.create
        ~process:(fun _ _ -> []) ~source:(Checkpoint_parallel.seekable_of_list [])
        ~sink:(Checkpoint_parallel.idempotent_sink (fun _ -> ()))
        ~store:(dummy_store ()) ());
  raises_invalid "capacity<=0"
    (fun () -> Checkpoint_parallel.run_exactly_once
        ~workers:2 ~capacity:0 ~checkpoint_every:4
        ~key_of:(fun s -> s) ~make_state:State_backend_memory.create
        ~process:(fun _ _ -> []) ~source:(Checkpoint_parallel.seekable_of_list [])
        ~sink:(Checkpoint_parallel.idempotent_sink (fun _ -> ()))
        ~store:(dummy_store ()) ());
  raises_invalid "checkpoint_every<=0"
    (fun () -> Checkpoint_parallel.run_exactly_once
        ~workers:2 ~capacity:16 ~checkpoint_every:0
        ~key_of:(fun s -> s) ~make_state:State_backend_memory.create
        ~process:(fun _ _ -> []) ~source:(Checkpoint_parallel.seekable_of_list [])
        ~sink:(Checkpoint_parallel.idempotent_sink (fun _ -> ()))
        ~store:(dummy_store ()) ());

  Printf.printf "\n-- корректные значения НЕ отвергаются\n";
  ok "event_time lateness=0"
    (fun () -> Pipe.event_time ~lateness:0 (empty ()));
  ok "dedup cooldown=0"
    (fun () -> Pipe.dedup (module ByStr) ~rule:(fun s -> s)
        ~cooldown:0 (empty ()));
  ok "keyed_join ttl=1"
    (fun () -> let _s : _ Stream.t = Pipe.keyed_join (module ByStr) ~ttl:1 [empty ()] in ());
  ok "keyed_join без ttl"
    (fun () -> let _s : _ Stream.t = Pipe.keyed_join (module ByStr) [empty ()] in ());

  Printf.printf "\nE-4 validation tests passed.\n"
