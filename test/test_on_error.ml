(** P2.4 / E-3: [?on_error] в process_keyed и window_fold.

    Без on_error исключение из пользовательского колбэка роняет
    пайплайн (и в exactly-once даёт бесконечный цикл crash-recovery).
    С on_error «ядовитое» событие уходит в обработчик вместе с событием,
    а обработка продолжается.

    Проверяем:
    - process_keyed: ядовитое событие ловится, on_error видит exn И
      событие, последующие события обрабатываются;
    - default (без on_error) — исключение пробрасывается;
    - window_fold: то же для пользовательского add. *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

module ByKey = Keyed.Make (struct
  type t = string * int
  let key (k, _) = k
end)

exception Poison

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  P2.4: ?on_error (process_keyed + window_fold)\n";
  Printf.printf "==========================================\n";

  (* ── process_keyed: on_event падает на cmd=666 ── *)
  Printf.printf "\n-- process_keyed: ядовитое событие → on_error, пайплайн живёт\n";
  let caught = ref [] in
  let emitted =
    [ Mf_event.data ("a", 1) 10;
      Mf_event.data ("a", 666) 20;   (* ядовитое *)
      Mf_event.data ("a", 2) 30 ]
    |> Stream.of_list
    |> Pipe.process_keyed (module ByKey)
         ~init:(fun () -> ())
         ~on_error:(fun e ev ->
           let ts = match ev with
             | Mf_event.Data (_, t) -> t | _ -> -1 in
           caught := (Printexc.to_string e, ts) :: !caught)
         ~on_event:(fun ctx _k _st (_key, cmd) ->
           if cmd = 666 then raise Poison
           else ctx.Pipe.emit cmd)
         ~on_timer:(fun _ _ _ _ _ -> ())
    |> Pipe.collect in
  check "ядовитое событие поймано on_error" (List.length !caught = 1);
  check "on_error видит событие (ts=20)"
    (List.exists (fun (_, ts) -> ts = 20) !caught);
  check "события до и после обработаны (emit 1 и 2)"
    (List.mem 1 emitted && List.mem 2 emitted);
  check "ядовитое НЕ эмитнуто" (not (List.mem 666 emitted));

  (* ── default: без on_error исключение пробрасывается ── *)
  Printf.printf "\n-- default: без on_error исключение пробрасывается\n";
  let raised =
    try
      let s =
        [ Mf_event.data ("a", 666) 10 ]
        |> Stream.of_list
        |> Pipe.process_keyed (module ByKey)
             ~init:(fun () -> ())
             ~on_event:(fun _ _ _ (_key, cmd) -> if cmd = 666 then raise Poison)
             ~on_timer:(fun _ _ _ _ _ -> ())
      in
      ignore (Pipe.collect s); false
    with Poison -> true in
  check "без on_error Poison пробрасывается" raised;

  (* ── window_fold: add падает на ядовитом значении ── *)
  Printf.printf "\n-- window_fold: ядовитое значение в add → on_error\n";
  let module ByK = Keyed.Make (struct
    type t = string * int
    let key (k, _) = k
  end) in
  let wf_caught = ref 0 in
  let folded =
    [ Mf_event.data ("w", 1) 10;
      Mf_event.data ("w", 666) 20;   (* add бросит *)
      Mf_event.data ("w", 2) 30;
      Mf_event.wm 1000 ]
    |> Stream.of_list
    |> Pipe.window_fold (module ByK)
         ~on_error:(fun _e _ev -> incr wf_caught)
         (Pipe.tumbling 100)
         ~init:(fun () -> 0)
         ~add:(fun acc (_k, v) -> if v = 666 then raise Poison else acc + v)
    |> Pipe.collect in
  check "window_fold: ядовитое значение поймано" (!wf_caught = 1);
  (* окно [0,100): 1 + 2 = 3 (666 пропущено) *)
  check "window_fold: аккумулятор без ядовитого (1+2=3)"
    (List.exists (fun (_k, acc) -> acc = 3) folded);

  Printf.printf "\non_error tests passed.\n"
