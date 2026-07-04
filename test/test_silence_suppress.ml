(** P3.1/P3.2 (G-2a, G-2b): on_silence (absence) и suppress_while.

    on_silence:
    - переход в тишину, когда watermark уходит за last+within, со
      временем перехода (last+within, не watermark);
    - recovery, когда has-событие приходит после тишины;
    - has-фильтр: событие без признака не сбрасывает тишину.

    suppress_while:
    - suppressed-события глушатся по ключу, пока controller включил
      подавление ([`On]); проходят после [`Off]. *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

(* значение: (lamp, has_reading) *)
module ByLamp = Keyed.Make (struct
  type t = string * bool
  let key (k, _) = k
end)

type alert = Silent of string * Time.t | Resumed of string * Time.t

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  P3.1/P3.2: on_silence + suppress_while\n";
  Printf.printf "==========================================\n";

  (* ── on_silence: тишина по порогу within=100 ── *)
  Printf.printf "\n-- on_silence: переход в тишину со временем last+within\n";
  let out1 =
    [ Mf_event.data ("a", true) 10;   (* наблюдение, дедлайн 110 *)
      Mf_event.wm 50;                  (* рано, тишины нет *)
      Mf_event.wm 130 ]                (* 130 >= 110 → тишина в t=110 *)
    |> Stream.of_list
    |> Pipe.on_silence (module ByLamp)
         ~within:100
         ~has:(fun _ -> true)
         ~on_silent:(fun k ~last:_ ~ts -> Silent (k, ts))
         ~on_resumed:(fun k ~ts -> Resumed (k, ts))
    |> Pipe.collect in
  check "тишина эмитится в момент last+within (110), не watermark (130)"
    (List.exists (function Silent ("a", 110) -> true | _ -> false) out1);

  (* ── recovery: has-событие после тишины ── *)
  Printf.printf "\n-- on_silence: recovery при возобновлении\n";
  let out2 =
    [ Mf_event.data ("a", true) 10;
      Mf_event.wm 130;                 (* тишина в 110 *)
      Mf_event.data ("a", true) 140;   (* возобновление → Resumed *)
      Mf_event.wm 150 ]
    |> Stream.of_list
    |> Pipe.on_silence (module ByLamp)
         ~within:100
         ~has:(fun _ -> true)
         ~on_silent:(fun k ~last:_ ~ts -> Silent (k, ts))
         ~on_resumed:(fun k ~ts -> Resumed (k, ts))
    |> Pipe.collect in
  check "есть тишина и последующий recovery"
    (List.exists (function Silent _ -> true | _ -> false) out2
     && List.exists (function Resumed ("a", 140) -> true | _ -> false) out2);

  (* ── has-фильтр: событие без признака не сбрасывает тишину ── *)
  Printf.printf "\n-- on_silence: has-фильтр (только has-события считаются)\n";
  let out3 =
    [ Mf_event.data ("a", true) 10;    (* has=true, дедлайн 110 *)
      Mf_event.data ("a", false) 50;   (* has=false — НЕ сбрасывает *)
      Mf_event.wm 130 ]                (* тишина всё равно в 110 *)
    |> Stream.of_list
    |> Pipe.on_silence (module ByLamp)
         ~within:100
         ~has:(fun (_, r) -> r)         (* только события с readings *)
         ~on_silent:(fun k ~last:_ ~ts -> Silent (k, ts))
    |> Pipe.collect in
  check "has=false не отодвигает тишину (эмит в 110)"
    (List.exists (function Silent ("a", 110) -> true | _ -> false) out3);

  (* ── монотонность event-time: late-событие не откатывает дедлайн ── *)
  Printf.printf "\n-- on_silence: late-событие не откатывает last_seen назад\n";
  let out_mono =
    [ Mf_event.data ("a", true) 100;   (* наблюдение t=100, дедлайн 200 *)
      Mf_event.data ("a", true) 50;    (* LATE (ts<100) — НЕ откатывает *)
      Mf_event.wm 210 ]                (* тишина от 100+100=200, не 50+100=150 *)
    |> Stream.of_list
    |> Pipe.on_silence (module ByLamp)
         ~within:100
         ~has:(fun _ -> true)
         ~on_silent:(fun k ~last:_ ~ts -> Silent (k, ts))
    |> Pipe.collect in
  check "late-событие не сдвинуло дедлайн назад (тишина в 200, не 150)"
    (List.exists (function Silent ("a", 200) -> true | _ -> false) out_mono);

  (* ── suppress_while: motion глушится при No_packets ── *)
  Printf.printf "\n-- suppress_while: подавление по ключу\n";
  (* controller: строки-события "on"/"off" по ключу; suppressed: int-алерты *)
  let controller =
    [ Mf_event.data ("x", `On) 10;     (* блокируем x *)
      Mf_event.data ("y", `On) 10;
      Mf_event.data ("x", `Off) 100 ]  (* разблокируем x *)
    |> Stream.of_list in
  let suppressed =
    [ Mf_event.data ("x", 1) 20;       (* x заблокирован → глушится *)
      Mf_event.data ("y", 2) 20;       (* y заблокирован → глушится *)
      Mf_event.data ("x", 3) 110 ]     (* x разблокирован → проходит *)
    |> Stream.of_list in
  let out4 =
    Pipe.suppress_while
      ~controller_key:(fun (k, _) -> Some k)
      ~gate:(fun (k, g) -> match g with `On -> `On k | `Off -> `Off k)
      ~suppressed_key:(fun (k, _) -> k)
      controller suppressed
    |> Pipe.collect in
  let vals = List.map snd out4 in
  check "заблокированные x@20, y@20 подавлены" (not (List.mem 1 vals) && not (List.mem 2 vals));
  check "разблокированный x@110 прошёл (=3)" (List.mem 3 vals);

  Printf.printf "\non_silence + suppress_while tests passed.\n"
