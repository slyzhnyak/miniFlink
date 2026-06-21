(** Регрессия R3: trigger не должен схлопывать разные ключи в одну
    state-машину из-за коллизии Hashtbl.hash.

    Раньше trigger использовал [string_of_int (Hashtbl.hash key)] как
    ключ строковых Hashtbl состояния и таймеров. Hashtbl.hash — 30
    бит, поэтому разные ключи могут дать одну строку; их state-машины
    схлопывались (silent state corruption). Фикс: детерминированный
    128-битный ключ (Marshal + MD5).

    Этот тест — не симуляция: он прогоняет ДВА ключа, реально
    коллидирующих под Hashtbl.hash, через настоящий Trigger.of_stream
    и проверяет, что их триггеры срабатывают независимо. *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

(* Пара строк, дающих ОДИНАКОВЫЙ Hashtbl.hash (найдена перебором).
   Под старым кодом обе делили бы один слот в state-таблице. *)
let k1 = "k44842"
let k2 = "k45283"

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Trigger: key collision safety (R3)\n";
  Printf.printf "==========================================\n";

  (* Предпосылка теста: ключи действительно коллидируют под
     Hashtbl.hash, но различны. Если это перестанет быть так (другая
     версия OCaml), тест нужно обновить — но фикс от этого не
     зависит. *)
  Printf.printf "\n-- precondition: keys collide under Hashtbl.hash\n";
  check "Hashtbl.hash k1 = Hashtbl.hash k2"
    (Hashtbl.hash k1 = Hashtbl.hash k2);
  check "k1 <> k2" (k1 <> k2);

  (* Триггер: alert когда value > 100, без debounce (срабатывает
     сразу). produce_alert возвращает сам ключ — так в выходе видно,
     ЧЕЙ триггер сработал. *)
  let spec =
    Trigger.create
      ~name:"collision_test"
      ~condition:(Trigger.greater_than 100.0)
      ~produce_alert:(fun ~key ~value:_ ~ts:_ -> key)
      ~produce_recovery:(fun ~key ~ts:_ -> key ^ "_ok")
      ()
  in

  (* Поток: k1 уходит в Problem (value 200 > 100), k2 остаётся Ok
     (value 50 < 100). Если ключи схлопнуты, событие k2 затрёт/собьёт
     state k1, и мы либо не увидим alert для k1, либо увидим лишний
     для k2. *)
  Printf.printf "\n-- k1 fires (200>100), k2 stays ok (50<100)\n";
  let events = [
    Mf_event.data (k1, 200.0) 0;     (* k1 → Problem, alert = k1 *)
    Mf_event.data (k2, 50.0)  100;   (* k2 → Ok, без alert *)
    Mf_event.wm 1000;
  ] in
  let alerts =
    events |> Stream.of_list |> Trigger.of_stream spec
    |> (fun s ->
        let acc = ref [] in
        let rec go () = match s () with
          | None -> ()
          | Some (Mf_event.Data (a, _)) -> acc := a :: !acc; go ()
          | Some _ -> go ()
        in go (); List.rev !acc)
  in
  Printf.printf "  alerts: [%s]\n%!" (String.concat "; " alerts);
  check "exactly one alert" (List.length alerts = 1);
  check "alert is for k1 (its state machine, not collapsed with k2)"
    (alerts = [k1]);

  (* Симметрично: теперь k2 уходит в Problem, k1 остаётся Ok. Свежий
     trigger. Подтверждает, что слот k2 не «занят» k1. *)
  Printf.printf "\n-- symmetric: k2 fires, k1 stays ok\n";
  let spec2 =
    Trigger.create
      ~name:"collision_test2"
      ~condition:(Trigger.greater_than 100.0)
      ~produce_alert:(fun ~key ~value:_ ~ts:_ -> key)
      ~produce_recovery:(fun ~key ~ts:_ -> key ^ "_ok")
      ()
  in
  let events2 = [
    Mf_event.data (k1, 50.0)  0;
    Mf_event.data (k2, 200.0) 100;
    Mf_event.wm 1000;
  ] in
  let alerts2 =
    events2 |> Stream.of_list |> Trigger.of_stream spec2
    |> (fun s ->
        let acc = ref [] in
        let rec go () = match s () with
          | None -> ()
          | Some (Mf_event.Data (a, _)) -> acc := a :: !acc; go ()
          | Some _ -> go ()
        in go (); List.rev !acc)
  in
  Printf.printf "  alerts: [%s]\n%!" (String.concat "; " alerts2);
  check "exactly one alert (k2)" (alerts2 = [k2]);

  Printf.printf "\nTrigger key-collision regression passed.\n"
