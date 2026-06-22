(** Регрессия: silence_age не должен схлопывать разные ключи из-за
    коллизии Hashtbl.hash (тот же класс, что R3 в trigger).

    Раньше silence_age использовал string_of_int (Hashtbl.hash key)
    как ключ строкового Hashtbl состояния. Hashtbl.hash — 30 бит, два
    разных ключа могли дать одну строку и схлопнуть свои last_seen в
    одну ячейку. Фикс: детерминированный 128-битный ключ (Marshal+MD5).

    Тест прогоняет пару строк, реально коллидирующих под Hashtbl.hash,
    через настоящий Item.silence_age и проверяет, что каждый ключ
    получает свои (key, 0)-эмиссии независимо. *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

(* Пара строк с ОДИНАКОВЫМ Hashtbl.hash (найдена перебором, та же что
   в test_trigger_key_collision). *)
let k1 = "k44842"
let k2 = "k45283"

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  silence_age: key collision safety\n";
  Printf.printf "==========================================\n";

  Printf.printf "\n-- precondition: keys collide under Hashtbl.hash\n";
  check "Hashtbl.hash k1 = Hashtbl.hash k2"
    (Hashtbl.hash k1 = Hashtbl.hash k2);
  check "k1 <> k2" (k1 <> k2);

  (* Поток: по одному Data-событию на каждый ключ. Каждое должно дать
     ровно одну (key, 0)-эмиссию для СВОЕГО ключа. Если ключи
     схлопнуты, второе событие перезапишет состояние первого, но
     (key,0) эмитится по самому событию — поэтому проверяем, что в
     выходе присутствуют ОБА ключа со своим нулём, и age растёт
     независимо. *)
  Printf.printf "\n-- two colliding keys produce independent (key,0)\n";
  let events = [
    Mf_event.data (k1, 1.0) 0;
    Mf_event.data (k2, 2.0) 100;
    Mf_event.wm 200;
  ] in
  let out =
    events |> Stream.of_list
    |> Item.silence_age ~by:(fun ((k,_):string*float) -> k) ~tick:(Time.seconds 1000)
    |> (fun s ->
        let acc = ref [] in
        let rec go () = match s () with
          | None -> ()
          | Some (Mf_event.Data ((k, age), _)) -> acc := (k, age) :: !acc; go ()
          | Some _ -> go ()
        in go (); List.rev !acc)
  in
  Printf.printf "  emissions: [%s]\n%!"
    (String.concat "; " (List.map (fun (k,a) -> Printf.sprintf "(%s,%d)" k a) out));
  check "k1 got a (k1, 0) emission"
    (List.exists (fun (k,a) -> k = k1 && a = 0) out);
  check "k2 got a (k2, 0) emission"
    (List.exists (fun (k,a) -> k = k2 && a = 0) out);

  (* Ключевой инвариант коллизии: таймер ключа читает состояние ИМЕННО
     своего ключа. on_timer эмитит (key, wm - last_seen), читая
     states[ks]. Если k1 и k2 коллидируют в ks, событие k2 затирает
     состояние k1, и таймер k1 эмитит age для ЧУЖОГО ключа (k2).

     Сценарий: k1@0, затем k2@50 (затёр бы k1 при коллизии), tick=100.
     На wm=100 срабатывает таймер. Корректно: оба ключа имеют свои
     last_seen (k1=0, k2=50), и под одним ks таймеры не путаются. *)
  Printf.printf "\n-- collision does not mix per-key last_seen (timer path)\n";
  let events2 = [
    Mf_event.data (k1, 1.0) 0;         (* k1: last_seen=0, таймер на 100 *)
    Mf_event.data (k2, 2.0) 50;        (* k2: last_seen=50, таймер на 150 *)
    Mf_event.wm 100;                   (* таймер k1 (fire_at=100) срабатывает *)
    Mf_event.wm 150;                   (* таймер k2 (fire_at=150) срабатывает *)
    Mf_event.wm 200;
  ] in
  let out2 =
    events2 |> Stream.of_list
    |> Item.silence_age ~by:(fun ((k,_):string*float) -> k) ~tick:100
    |> (fun s ->
        let acc = ref [] in
        let rec go () = match s () with
          | None -> ()
          | Some (Mf_event.Data ((k, age), wm)) -> acc := (k, age, wm) :: !acc; go ()
          | Some _ -> go ()
        in go (); List.rev !acc)
  in
  Printf.printf "  emissions: [%s]\n%!"
    (String.concat "; " (List.map (fun (k,a,w) ->
       Printf.sprintf "(%s,age=%d@%d)" k a w) out2));
  (* k1: событие@0 → таймер@100 → age = 100-0 = 100. Если коллизия
     схлопнула k1 в k2, таймер@100 прочитал бы last_seen k2 (50) и/или
     эмитнул бы под ключом k2 — оба случая ломают инвариант. *)
  check "k1 timer at wm=100 emits (k1, age=100) — own last_seen, own key"
    (List.exists (fun (k,a,w) -> k = k1 && a = 100 && w = 100) out2);
  check "k2 timer at wm=150 emits (k2, age=100) — own last_seen, own key"
    (List.exists (fun (k,a,w) -> k = k2 && a = 100 && w = 150) out2);
  (* при коллизии под одним ks остаётся только ОДИН таймер (второй
     insert на тот же ks может затереть/смешать), поэтому пропал бы
     один из ключей в timer-эмиссиях *)
  let k1_timer_emits = List.filter (fun (k,a,_) -> k = k1 && a > 0) out2 in
  let k2_timer_emits = List.filter (fun (k,a,_) -> k = k2 && a > 0) out2 in
  check "both keys produce timer (age>0) emissions independently"
    (List.length k1_timer_emits >= 1 && List.length k2_timer_emits >= 1);

  Printf.printf "\nsilence_age key-collision regression passed.\n"
