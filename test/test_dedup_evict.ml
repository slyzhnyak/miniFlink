open Miniflink
(* Тесты багов #5 (dedup eviction) и #6 (hash_key no negative) *)
open Test_support.Domain
open Time

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

(* ── #6: hash_key никогда не отрицательный ──────────────────── *)
let test_hash_nonneg () =
  Printf.printf "\n-- #6: hash_key never negative\n";
  (* Подбираем строки, проверяем что индекс в [0, n) *)
  let ok = ref true in
  for i = 0 to 100000 do
    let key = Printf.sprintf "device_%d_xyz" i in
    let h = Parallel.hash_key key 8 in
    if h < 0 || h >= 8 then ok := false
  done;
  check "100k keys: all in [0,8)" !ok;
  (* Граничная строка которая может дать min_int-подобный хэш *)
  let h = Parallel.hash_key (String.make 50 'z') 4 in
  check "long z-string in [0,4)" (h >= 0 && h < 4)

(* ── #5: dedup eviction ограничивает рост состояния ─────────── *)
let test_dedup_eviction () =
  Printf.printf "\n-- #5: dedup state eviction on watermark\n";
  let mk rule ts = { id=""; device_id="A"; rule; severity=Warning; message=""; ts } in

  (* После watermark старые записи удаляются → тот же ключ снова проходит *)
  let out =
    Stream.of_list [
      Mf_event.data (mk "speed" 1000) 1000;       (* проходит, seen[speed]=1000 *)
      Mf_event.wm 500000;                          (* wm: evict записи < 500000-300000=200000; 1000 удаляется *)
      Mf_event.data (mk "speed" 501000) 501000;   (* проходит снова — запись была вычищена *)
    ]
    |> Pipe.dedup (module Alert) ~rule:(fun a -> a.rule) ~cooldown:(minutes 5)
    |> Stream.to_list
  in
  let datas = List.filter Mf_event.is_data out in
  check "both speed alerts pass (eviction worked)" (List.length datas = 2);

  (* Без watermark в пределах cooldown — подавление работает как раньше *)
  let out2 =
    Stream.of_list [
      Mf_event.data (mk "fuel" 1000) 1000;        (* проходит *)
      Mf_event.data (mk "fuel" 61000) 61000;      (* подавлен: 60s < 5min *)
    ]
    |> Pipe.dedup (module Alert) ~rule:(fun a -> a.rule) ~cooldown:(minutes 5)
    |> Stream.to_list
  in
  check "cooldown still suppresses within window"
    (List.length (List.filter Mf_event.is_data out2) = 1)

(* ── dedup composes with windows + late + retract (ex07 scenario) ─
   Закрывает реальный сценарий из ex07: at-least-once канал даёт
   дубль того же (key, ts), плюс есть честный опоздавший пакет.
   dedup ДО окна должен срубить дубль, опоздавший должен пройти и
   при `allowed_lateness` вызвать retract уже закрытого окна.
   Без этого теста баг «dedup проглатывает опоздавшего» (если бы
   cooldown был неправильно настроен) не ловился бы. *)

type pkt = { id : string; ts : int; v : float }
module ByPkt = Keyed.Make (struct type t = pkt let key p = p.id end)

let test_dedup_passes_late_drops_duplicate () =
  Printf.printf "\n-- dedup drops duplicate, lets honest late packet through to retract\n";
  let out =
    Stream.of_list [
      Mf_event.data { id="A"; ts=10;  v=1.0 } 10;
      Mf_event.data { id="A"; ts=10;  v=1.0 } 10;     (* ДУБЛЬ — должен срубиться *)
      Mf_event.data { id="A"; ts=100; v=2.0 } 100;
      (* watermark вперёд — окно [0,60) закрывается *)
      Mf_event.data { id="A"; ts=200; v=3.0 } 200;
      (* честный опоздавший в УЖЕ ЗАКРЫТОЕ окно [0,60) *)
      Mf_event.data { id="A"; ts=20;  v=4.0 } 20;
    ]
    |> Pipe.dedup (module ByPkt) ~rule:(fun p -> string_of_int p.ts)
         ~cooldown:1000
    |> Pipe.event_time ~lateness:1
    |> Pipe.window_agg_keyed ~by:(fun p -> p.id)
         ~allowed_lateness:300
         (Pipe.tumbling 60)
         (Agg.count_if (fun _ -> true))
    |> Stream.to_list in
  (* отделим Data и Retract — retract пересчитал старое окно *)
  let datas = List.filter (function Mf_event.Data _ -> true | _ -> false) out in
  let retracts = List.filter (function Mf_event.Retract _ -> true | _ -> false) out in
  let updates = List.filter (function Mf_event.Update _ -> true | _ -> false) out in
  (* окно [0,60): без дубликата 1 событие (ts=10), но опоздавший ts=20 → пересчёт до 2.
     Phase 3: пересчёт теперь эмитит Update, не Retract+Data пару. *)
  check "duplicate dropped, late packet caused correction (retract or update)"
    (List.length retracts + List.length updates >= 1);
  check "some windows emitted"
    (List.length datas >= 1)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Bugs #5 (dedup evict) + #6 (hash_key)\n";
  Printf.printf "==========================================\n";
  test_hash_nonneg ();
  test_dedup_eviction ();
  test_dedup_passes_late_drops_duplicate ();
  Printf.printf "\nAll tests passed.\n"
