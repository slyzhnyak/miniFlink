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

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Bugs #5 (dedup evict) + #6 (hash_key)\n";
  Printf.printf "==========================================\n";
  test_hash_nonneg ();
  test_dedup_eviction ();
  Printf.printf "\nAll tests passed.\n"
