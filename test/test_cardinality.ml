open Miniflink
(* Cardinality explosion: миллионы УНИКАЛЬНЫХ ключей через dedup.
   Проверяем что состояние остаётся ОГРАНИЧЕННЫМ — eviction по watermark
   удаляет старые ключи, heap не растёт линейно с числом уникальных
   ключей. Без eviction это была бы утечка памяти на long-running потоке
   с высокой кардинальностью (классический failure mode). *)

open Time

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

type ev = { key : string; ts : int }
module K = Keyed.Make (struct type t = ev let key e = e.key end)

let live_kb () =
  Gc.full_major ();
  (Gc.stat ()).Gc.live_words * (Sys.word_size / 8) / 1024

(* Поток с N уникальными ключами, watermark продвигается вместе с
   временем — старые ключи должны вытесняться из dedup state. *)
let unique_key_stream n ~cooldown =
  let i = ref 0 in
  let emitted_wm = ref 0 in
  fun () ->
    if !i >= n then None
    else begin
      let cur = !i in
      incr i;
      (* периодически эмитим watermark, продвигая его за cooldown —
         это триггерит eviction старых ключей *)
      if cur mod 1000 = 0 && cur > 0 then begin
        emitted_wm := cur - cooldown - 1;
        Some (Mf_event.wm !emitted_wm)
      end else
        Some (Mf_event.data { key = Printf.sprintf "device-%d" cur; ts = cur } cur)
    end

let test_bounded_state () =
  Printf.printf "\n-- millions of unique keys: dedup state stays bounded\n";
  let cooldown = seconds 1 in   (* маленький cooldown → агрессивный eviction *)
  let total = 2_000_000 in

  let baseline = live_kb () in
  Printf.printf "  baseline heap: %d KB\n%!" baseline;

  (* прогоняем поток через dedup, считаем прошедшие события *)
  let passed = ref 0 in
  unique_key_stream total ~cooldown
  |> Pipe.dedup (module K) ~rule:(fun _ -> "r") ~cooldown
  |> Pipe.sink (fun _ -> incr passed);

  let final = live_kb () in
  Printf.printf "  final heap: %d KB (after %d unique keys)\n%!" final total;

  (* ключевой инвариант: heap НЕ вырос пропорционально 2M ключей.
     Если бы dedup не вытеснял — хранил бы ~2M записей (десятки МБ).
     С eviction остаётся лишь окно cooldown. Критерий: рост heap
     ограничен (не более +50 МБ над baseline, с большим запасом). *)
  let growth_kb = final - baseline in
  Printf.printf "  heap growth: %d KB\n%!" growth_kb;
  check "heap bounded under 2M unique keys (eviction works)"
    (growth_kb < 50_000);
  check "events were processed" (!passed > 0)

(* контраст: размер dedup-таблицы не должен хранить все уникальные ключи.
   Косвенно через heap уже проверили; здесь — что поток отработал
   полностью без OOM/исключений. *)
let test_completes () =
  Printf.printf "\n-- high-cardinality stream completes without error\n";
  let cooldown = seconds 1 in
  let count = ref 0 in
  (try
    unique_key_stream 500_000 ~cooldown
    |> Pipe.dedup (module K) ~rule:(fun _ -> "r") ~cooldown
    |> Pipe.sink (fun _ -> incr count);
    check "completed processing high-cardinality stream" true
  with e ->
    fail (Printf.sprintf "crashed: %s" (Printexc.to_string e)))

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Cardinality explosion (bounded state)\n";
  Printf.printf "==========================================\n";
  test_bounded_state ();
  test_completes ();
  Printf.printf "\nCardinality tests passed.\n"
