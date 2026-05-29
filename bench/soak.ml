(* ============================================================
   Soak.ml — длительный прогон для проверки утечек памяти.

   НЕ часть `dune test` (слишком долгий). Запуск вручную:
     dune exec bench/soak.exe
     dune exec bench/soak.exe -- 10000000   (10M событий)

   Проверяет: при обработке N миллионов событий через полный
   pipeline (с dedup eviction и window purging) heap остаётся
   ограниченным — не растёт линейно с числом событий.

   Если бы dedup или window не вычищали состояние (баги #5, #2),
   live_words рос бы монотонно. Тест ловит регрессии eviction.
   ============================================================ *)

open Domain
open Time

let devices = Table.of_list [
  "A",{owner="o";max_speed=90.;zone="z"}; "B",{owner="o";max_speed=80.;zone="z"};
  "C",{owner="o";max_speed=85.;zone="z"}; "D",{owner="o";max_speed=70.;zone="z"};
]

let pipeline src =
  src
  |> Mf_event.with_watermarks ~latency:(seconds 3)
  |> Pipe.enrich (module Telemetry) ~from:devices
       ~merge:(fun t d -> { t with device = d })
  |> Pipe.window (module Telemetry) (Pipe.tumbling (seconds 30))
  |> Pipe.aggregate Rules.compute
  |> Pipe.flat_map (Rules.check Rules.fleet)
  |> Pipe.dedup (module Alert) ~rule:(fun a -> a.rule) ~cooldown:(minutes 5)

(* Бесконечный генератор: device cycle, монотонное время *)
let make_source n =
  let i = ref 0 in
  fun () ->
    if !i >= n then None
    else begin
      let idx = !i in
      incr i;
      let dev = [|"A";"B";"C";"D"|].(idx mod 4) in
      let ts  = idx * 100 in   (* 100ms между событиями *)
      let speed = if idx mod 10 = 0 then 130. else 60. in
      let fuel  = if idx mod 7 = 0 then 15. else 80. in
      Some (Mf_event.data
        { device_id=dev; speed_kmh=speed; fuel_pct=fuel;
          position={lat=55.;lon=37.}; ts; device=None } ts)
    end

let live_kb () =
  Gc.full_major ();
  (Gc.stat ()).Gc.live_words * (Sys.word_size / 8) / 1024

let () =
  let n = try int_of_string Sys.argv.(1) with _ -> 2_000_000 in
  Printf.printf "=== Soak test: %d events ===\n%!" n;

  let baseline = live_kb () in
  Printf.printf "baseline live heap: %d KB\n%!" baseline;

  (* Замеряем heap на чекпойнтах *)
  let checkpoints = ref [] in
  let processed = ref 0 in
  let check_every = max 1 (n / 10) in

  let src = make_source n in
  let alerts = ref 0 in
  let wrapped () =
    let r = src () in
    (match r with Some _ ->
      incr processed;
      if !processed mod check_every = 0 then
        checkpoints := (!processed, live_kb ()) :: !checkpoints
     | None -> ());
    r
  in

  pipeline wrapped |> Pipe.sink (fun _ -> incr alerts);

  let final = live_kb () in
  Printf.printf "\nCheckpoints (events processed -> live heap KB):\n";
  List.iter (fun (p, kb) -> Printf.printf "  %9d -> %6d KB\n" p kb)
    (List.rev !checkpoints);
  Printf.printf "\nfinal live heap: %d KB\n" final;
  Printf.printf "alerts emitted:  %d\n" !alerts;

  (* Критерий: heap на последнем чекпойнте не более чем в 3x
     от первого чекпойнта. Линейный рост (утечка) дал бы ~10x. *)
  (match List.rev !checkpoints with
   | (_, first_kb) :: _ when first_kb > 0 ->
     let last_kb = snd (List.hd !checkpoints) in
     let ratio = float_of_int last_kb /. float_of_int first_kb in
     Printf.printf "growth ratio (last/first checkpoint): %.2fx\n" ratio;
     if ratio <= 3.0 then
       Printf.printf "\nPASS: heap bounded (state eviction working)\n"
     else
       (Printf.printf "\nFAIL: heap grew %.1fx — possible state leak\n" ratio;
        exit 1)
   | _ -> Printf.printf "\n(not enough checkpoints)\n")
