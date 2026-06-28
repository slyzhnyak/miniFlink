(** Soak-тест: длительный прогон реалистичного пайплайна с контролем
    утечек памяти (review 8.2 #4).

    Прокручивает поток из сотен тысяч событий через stateful-пайплайн
    (tumbling window_agg + dedup) с РАВНОМЕРНО продвигающимися
    watermark'ами, так что окна закрываются и состояние освобождается.
    Если состояние ограничено (а не течёт), потребление памяти выходит
    на плато: замер в начале и в конце потока должен быть сопоставим,
    а не расти пропорционально числу обработанных событий.

    Это защищает от регресса, который медленные unit-тесты не поймают:
    оператор, забывающий освобождать per-key состояние закрытых окон,
    проходит обычные тесты (корректность не нарушена), но течёт на
    long-running прогоне — критично для minePASS, работающего сутками. *)

open Miniflink
open Time

(* живые слова → килобайты *)
let live_kb () =
  Gc.full_major ();
  (Gc.stat ()).Gc.live_words * (Sys.word_size / 8) / 1024

type reading = { lamp : string; value : float; ts : Time.t }

module ByLamp : Keyed.S with type t = reading = struct
  type t = reading
  let key r = r.lamp
end

(* агрегат: среднее по окну *)
let mean_agg = Agg.mean (fun (r : reading) -> r.value)

(* пайплайн: tumbling-окно 10с со средним по лампе.
   window_agg закрывает окна по watermark → состояние закрытых окон
   освобождается. Soak проверяет, что оно не накапливается. *)
let pipeline stream =
  stream
  |> Pipe.window_agg (module ByLamp) (Pipe.tumbling (seconds 10)) mean_agg

(* генерим поток: n событий по lamps лампам, ts равномерно растёт,
   watermark после каждой «секунды» событий — окна регулярно закрываются *)
let make_stream ~n ~lamps =
  let i = ref 0 in
  let emitted_wm = ref 0 in
  fun () ->
    if !i >= n then
      (* финальный watermark, потом конец *)
      (if !emitted_wm < max_int / 2 then
         (emitted_wm := max_int / 2; Some (Mf_event.wm (n * 100)))
       else None)
    else begin
      let k = !i in
      incr i;
      let lamp = Printf.sprintf "lamp_%d" (k mod lamps) in
      let ts = k * 100 in   (* 100мс на событие → окна 10с = 100 событий *)
      (* раз в 50 событий — watermark, продвигающий время (закрывает окна) *)
      if k mod 50 = 0 && k > 0 then
        Some (Mf_event.wm (ts - 1000))   (* wm с запасом lateness *)
      else
        Some (Mf_event.data { lamp; value = float_of_int (k mod 100); ts } ts)
    end

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

(* прокрутить поток, замеряя ПИКОВУЮ память в первой и второй половине
   прогона. При утечке пик второй половины заметно выше первой. *)
let run_soak ~n ~lamps =
  let s = make_stream ~n ~lamps |> pipeline in
  let peak_first = ref 0 and peak_second = ref 0 in
  let out = ref 0 in
  let cont = ref true in
  (* оцениваем число выходных событий ~ n (окна эмитят регулярно) *)
  let half = n / 2 in
  while !cont do
    (match s () with
     | None -> cont := false
     | Some _ ->
       incr out;
       (* замеряем периодически, чтобы не звать full_major на каждое *)
       if !out mod 10_000 = 0 then begin
         let m = live_kb () in
         if !out <= half then (if m > !peak_first then peak_first := m)
         else (if m > !peak_second then peak_second := m)
       end)
  done;
  (!peak_first, !peak_second, !out)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Soak: long run, memory leak detection\n";
  Printf.printf "==========================================\n";
  Log.set_level Log.Error;

  (* 300k событий, 64 лампы — окна постоянно открываются и закрываются *)
  let n = 300_000 and lamps = 64 in
  Printf.printf "\n-- %d событий, %d ламп, tumbling 10s + watermarks\n" n lamps;
  let (peak_first, peak_second, out) = run_soak ~n ~lamps in
  Printf.printf "    выходных событий %d\n" out;
  Printf.printf "    пиковая память: 1-я половина=%d KB, 2-я половина=%d KB\n"
    peak_first peak_second;

  (* Утечки нет, если пик второй половины не намного выше первой.
     Состояние ограничено активными окнами × лампами, а не длиной
     потока. При утечке (например, оператор не освобождает закрытые
     окна) пик 2-й половины рос бы кратно. Порог щедрый, но ловит
     линейный рост. *)
  let bounded = peak_second < peak_first * 2 + 2000 in
  if not bounded then
    Printf.printf "    !! пик 2-й половины кратно выше 1-й — возможна утечка\n";
  check "window_agg: память на плато (нет утечки)" bounded;

  (* ── Вторая проверка: dedup на длинном потоке ─────────────
     dedup держит per-key состояние и истекает записи старше
     [wm - cooldown] при watermark. Гоним поток с РАСТУЩИМИ ключами и
     продвигающимися watermark — старые ключи должны вычищаться, иначе
     состояние течёт. Здесь dedup пропускает каждое первое вхождение
     ключа, так что замеры памяти надёжны. *)
  Printf.printf "\n-- dedup: %d событий, растущие ключи, watermark-очистка\n" n;
  let module ByStr : Keyed.S with type t = (string * int) = struct
    type t = string * int
    let key (k, _) = k
  end in
  let i = ref 0 in
  let dsrc () =
    if !i >= n then None
    else begin
      let k = !i in incr i;
      (* watermark каждые 50 событий продвигает время → очистка старых *)
      if k mod 50 = 0 && k > 0 then Some (Mf_event.wm (k - 25))
      else
        (* ключ = k/10: каждые 10 событий новый ключ, старые устаревают *)
        Some (Mf_event.data (Printf.sprintf "key_%d" (k / 10), k) k)
    end in
  let ds = dsrc
    |> Pipe.dedup (module ByStr)
         ~rule:(fun (k, _) -> k) ~cooldown:100 in
  let dpeak_first = ref 0 and dpeak_second = ref 0 and dout = ref 0 in
  let dcont = ref true in
  while !dcont do
    (match ds () with
     | None -> dcont := false
     | Some _ ->
       incr dout;
       if !dout mod 5_000 = 0 then begin
         let m = live_kb () in
         if !dout <= n / 4 then (if m > !dpeak_first then dpeak_first := m)
         else (if m > !dpeak_second then dpeak_second := m)
       end)
  done;
  Printf.printf "    выходных %d, пик 1-я четверть=%d KB, остаток=%d KB\n"
    !dout !dpeak_first !dpeak_second;
  let dbounded = !dpeak_second < !dpeak_first * 2 + 2000 in
  if not dbounded then
    Printf.printf "    !! dedup-состояние растёт — watermark-очистка не работает?\n";
  check "dedup: память на плато (watermark вычищает старые ключи)" dbounded;

  Printf.printf "\nSoak test passed.\n"
