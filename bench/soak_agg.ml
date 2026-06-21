(** Soak для операторов, изменённых в раундах 4-5: group_by (Map),
    window_agg (noop-suppression), efilt. Проверяет что при долгом
    прогоне с непрерывными late-data коррекциями heap остаётся
    ограниченным — окна вычищаются за allowed_lateness, Map-
    аккумуляторы не накапливаются.

    Запуск: dune exec bench/soak_agg.exe -- 5000000 *)

open Miniflink

module K = struct type t = int * string * float let key (w,_,_) = string_of_int (w mod 64) end

let live_kb () =
  Gc.full_major ();
  (Gc.stat ()).Gc.live_words * (Sys.word_size / 8) / 1024

(* Бесконечный поток: окна по 1с, 3 группы на окно + late событие в
   предыдущее окно (даёт постоянные corrections). Время монотонно. *)
let make_source n =
  let i = ref 0 in
  fun () ->
    if !i >= n then None
    else begin
      let idx = !i in incr i;
      let w = idx / 4 in
      let base = w * 1000 in
      let ev = match idx mod 4 with
        | 0 -> Mf_event.data (w, "B1", float (idx mod 50)) base
        | 1 -> Mf_event.data (w, "B2", float (idx mod 30)) (base + 100)
        | 2 -> Mf_event.wm (base + 1500)   (* fire окна w-1 *)
        | _ -> (* late в окно w-1: непрерывные corrections *)
          if w > 0 then Mf_event.data (w - 1, "B3", float (idx mod 90)) ((w-1)*1000 + 200)
          else Mf_event.data (w, "B3", 1.0) (base + 200)
      in Some ev
    end

let () =
  let n = try int_of_string Sys.argv.(1) with _ -> 2_000_000 in
  Printf.printf "=== Soak agg (group_by/window_agg/efilt): %d events ===\n%!" n;
  let baseline = live_kb () in
  Printf.printf "baseline live heap: %d KB\n%!" baseline;

  let checkpoints = ref [] in
  let processed = ref 0 in
  let check_every = max 1 (n / 10) in
  let src = make_source n in
  let wrapped () =
    let r = src () in
    (match r with Some _ ->
      incr processed;
      if !processed mod check_every = 0 then
        checkpoints := (!processed, live_kb ()) :: !checkpoints
     | None -> ());
    r in

  let updates = ref 0 and datas = ref 0 in
  wrapped
  |> Pipe.window_agg (module K) ~allowed_lateness:2000 (Pipe.tumbling 1000)
       Agg.(group_by ~key:(fun (_,b,_) -> b) ~inner:(median (fun (_,_,r) -> r)))
  |> Pipe.filter (fun (_key, groups) -> List.length groups > 0)  (* efilt путь *)
  |> Pipe.sink (fun _ -> ());
  ignore datas;

  (* пересчёт updates отдельным проходом для статистики не нужен —
     важен heap. Но прогоним поток до конца через sink выше. *)
  let final = live_kb () in
  Printf.printf "\nCheckpoints (events -> live heap KB):\n";
  List.iter (fun (p, kb) -> Printf.printf "  %9d -> %6d KB\n" p kb)
    (List.rev !checkpoints);
  Printf.printf "\nfinal live heap: %d KB\n" final;
  ignore updates;

  let cps = List.rev !checkpoints in
  (match cps with
   | (_, first) :: _ ->
     let (_, last) = List.nth cps (List.length cps - 1) in
     let ratio = float last /. float (max 1 first) in
     Printf.printf "growth ratio (last/first): %.2fx\n" ratio;
     if ratio <= 3.0 then Printf.printf "\nPASS: heap bounded (window purge + Map acc not leaking)\n"
     else Printf.printf "\nFAIL: heap grew %.1fx — possible leak\n" ratio
   | _ -> Printf.printf "no checkpoints\n")
