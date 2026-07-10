(* bench_checkpoint_state — нужен ли инкрементальный чекпоинт?

   Вопрос (из внешнего ревью, отложенный как «архитектурное решение»):
   снапшот пишется ЦЕЛИКОМ на каждый чекпоинт — State_backend.snapshot
   делает fold по ВСЕМ парам + Marshal, стоимость O(весь стейт)
   независимо от того, сколько ключей реально изменилось с прошлого
   чекпоинта. Инкрементальный чекпоинт писал бы только дельту.

   Меряем тремя частями:
   1. Сырая стоимость snapshot/restore по сетке (ключи × размер значения)
      — сколько стоит полный снапшот стейта данного размера.
   2. Модель инкрементальности: снапшот только изменённой доли (churn
      1%/10%/50%) — во сколько раз дельта дешевле полного.
   3. Сквозной overhead в run_exactly_once: время с чекпоинтами против
      без, на лёгком и тяжёлом стейте → амортизированная цена и пауза
      на чекпоинт.

   Итог — проекция на профиль minePASS (лампы ~2-5k ключей, состояние
   на ключ ~100-500Б) и порог, где инкрементальность становится нужна. *)

let time_med ~runs f =
  (* медиана wall-времени; первый прогон — warmup, не считается *)
  ignore (f ());
  let ts = Array.init runs (fun _ ->
    let t0 = Unix.gettimeofday () in
    ignore (f ());
    Unix.gettimeofday () -. t0) in
  Array.sort compare ts;
  ts.(runs / 2)

let mb b = float_of_int b /. 1024. /. 1024.

(* ── Часть 1+2: сырой snapshot/restore + модель дельты ── *)

let fill_backend n vbytes =
  let bk = Miniflink.State_backend_memory.create () in
  let v = Bytes.make vbytes 'x' in
  for i = 0 to n - 1 do
    Miniflink.State_backend_memory.set bk ("k" ^ string_of_int i) v
  done;
  bk

let part1_2 () =
  Printf.printf "── 1. Полный snapshot/restore: стоимость от размера стейта ──\n\n";
  Printf.printf "%10s %8s %10s | %10s %10s %10s\n"
    "ключей" "знач,Б" "стейт" "snap,мс" "restore,мс" "МБ/с";
  let grid = [ (1_000, 64); (10_000, 64); (100_000, 64);
               (1_000, 1024); (10_000, 1024); (100_000, 1024) ] in
  let snap_ms = Hashtbl.create 8 in
  List.iter (fun (n, v) ->
    let bk = fill_backend n v in
    let snap = ref Bytes.empty in
    let t_snap = time_med ~runs:5 (fun () ->
      snap := Miniflink.State_backend_memory.snapshot bk) in
    let size = Bytes.length !snap in
    let bk2 = Miniflink.State_backend_memory.create () in
    let t_rest = time_med ~runs:5 (fun () ->
      Miniflink.State_backend_memory.restore bk2 !snap) in
    Hashtbl.replace snap_ms (n, v) (t_snap *. 1000.);
    Printf.printf "%10d %8d %9.1fМ | %10.2f %10.2f %10.0f\n"
      n v (mb size) (t_snap *. 1000.) (t_rest *. 1000.)
      (mb size /. t_snap)
  ) grid;

  Printf.printf "\n── 2. Модель инкрементальности: дельта против полного ──\n";
  Printf.printf "   (если между чекпоинтами меняется c%% ключей, дельта ≈ снапшот c%%·K)\n\n";
  Printf.printf "%10s %8s | %12s %12s %12s\n"
    "ключей" "знач,Б" "churn 1%" "churn 10%" "churn 50%";
  List.iter (fun (n, v) ->
    let full = Hashtbl.find snap_ms (n, v) in
    let delta c =
      let bkc = fill_backend (max 1 (n * c / 100)) v in
      let t = time_med ~runs:5 (fun () ->
        ignore (Miniflink.State_backend_memory.snapshot bkc)) in
      t *. 1000. in
    let d1 = delta 1 and d10 = delta 10 and d50 = delta 50 in
    Printf.printf "%10d %8d | %6.2fмс %2.0fx %6.2fмс %2.0fx %6.2fмс %2.0fx\n"
      n v d1 (full /. d1) d10 (full /. d10) d50 (full /. d50)
  ) [ (10_000, 64); (100_000, 64); (100_000, 1024) ]

(* ── Часть 3: сквозной overhead в run_exactly_once ── *)

let e2e ~workers ~events ~keys ~vbytes ~checkpoint_every =
  let evs = List.init events (fun i ->
    Miniflink.Mf_event.data ("k" ^ string_of_int (i mod keys)) (i * 10)) in
  let v = Bytes.make vbytes 'x' in
  let process backend ev =
    match ev with
    | Miniflink.Mf_event.Data (key, _) ->
      Miniflink.State_backend_memory.set backend key v; [key]
    | _ -> [] in
  fun () ->
    let store = Miniflink.Checkpoint_parallel.make_store () in
    Miniflink.Checkpoint_parallel.run_exactly_once
      ~workers ~capacity:64 ~checkpoint_every
      ~key_of:(fun k -> k)
      ~make_state:Miniflink.State_backend_memory.create
      ~process
      ~source:(Miniflink.Checkpoint_parallel.seekable_of_list evs)
      ~sink:(Miniflink.Checkpoint_parallel.idempotent_sink (fun _ -> ()))
      ~store
      ()

let part3 () =
  Printf.printf "\n── 3. Сквозной overhead чекпоинтов в run_exactly_once ──\n";
  Printf.printf "   (события=40000, воркеров=4; off = чекпоинт один в самом конце)\n\n";
  Printf.printf "%22s | %9s %9s %9s | %8s %12s\n"
    "стейт" "off,мс" "cp1000,мс" "cp250,мс" "ovh@1000" "пауза/cp,мс";
  let events = 40_000 and workers = 4 in
  List.iter (fun (keys, vbytes, label) ->
    let t_off = time_med ~runs:3
        (e2e ~workers ~events ~keys ~vbytes ~checkpoint_every:(events + 1)) in
    let t_cp1k = time_med ~runs:3
        (e2e ~workers ~events ~keys ~vbytes ~checkpoint_every:1000) in
    let t_cp250 = time_med ~runs:3
        (e2e ~workers ~events ~keys ~vbytes ~checkpoint_every:250) in
    let n_cp = events / 1000 in
    let ovh = (t_cp1k -. t_off) /. t_off *. 100. in
    let pause = (t_cp1k -. t_off) /. float_of_int n_cp *. 1000. in
    Printf.printf "%22s | %9.0f %9.0f %9.0f | %7.1f%% %12.2f\n"
      label (t_off *. 1000.) (t_cp1k *. 1000.) (t_cp250 *. 1000.) ovh pause
  ) [ (2_000, 128, "minePASS: 2k x 128Б");
      (20_000, 512, "тяжёлый: 20k x 512Б");
      (50_000, 1024, "оч.тяжёлый: 50k x 1КБ") ]

let () =
  Printf.printf "═══════════════════════════════════════════════════════════\n";
  Printf.printf "  Чекпоинт тяжёлого стейта: нужен ли инкрементальный?\n";
  Printf.printf "═══════════════════════════════════════════════════════════\n\n";
  part1_2 ();
  part3 ();
  Printf.printf "\n(интерпретация в конце вывода не печатается — см. анализ)\n"
