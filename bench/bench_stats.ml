(** Многократный прогон + статистика времени и аллокаций.

    Один прогон не отделяет шум от эффекта. На OCaml 4.14 разброс
    между прогонами ~5-15% — типичная «оптимизация на 10%» может
    оказаться шумом. Эта обвязка:
    - прогоняет [n] раз с warmup (первый прогон отбрасывается:
      cache cold, GC ещё не разогрет);
    - собирает min, median, p95, max времени и пиковой кучи;
    - печатает таблицу.

    Используется для регрессий (bench_ex07) и для микро-бенчмарков. *)

type measurement = {
  name              : string;
  wall_seconds      : float;
  allocated_mb      : float;   (** delta minor+major words с начала прогона *)
  heap_peak_mb      : float;   (** heap_words ПОСЛЕ прогона (приближение к пику) *)
}

let measure_once ~name (f : unit -> 'a) : 'a * measurement =
  Gc.compact ();
  let g0 = Gc.stat () in
  let t0 = Unix.gettimeofday () in
  let result = f () in
  let t1 = Unix.gettimeofday () in
  let g1 = Gc.stat () in
  let m = {
    name;
    wall_seconds = t1 -. t0;
    allocated_mb =
      (g1.minor_words +. g1.major_words -. g0.minor_words -. g0.major_words)
      *. 8. /. 1024. /. 1024.;
    heap_peak_mb = float_of_int g1.heap_words *. 8. /. 1024. /. 1024.;
  } in
  result, m

type stats = {
  s_name      : string;
  s_runs      : int;
  s_min       : float;
  s_median    : float;
  s_p95       : float;
  s_max       : float;
  s_mean_alloc_mb : float;
  s_mean_heap_mb  : float;
}

let percentile sorted p =
  let n = Array.length sorted in
  if n = 0 then 0. else
  let idx = int_of_float (p *. float_of_int (n - 1)) in
  sorted.(max 0 (min (n - 1) idx))

let compute_stats name (ms : measurement list) : stats =
  let times = Array.of_list (List.map (fun m -> m.wall_seconds) ms) in
  Array.sort compare times;
  let n = Array.length times in
  let sum_alloc = List.fold_left (fun a m -> a +. m.allocated_mb) 0. ms in
  let sum_heap  = List.fold_left (fun a m -> a +. m.heap_peak_mb) 0. ms in
  {
    s_name   = name;
    s_runs   = n;
    s_min    = times.(0);
    s_median = percentile times 0.50;
    s_p95    = percentile times 0.95;
    s_max    = times.(n - 1);
    s_mean_alloc_mb = sum_alloc /. float_of_int n;
    s_mean_heap_mb  = sum_heap  /. float_of_int n;
  }

(** Прогнать [f] [runs+1] раз: первый прогон — warmup, отбрасывается.
    Возвращает результат последнего прогона + статистику. *)
let run_many ~name ~runs (f : unit -> 'a) : 'a * stats =
  (* warmup *)
  let _, _ = measure_once ~name:(name ^ "/warmup") f in
  let ms = ref [] in
  let last = ref None in
  for _ = 1 to runs do
    let r, m = measure_once ~name f in
    ms := m :: !ms;
    last := Some r
  done;
  let stats = compute_stats name !ms in
  (match !last with Some r -> r, stats | None -> assert false)

(** Печатает строку статистики. Удобный формат с p95 для regression-
    сравнения: «было vs стало» — нужно сравнивать p95 (стабильно),
    а не median (шумит при высоких квантилях). *)
let print_stats (s : stats) : unit =
  Printf.printf "  %-32s  min %.2fс  med %.2fс  p95 %.2fс  max %.2fс  alloc ~%.0fМБ  heap ~%.0fМБ\n"
    s.s_name s.s_min s.s_median s.s_p95 s.s_max s.s_mean_alloc_mb s.s_mean_heap_mb

let throughput_ev_per_sec ~(n_events : int) (s : stats) : float =
  float_of_int n_events /. s.s_median
