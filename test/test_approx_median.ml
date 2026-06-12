(** Тест корректности [Agg.approx_median] — p² estimator.

    Не unit-тест в общепринятом смысле, а инвариант-тест: прогоняем
    одни и те же данные через [Agg.median] (эталон) и [Agg.approx_median]
    (приближение), сравниваем результат. Для малых n требуем
    байт-в-байт совпадение (там warmup-fallback). Для больших — ошибка
    ≤5% от true median на однородных данных. *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let approx_eq ?(eps = 1e-9) a b = abs_float (a -. b) <= eps

(* Прогнать оба агрегата на списке, вернуть (exact, approx) *)
let run_both xs : float option * float option =
  let exact  = Agg.run (Agg.median       (fun x -> x)) xs in
  let approx = Agg.run (Agg.approx_median (fun x -> x)) xs in
  (exact, approx)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Agg.approx_median (p² estimator)\n";
  Printf.printf "==========================================\n";

  (* ── n=0..5: warmup, должны совпадать точно ─────────────── *)

  Printf.printf "\n-- warmup: n ≤ 5, ожидаем точное совпадение --\n";

  check "empty list → None"
    (run_both [] = (None, None));

  let e, a = run_both [42.0] in
  check "n=1 → exact 42.0"
    (e = Some 42.0 && a = Some 42.0);

  let e, a = run_both [3.0; 1.0; 2.0] in
  check "n=3 → exact median"
    (e = Some 2.0 && a = Some 2.0);

  let e, a = run_both [4.0; 1.0; 3.0; 2.0] in
  check "n=4 → exact median (среднее двух средних)"
    (e = Some 2.5 && a = Some 2.5);

  let e, a = run_both [5.0; 1.0; 3.0; 2.0; 4.0] in
  check "n=5 → exact median"
    (e = Some 3.0 && a = Some 3.0);

  (* ── n=6+: p² engaged, разница допускается ───────────────── *)

  Printf.printf "\n-- p² engaged: n ≥ 6 --\n";

  (* Монотонная последовательность 1..N: median = (N+1)/2 *)
  let monotonic n =
    let xs = List.init n (fun i -> float (i + 1)) in
    run_both xs in
  let true_median n = float (n + 1) /. 2. in

  let pct_err exact approx =
    match exact, approx with
    | Some e, Some a -> abs_float (a -. e) /. abs_float e *. 100.
    | _ -> infinity in

  let test_monotonic n_target ~max_err =
    let e, a = monotonic n_target in
    let err = pct_err e a in
    let _ = true_median n_target in
    check (Printf.sprintf "n=%d монотонно: exact=%.1f approx=%.1f (err %.2f%%) ≤ %.0f%%"
             n_target (match e with Some v -> v | _ -> 0.)
             (match a with Some v -> v | _ -> 0.) err max_err)
      (err <= max_err)
  in
  (* На малых n у p² мало статистики (5 маркеров на 10 точек = 2 точки
     на маркер). По статье Jain-Chlamtac, <5% точность начинается с
     n~100. Распускаем допуск для малых n. *)
  test_monotonic 10    ~max_err:15.;
  test_monotonic 50    ~max_err:8.;
  test_monotonic 100   ~max_err:5.;
  test_monotonic 1000  ~max_err:2.;
  test_monotonic 10000 ~max_err:1.;

  (* Все одинаковые: median = это значение, точно *)
  Printf.printf "\n-- константа: все одинаковые значения --\n";
  let e, a = run_both (List.init 100 (fun _ -> 7.5)) in
  check "100 одинаковых: exact = approx = 7.5"
    (e = Some 7.5 && approx_eq (match a with Some v -> v | _ -> nan) 7.5);

  (* Случайные uniform — статистическая проверка *)
  Printf.printf "\n-- случайные uniform [0, 1000] --\n";
  Random.self_init ();
  let n = 5000 in
  let xs = List.init n (fun _ -> Random.float 1000.) in
  let e, a = run_both xs in
  let err = pct_err e a in
  check (Printf.sprintf "5K uniform: exact≈%.1f approx≈%.1f (err %.2f%%) ≤ 3%%"
           (match e with Some v -> v | _ -> 0.)
           (match a with Some v -> v | _ -> 0.) err)
    (err <= 3.0);

  (* Bimodal — два кластера, медиана между ними. Тест на правдоподобие. *)
  Printf.printf "\n-- bimodal: половина в [0, 10], половина в [100, 110] --\n";
  let bimodal =
    List.init 500 (fun _ -> Random.float 10.) @
    List.init 500 (fun _ -> 100. +. Random.float 10.) in
  let e, a = run_both bimodal in
  let err = pct_err e a in
  check (Printf.sprintf "bimodal: exact≈%.1f approx≈%.1f (err %.2f%%) ≤ 30%%"
           (match e with Some v -> v | _ -> 0.)
           (match a with Some v -> v | _ -> 0.) err)
    (err <= 30.0);  (* Bimodal — самый сложный случай для p², допуск шире *)

  (* Skewed (экспоненциальное): хвост вправо *)
  Printf.printf "\n-- skewed: экспоненциальное распределение --\n";
  let skewed = List.init 2000 (fun _ ->
    -. log (1. -. Random.float 0.999) *. 10.) in  (* mean ~10 *)
  let e, a = run_both skewed in
  let err = pct_err e a in
  check (Printf.sprintf "skewed: exact≈%.1f approx≈%.1f (err %.2f%%) ≤ 10%%"
           (match e with Some v -> v | _ -> 0.)
           (match a with Some v -> v | _ -> 0.) err)
    (err <= 10.0);

  (* ── Real-world сценарий ex07: RSSI значения с шумом ───── *)
  Printf.printf "\n-- realistic: RSSI -50dBm ± 4dB шума, 200 точек --\n";
  let rssi_with_noise =
    List.init 200 (fun i -> -. 50. +. float ((i * 7 + 13) mod 9 - 4)) in
  let e, a = run_both rssi_with_noise in
  let err =
    match e, a with
    | Some e_v, Some a_v -> abs_float (a_v -. e_v)
    | _ -> infinity in
  check (Printf.sprintf "RSSI: exact=%.2f approx=%.2f (abs err %.2f dB) ≤ 1.0 dB"
           (match e with Some v -> v | _ -> 0.)
           (match a with Some v -> v | _ -> 0.) err)
    (err <= 1.0);

  (* ── Sanity: O(1) состояние (косвенно: 10K точек проходят быстро) ── *)
  Printf.printf "\n-- O(1) state: 100K точек должны пройти мгновенно --\n";
  let n_big = 100_000 in
  let big = List.init n_big (fun i -> float (i mod 1000)) in
  let t0 = Unix.gettimeofday () in
  let _ = Agg.run (Agg.approx_median (fun x -> x)) big in
  let dt = Unix.gettimeofday () -. t0 in
  check (Printf.sprintf "100K точек за %.3fс (≤ 0.5с — O(1) per add)" dt)
    (dt <= 0.5);

  Printf.printf "\nAll tests passed.\n"
