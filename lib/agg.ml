(* Агрегатор с ЭКЗИСТЕНЦИАЛЬНЫМ аккумулятором: тип ('a,'r) t прячет
   внутренний 'acc через GADT, наружу видны только вход 'a и результат 'r.
   Это позволяет комбинировать агрегаты с разными аккумуляторами
   (both: 'acc1 * 'acc2) единообразно. *)
type ('a, 'r) t =
  | Agg : {
      init   : unit -> 'acc;
      add    : 'acc -> 'a -> 'acc;
      finish : 'acc -> 'r;
    } -> ('a, 'r) t

(* ── Готовые агрегаторы ───────────────────────────────────── *)

let count =
  Agg { init = (fun () -> 0); add = (fun n _ -> n + 1); finish = (fun n -> n) }

let count_if pred =
  Agg { init = (fun () -> 0);
        add = (fun n x -> if pred x then n + 1 else n);
        finish = (fun n -> n) }

let sum f =
  Agg { init = (fun () -> 0.); add = (fun s x -> s +. f x); finish = (fun s -> s) }

let mean f =
  Agg { init = (fun () -> (0., 0));
        add = (fun (s, n) x -> (s +. f x, n + 1));
        finish = (fun (s, n) -> if n = 0 then None else Some (s /. float_of_int n)) }

let min_by f =
  Agg { init = (fun () -> None);
        add = (fun acc x ->
          let v = f x in
          match acc with None -> Some v | Some m -> Some (Float.min m v));
        finish = (fun acc -> acc) }

let max_by f =
  Agg { init = (fun () -> None);
        add = (fun acc x ->
          let v = f x in
          match acc with None -> Some v | Some m -> Some (Float.max m v));
        finish = (fun acc -> acc) }

let arg_min f =
  Agg { init = (fun () -> None);
        add = (fun acc x ->
          match acc with
          | None -> Some (x, f x)
          | Some (_, bv) as a -> if f x < bv then Some (x, f x) else a);
        finish = (function None -> None | Some (x, _) -> Some x) }

let arg_max f =
  Agg { init = (fun () -> None);
        add = (fun acc x ->
          match acc with
          | None -> Some (x, f x)
          | Some (_, bv) as a -> if f x > bv then Some (x, f x) else a);
        finish = (function None -> None | Some (x, _) -> Some x) }

let first =
  Agg { init = (fun () -> None);
        add = (fun acc x -> match acc with None -> Some x | s -> s);
        finish = (fun acc -> acc) }

let last =
  Agg { init = (fun () -> None);
        add = (fun _ x -> Some x);
        finish = (fun acc -> acc) }

(* median: середина отсортированных значений (чётное n — среднее двух
   средних). Аккумулятор — список всех значений: median принципиально
   не считается инкрементально, O(n) памяти на окно неизбежен. *)
let median f =
  Agg { init = (fun () -> []);
        add = (fun acc x -> f x :: acc);
        finish = (fun acc ->
          match List.sort compare acc with
          | [] -> None
          | sorted ->
            let n = List.length sorted in
            if n mod 2 = 1 then Some (List.nth sorted (n / 2))
            else Some ((List.nth sorted (n/2 - 1) +. List.nth sorted (n/2)) /. 2.)) }

(** Состояние p² estimator (Jain & Chlamtac 1985, CACM 28:10).

    {b Алгоритм:} держим 5 «маркеров», приближающих min, 25%-, 50%-,
    75%-перцентили и max наблюдённых значений. На каждый новый
    `add` обновляем фактические позиции маркеров (целое n_i) и
    «желаемые» позиции (вещественное np_i). Если фактическая и желаемая
    разошлись на ≥1, двигаем средние три маркера через квадратичную
    интерполяцию по Newton-Cotes (отсюда «p²» — parabolic prediction).

    {b Гарантии точности}: на однородных распределениях ошибка обычно
    <1% от истинной медианы после ~100 событий. На сильно асимметричных
    или с тяжёлыми хвостами — 5-10%. Строгой математической гарантии
    как у Greenwald-Khanna нет; для требований жёсткого p99 точности
    использовать {!median}.

    {b Память:} O(1) — 10 чисел + счётчик независимо от размера окна.
    Это даёт огромную выгоду на больших окнах: вместо хранения N
    значений и сортировки за O(N log N) при каждом закрытии — постоянное
    состояние и O(1) операции. *)
type p2 = {
  mutable n_count : int;        (* всего наблюдений *)
  mutable warmup  : float list; (* для первых 5 наблюдений *)
  q : float array;              (* высоты маркеров (значения) *)
  n : int array;                (* фактические позиции *)
  np : float array;             (* желаемые позиции *)
  dn : float array;             (* increments желаемых позиций *)
}

(* Initial dn для медианы (p=0.5): 0, 0.25, 0.5, 0.75, 1 *)
let p2_make () = {
  n_count = 0;
  warmup  = [];
  q = Array.make 5 0.;
  n = [| 1; 2; 3; 4; 5 |];
  np = [| 1.; 2.; 3.; 4.; 5. |];     (* совпадает с n при инициализации *)
  dn = [| 0.; 0.25; 0.5; 0.75; 1. |];
}

(* Quadratic Newton-Cotes interpolation: предсказывает значение маркера
   i, если бы он был на позиции [n_i + d] (где d = ±1). Если результат
   не лежит между соседями — fallback на линейную. См. формулу (3) в
   статье Jain-Chlamtac 1985. *)
let p2_parabolic st i d =
  let qi = st.q.(i) and qim = st.q.(i-1) and qip = st.q.(i+1) in
  let ni = float st.n.(i) and nim = float st.n.(i-1) and nip = float st.n.(i+1) in
  qi +. (d /. (nip -. nim)) *.
    ((ni -. nim +. d) *. (qip -. qi) /. (nip -. ni) +.
     (nip -. ni -. d) *. (qi -. qim) /. (ni -. nim))

let p2_linear st i d =
  let qi = st.q.(i) in
  let qd = st.q.(i + int_of_float d) in
  let ni = float st.n.(i) and nd = float st.n.(i + int_of_float d) in
  qi +. d *. (qd -. qi) /. (nd -. ni)

let p2_add st (x : float) =
  st.n_count <- st.n_count + 1;
  if st.n_count <= 5 then
    (* warmup: накапливаем первые 5, инициализируем q при n=5 *)
    st.warmup <- x :: st.warmup
  else begin
    if st.n_count = 6 then begin
      (* Завершение warmup на 5-м элементе перед первым «настоящим» add *)
      let sorted = List.sort compare st.warmup |> Array.of_list in
      Array.blit sorted 0 st.q 0 5
    end;
    (* Поиск ячейки [k] куда попадает x. *)
    let k =
      if x < st.q.(0) then begin st.q.(0) <- x; 0 end
      else if x >= st.q.(4) then begin st.q.(4) <- x; 3 end
      else begin
        let rec find i =
          if i = 4 then 3
          else if x < st.q.(i + 1) then i
          else find (i + 1)
        in find 0
      end
    in
    (* Увеличиваем фактические позиции маркеров k+1..4 *)
    for i = k + 1 to 4 do
      st.n.(i) <- st.n.(i) + 1
    done;
    (* Увеличиваем желаемые позиции для всех *)
    for i = 0 to 4 do
      st.np.(i) <- st.np.(i) +. st.dn.(i)
    done;
    (* Корректируем 3 средних маркера (i=1,2,3) если фактическое
       и желаемое разошлись на >=1 *)
    for i = 1 to 3 do
      let d = st.np.(i) -. float st.n.(i) in
      let n_above = st.n.(i+1) - st.n.(i) in
      let n_below = st.n.(i) - st.n.(i-1) in
      if (d >= 1. && n_above > 1) || (d <= -1. && n_below > 1) then begin
        let d = if d >= 0. then 1. else -1. in
        let parab = p2_parabolic st i d in
        let qi = st.q.(i) in
        let qim = st.q.(i-1) and qip = st.q.(i+1) in
        let new_q =
          if qim < parab && parab < qip then parab
          else p2_linear st i d
        in
        ignore qi;
        st.q.(i) <- new_q;
        st.n.(i) <- st.n.(i) + int_of_float d
      end
    done
  end

let p2_get st : float option =
  match st.n_count with
  | 0 -> None
  | n when n <= 5 ->
    (* warmup: точная медиана из накопленных *)
    let sorted = List.sort compare st.warmup in
    let m = List.length sorted in
    if m mod 2 = 1 then Some (List.nth sorted (m / 2))
    else Some ((List.nth sorted (m/2 - 1) +. List.nth sorted (m/2)) /. 2.)
  | _ -> Some st.q.(2)   (* центральный маркер = медиана *)

(** Приближённая медиана через p² estimator (O(1) состояние). См. {!p2}
    выше — там полное обсуждение точности и trade-off.

    Используйте когда:
    - окна большие (>1000 элементов) и {!median} становится узким;
    - точность ±5% медианы приемлема для use case (например, мониторинг
      RSSI: 5% от -50dBm = ±2.5dBm — обычно в пределах шума канала).

    НЕ используйте когда:
    - окна маленькие (<50): нет существенного выигрыша;
    - нужна жёсткая гарантия точности (например, для определения медианной
      зарплаты или медианного времени отклика SLA).

    Алгоритм: Jain R, Chlamtac I (1985), "The p² algorithm for dynamic
    calculation of quantiles and histograms without storing observations",
    CACM 28(10). *)
let approx_median f =
  Agg { init = p2_make;
        add = (fun st x -> p2_add st (f x); st);
        finish = p2_get }

(* group_by: двухуровневая агрегация. Внутри одного окна группируем
   значения по подключу [key] и применяем [inner] к каждой группе;
   результат — список (подключ, inner_result). Память O(числа подключей)
   на окно, потому что нужно держать аккумулятор inner для каждой группы. *)
let group_by (type a) (type r)
    ~(key : a -> string)
    ~(inner : (a, r) t) : (a, (string * r) list) t =
  let Agg { init = inner_init; add = inner_add; finish = inner_finish } = inner in
  Agg {
    init = (fun () -> Hashtbl.create 8);
    add = (fun tbl x ->
      let k = key x in
      let acc = match Hashtbl.find_opt tbl k with
        | Some a -> a | None -> inner_init () in
      Hashtbl.replace tbl k (inner_add acc x);
      tbl);
    finish = (fun tbl ->
      Hashtbl.fold (fun k acc rest -> (k, inner_finish acc) :: rest) tbl []);
  }

(* top_k_by: K элементов с наибольшим [by]; сортировка по убыванию.
   Аккумулятор — отсортированный список пар (x, by_x) длины ≤ K;
   вставка O(K). Память O(K) на окно. *)
let top_k_by k ~by =
  if k <= 0 then invalid_arg "top_k_by: k должен быть > 0";
  let insert x bx acc =
    let rec go = function
      | [] -> [(x, bx)]
      | (_, by_y) :: _ as rest when bx > by_y -> (x, bx) :: rest
      | hd :: tl -> hd :: go tl in
    go acc in
  let take_k lst =
    let rec go n = function
      | [] -> [] | _ when n = 0 -> []
      | hd :: tl -> hd :: go (n-1) tl in
    go k lst in
  Agg {
    init = (fun () -> []);
    add = (fun acc x -> take_k (insert x (by x) acc));
    finish = (fun acc -> List.map fst acc);
  }

(* bottom_k_by: K элементов с НАИМЕНЬШИМ [by]; сортировка по возрастанию. *)
let bottom_k_by k ~by =
  if k <= 0 then invalid_arg "bottom_k_by: k должен быть > 0";
  let insert x bx acc =
    let rec go = function
      | [] -> [(x, bx)]
      | (_, by_y) :: _ as rest when bx < by_y -> (x, bx) :: rest
      | hd :: tl -> hd :: go tl in
    go acc in
  let take_k lst =
    let rec go n = function
      | [] -> [] | _ when n = 0 -> []
      | hd :: tl -> hd :: go (n-1) tl in
    go k lst in
  Agg {
    init = (fun () -> []);
    add = (fun acc x -> take_k (insert x (by x) acc));
    finish = (fun acc -> List.map fst acc);
  }

let to_list =
  Agg { init = (fun () -> []);
        add = (fun acc x -> x :: acc);
        finish = (fun acc -> List.rev acc) }

(* ── Комбинирование ───────────────────────────────────────── *)

let both (Agg a) (Agg b) =
  Agg {
    init = (fun () -> (a.init (), b.init ()));
    add = (fun (sa, sb) x -> (a.add sa x, b.add sb x));
    finish = (fun (sa, sb) -> (a.finish sa, b.finish sb));
  }

let map g (Agg a) =
  Agg { init = a.init; add = a.add; finish = (fun s -> g (a.finish s)) }

let contramap g (Agg a) =
  Agg { init = a.init; add = (fun s x -> a.add s (g x)); finish = a.finish }

let ( let+ ) agg g = map g agg
let ( and+ ) a b = both a b

(* ── Применение ───────────────────────────────────────────── *)

let run (Agg a) xs =
  a.finish (List.fold_left a.add (a.init ()) xs)

(* Применить агрегатор где нужен доступ к скрытому 'acc. Продолжение [k]
   ДОЛЖНО быть полиморфно по 'acc (rank-2) — иначе тип аккумулятора
   «убежал» бы из scope. Выражаем это record-ом с универсальным полем. *)
type ('a, 'r, 'b) parts_k =
  { k : 'acc. (unit -> 'acc) -> ('acc -> 'a -> 'acc) -> ('acc -> 'r) -> 'b }

let with_parts (Agg a) { k } = k a.init a.add a.finish
