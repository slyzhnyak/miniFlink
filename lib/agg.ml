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
