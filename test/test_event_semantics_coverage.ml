(** Покрытие семантики Update/Retract на базовых операторах.

    Мотивация: много тестов работают только на Data-событиях, поэтому
    asymmetric Update (где predicate/transform по-разному реагирует на
    old vs new) и Retract не покрыты. Здесь проверяем КАЖДЫЙ базовый
    оператор на согласованность Update/Retract обработки.

    Ключевой инвариант atomic Update: если downstream получает
    Update{old, new}, он ДОЛЖЕН был ранее видеть old (как Data или как
    new предыдущего Update). Иначе "коррекция" ссылается на значение,
    которого downstream не знает — рассогласование состояния. *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let collect s =
  let acc = ref [] in
  let rec go () = match s () with
    | None -> () | Some e -> acc := e :: !acc; go () in
  go (); List.rev !acc

(* классификация выходного потока по типам событий *)
let classify evs =
  List.fold_left (fun (d, u, r, w) e -> match e with
    | Mf_event.Data _ -> (d+1, u, r, w)
    | Mf_event.Update _ -> (d, u+1, r, w)
    | Mf_event.Retract _ -> (d, u, r+1, w)
    | Mf_event.Watermark _ -> (d, u, r, w+1)) (0,0,0,0) evs

let () =
  Printf.printf "Event-semantics coverage: filter / map / flatmap\n%!";

  (* ════════════════════════════════════════════════════════════
     1. FILTER — asymmetric Update (предикат по-разному на old/new)
     ════════════════════════════════════════════════════════════ *)
  Printf.printf "\n-- 1. filter: asymmetric Update (p old <> p new)\n";

  (* Предикат: пропускать v > 50.
     Update{old=10, new=90}: p(old)=false, p(new)=true.
     old=10 НИКОГДА не проходил фильтр — downstream его не видел.
     Что должен сделать filter с таким Update? *)
  let events = [
    Mf_event.data 10 0;        (* p(10)=false → отфильтрован *)
    Mf_event.update 10 90 100; (* p(old)=false, p(new)=true *)
    Mf_event.wm 1000;
  ] in
  let out = events |> Stream.of_list
    |> Pipe.filter (fun v -> v > 50)
    |> collect in
  let (d, u, r, _) = classify out in
  Printf.printf "  output: Data=%d Update=%d Retract=%d\n" d u r;
  List.iter (function
    | Mf_event.Data (v,_) -> Printf.printf "    Data %d\n" v
    | Mf_event.Update {old;new_value;_} -> Printf.printf "    Update %d→%d\n" old new_value
    | Mf_event.Retract (v,_) -> Printf.printf "    Retract %d\n" v
    | Mf_event.Watermark _ -> ()) out;
  (* Корректно: Update где p(old)=false, p(new)=true становится
     Data(new) — для downstream это появление нового значения, а не
     коррекция несуществующего. *)
  check "asymmetric Update (F,T) → Data(90), not Update"
    (d = 1 && u = 0 && r = 0
     && List.exists (function Mf_event.Data (90,_) -> true | _ -> false) out);

  (* ════════════════════════════════════════════════════════════
     2. FILTER — asymmetric Update (p old=true, p new=false)
     ════════════════════════════════════════════════════════════ *)
  Printf.printf "\n-- 2. filter: Update p(old)=true p(new)=false\n";
  let events = [
    Mf_event.data 90 0;        (* p(90)=true → проходит, downstream видит 90 *)
    Mf_event.update 90 10 100; (* p(old)=true, p(new)=false *)
    Mf_event.wm 1000;
  ] in
  let out = events |> Stream.of_list
    |> Pipe.filter (fun v -> v > 50)
    |> collect in
  let (d, u, r, _) = classify out in
  Printf.printf "  output: Data=%d Update=%d Retract=%d\n" d u r;
  (* downstream видел 90 (прошло). Теперь 90→10, но 10 не проходит
     фильтр. Для downstream это ИСЧЕЗНОВЕНИЕ 90 → Retract(90). *)
  check "Update (T,F) → Retract(90)"
    (r = 1 && u = 0
     && List.exists (function Mf_event.Retract (90,_) -> true | _ -> false) out);

  (* ════════════════════════════════════════════════════════════
     3. FILTER — Retract проходит независимо от предиката
     ════════════════════════════════════════════════════════════ *)
  Printf.printf "\n-- 3. filter: Retract passthrough\n";
  let events = [
    Mf_event.data 90 0;
    Mf_event.retract 90 100;
    Mf_event.data 30 200;      (* p(30)=false *)
    Mf_event.retract 30 300;   (* retract отфильтрованного значения *)
    Mf_event.wm 1000;
  ] in
  let out = events |> Stream.of_list
    |> Pipe.filter (fun v -> v > 50)
    |> collect in
  let (d, _, r, _) = classify out in
  Printf.printf "  output: Data=%d Retract=%d\n" d r;
  (* Retract(90) уместен (90 проходило). Retract(30) подавлен — 30
     никогда не проходило фильтр, downstream его не видел. *)
  check "only Retract(90) passes; Retract(30) suppressed"
    (r = 1 && d = 1
     && List.exists (function Mf_event.Retract (90,_) -> true | _ -> false) out
     && not (List.exists (function Mf_event.Retract (30,_) -> true | _ -> false) out));

  (* ════════════════════════════════════════════════════════════
     4. MAP — Update трансформирует обе стороны согласованно
     ════════════════════════════════════════════════════════════ *)
  Printf.printf "\n-- 4. map: Update transforms both old and new\n";
  let events = [
    Mf_event.update 5 10 100;
    Mf_event.wm 1000;
  ] in
  let out = events |> Stream.of_list
    |> Pipe.map (fun v -> v * 2)
    |> collect in
  (match out with
   | [Mf_event.Update {old; new_value; _}; Mf_event.Watermark _] ->
     check "map transforms both: Update{5→10} → Update{10→20}"
       (old = 10 && new_value = 20)
   | _ -> fail "map broke Update structure");

  (* ════════════════════════════════════════════════════════════
     5. FLATMAP — Update на N результатов синхронно
     ════════════════════════════════════════════════════════════ *)
  Printf.printf "\n-- 5. flat_map: Update → synchronized N Updates\n";
  let events = [
    Mf_event.update 2 3 100;   (* f(2)=[2;4], f(3)=[3;6] → zip *)
    Mf_event.wm 1000;
  ] in
  let out = events |> Stream.of_list
    |> Pipe.flat_map (fun v -> [v; v * 2])
    |> collect in
  let updates = List.filter_map (function
    | Mf_event.Update {old; new_value; _} -> Some (old, new_value)
    | _ -> None) out in
  Printf.printf "  updates: %s\n"
    (String.concat " " (List.map (fun (o,n) -> Printf.sprintf "%d→%d" o n) updates));
  check "flat_map zips: [2→3; 4→6]" (updates = [(2,3); (4,6)]);

  (* ════════════════════════════════════════════════════════════
     6. WINDOW_AGG — noop-коррекция подавляется (late data, тот же результат)
     ════════════════════════════════════════════════════════════ *)
  Printf.printf "\n-- 6. window_agg: noop late correction is suppressed\n";
  let module WK = struct type t = int let key _ = "k" end in
  (* sum: late Data 0 не меняет сумму → не должно быть Update{100→100} *)
  let events = [
    Mf_event.data 100 0;
    Mf_event.wm 2000;          (* закрывает окно, emit Data sum=100 *)
    Mf_event.data 0 500;       (* late, sum 100+0=100, результат тот же *)
    Mf_event.wm 3000;
  ] in
  let out = events |> Stream.of_list
    |> Pipe.window_agg (module WK) ~allowed_lateness:5000
         (Pipe.tumbling 1000) Agg.(sum (fun x -> float_of_int x))
    |> collect in
  let (d, u, _, _) = classify out in
  Printf.printf "  output: Data=%d Update=%d\n" d u;
  check "sum+0 late data: no noop Update (just initial Data)"
    (d = 1 && u = 0);

  (* median: late значение между существующими → median тот же *)
  let module WK2 = struct type t = float let key _ = "k" end in
  let events = [
    Mf_event.data 10.0 0;
    Mf_event.data 20.0 100;
    Mf_event.wm 6000;          (* median(10,20)=15 *)
    Mf_event.data 15.0 500;    (* late, median(10,15,20)=15 — тот же! *)
    Mf_event.wm 7000;
  ] in
  let out = events |> Stream.of_list
    |> Pipe.window_agg (module WK2) ~allowed_lateness:10000
         (Pipe.tumbling 5000) Agg.(median (fun x -> x))
    |> collect in
  let (_, u, _, _) = classify out in
  Printf.printf "  median late same-value: Update=%d\n" u;
  check "median unchanged by late value: no noop Update" (u = 0);

  (* контроль: late data которое РЕАЛЬНО меняет результат → Update есть *)
  let events = [
    Mf_event.data 10.0 0;
    Mf_event.data 20.0 100;
    Mf_event.wm 6000;          (* median=15 *)
    Mf_event.data 100.0 500;   (* late, median(10,20,100)=20 — изменился! *)
    Mf_event.wm 7000;
  ] in
  let out = events |> Stream.of_list
    |> Pipe.window_agg (module WK2) ~allowed_lateness:10000
         (Pipe.tumbling 5000) Agg.(median (fun x -> x))
    |> collect in
  let real_updates = List.filter_map (function
    | Mf_event.Update { old = (_, o); new_value = (_, n); _ } -> Some (o, n)
    | _ -> None) out in
  Printf.printf "  median changed by late value: %d real update(s)\n"
    (List.length real_updates);
  check "median changed by late value: emits real Update (15→20)"
    (real_updates = [(Some 15.0, Some 20.0)]);

  Printf.printf "\nDone.\n"
