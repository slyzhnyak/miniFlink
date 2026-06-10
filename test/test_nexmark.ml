open Miniflink
open Time
(* ════════════════════════════════════════════════════════════
   NEXMark — индустриальный корректностный набор для стриминга.

   NEXMark моделирует онлайн-аукцион (Person / Auction / Bid) и
   определяет набор запросов с ИЗВЕСТНОЙ семантикой. Это де-факто
   стандарт (Flink, Beam, Hazelcast Jet, RisingWave, Feldera).

   Здесь — применимое к miniFlink подмножество, реализованное на нашем
   API, с проверкой результата против эталона. Цель: сверить движок с
   ВНЕШНЕ определённой семантикой, а не только с самодельными
   инвариантами. Hazelcast Jet использует ровно q1/q2/q5/q8 как
   репрезентативный набор — мы покрываем их и ещё несколько.

   Версии NEXMark (чтобы не путать число запросов): оригинал 2002 — мелкий
   SQL-draft; канон Beam/Flink — q0–q13 (~14), с ним сверяемся; расширенная
   Ververica/Feldera — q0–q22 (~23), q14–q22 в основном SQL вне нашей области.

   Что НЕ реализовано и почему — см. TODO в README (раздел NEXMark):
   q4/q6 (retraction-over-window),
   q9–q13 (SQL-специфика, filesystem-коннекторы, UDF).
   ════════════════════════════════════════════════════════════ *)

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

(* ── Модель аукциона ──────────────────────────────────────── *)

type person  = { p_id : int; p_name : string; p_state : string; p_ts : Time.t }
type auction = { a_id : int; a_seller : int; a_category : int; a_ts : Time.t }
type bid     = { b_auction : int; b_bidder : int; b_price : int; b_ts : Time.t }

(* ── Q1 (CURRENCY_CONVERSION): map цены bid $ → € ──────────── *)
(* «What are the bid values in Euros?» — простой map. *)
let test_q1 () =
  Printf.printf "\n-- Q1 CURRENCY_CONVERSION (map): bid price USD -> EUR\n";
  let rate = 0.85 in
  let bids = [
    { b_auction=1; b_bidder=10; b_price=100; b_ts=seconds 1 };
    { b_auction=2; b_bidder=11; b_price=200; b_ts=seconds 2 };
  ] in
  let out =
    Mf_event.of_list ~ts:(fun b -> b.b_ts) bids
    |> Pipe.map (fun b -> { b with b_price = int_of_float (float_of_int b.b_price *. rate) })
    |> Stream.to_list
    |> List.filter_map (function Mf_event.Data (b,_) -> Some b.b_price | _ -> None) in
  check "prices converted to EUR" (out = [85; 170])

(* ── Q2 (SELECTION): filter bid'ов по auction id ──────────── *)
(* «Auctions with particular auction numbers» — простой filter. *)
let test_q2 () =
  Printf.printf "\n-- Q2 SELECTION (filter): bids for specific auctions\n";
  let wanted = [1007; 1020] in
  let bids = List.init 30 (fun i ->
    { b_auction=1000+i; b_bidder=i; b_price=i*10; b_ts=seconds i }) in
  let out =
    Mf_event.of_list ~ts:(fun b -> b.b_ts) bids
    |> Pipe.filter (fun b -> List.mem b.b_auction wanted)
    |> Stream.to_list
    |> List.filter_map (function Mf_event.Data (b,_) -> Some b.b_auction | _ -> None)
    |> List.sort compare in
  check "only wanted auctions pass" (out = [1007; 1020])

(* ── Q3 (LOCAL_ITEM_SUGGESTION): incremental join person⋈auction ─ *)
(* «Кто продаёт в определённых штатах?» Incremental join по seller id
   через per-key state + timer. Семантика Beam: auction может прийти
   РАНЬШЕ person продавца — буферизуем такие auction'ы, пока не появится
   person; затем все аукционы этого продавца используют сохранённого
   person. person-state истекает по таймеру (не висит вечно если продавец
   так и не пришёл). Фильтр: только person из заданных штатов. *)

type pa3 = P3 of person | A3 of auction
module BySeller = Keyed.Make (struct
  type t = pa3
  let key = function P3 p -> string_of_int p.p_id | A3 a -> string_of_int a.a_seller
end)

let test_q3 () =
  Printf.printf "\n-- Q3 LOCAL_ITEM_SUGGESTION: incremental join (state + timer)\n";
  let wanted_states = ["OR"; "ID"; "CA"] in
  let state_ttl = 100 in   (* person-state истекает через 100 после установки *)
  (* person 1 (OR) приходит ПОСЛЕ своего первого аукциона — проверяем
     буферизацию. person 2 (NY) не в списке штатов — фильтр отсечёт.
     person 3 (CA) приходит, потом его аукцион. *)
  let events = [
    A3 { a_id=100; a_seller=1; a_category=1; a_ts=seconds 1 };  (* до person 1 — буфер *)
    P3 { p_id=2; p_name="bob";   p_state="NY"; p_ts=seconds 2 };
    A3 { a_id=200; a_seller=2; a_category=1; a_ts=seconds 3 };  (* NY → фильтр отсечёт *)
    P3 { p_id=1; p_name="alice"; p_state="OR"; p_ts=seconds 4 };(* теперь матчим буфер *)
    P3 { p_id=3; p_name="carol"; p_state="CA"; p_ts=seconds 5 };
    A3 { a_id=300; a_seller=3; a_category=2; a_ts=seconds 6 };  (* person уже есть *)
    A3 { a_id=101; a_seller=1; a_category=1; a_ts=seconds 7 };  (* ещё один у alice *)
  ] in
  (* состояние продавца: сохранённый person + буфер ранних аукционов *)
  let matches = ref [] in
  let _ =
    Mf_event.of_list ~ts:(fun e -> match e with P3 p -> p.p_ts | A3 a -> a.a_ts) events
    |> Pipe.process_keyed (module BySeller)
         ~init:(fun () -> (ref None, ref []))   (* (person option, pending auctions) *)
         ~on_event:(fun ctx _key (pers, pending) e ->
           match e with
           | P3 p ->
             if List.mem p.p_state wanted_states then begin
               pers := Some p;
               ctx.Pipe.set_event_timer (p.p_ts + state_ttl);  (* истечение person-state *)
               (* матчим накопленные аукционы *)
               List.iter (fun a -> ctx.Pipe.emit (p.p_name, a.a_id)) (List.rev !pending);
               pending := []
             end
           | A3 a ->
             (match !pers with
              | Some p -> ctx.Pipe.emit (p.p_name, a.a_id)   (* person известен *)
              | None -> pending := a :: !pending))           (* буферизуем до person *)
         ~on_timer:(fun _ctx _key (pers, pending) _t _kind ->
           (* person-state истёк — очищаем (последующие аукционы снова буфер) *)
           pers := None; pending := [])
    |> Stream.to_list
    |> List.iter (function
       | Mf_event.Data ((name, aid), _) -> matches := (name, aid) :: !matches
       | _ -> ()) in
  let got = List.sort compare !matches in
  (* alice (OR): аукционы 100 (буфер) и 101 (после) → (alice,100),(alice,101)
     carol (CA): аукцион 300 → (carol,300)
     bob (NY): отфильтрован *)
  check "Q3 matches: alice's 100+101, carol's 300, bob(NY) filtered out"
    (got = [("alice",100); ("alice",101); ("carol",300)])

(* ── Q5: какие аукционы собрали больше всего ставок за период ─ *)
(* Sliding window + агрегация: за каждое окно — аукцион(ы) с
   максимальным числом ставок. Здесь tumbling для детерминизма проверки.
   Считаем число bid'ов по аукциону в окне, затем берём максимум. *)
module BidByAuction = Keyed.Make (struct
  type t = bid let key b = string_of_int b.b_auction
end)

let test_q5 () =
  Printf.printf "\n-- Q5: auction(s) with most bids in the window\n";
  (* окно [0,10): аукцион 1 — 3 ставки, аукцион 2 — 1 ставка → max=1 (3 ставки) *)
  let bids = [
    { b_auction=1; b_bidder=1; b_price=10; b_ts=seconds 1 };
    { b_auction=1; b_bidder=2; b_price=11; b_ts=seconds 2 };
    { b_auction=2; b_bidder=3; b_price=12; b_ts=seconds 3 };
    { b_auction=1; b_bidder=4; b_price=13; b_ts=seconds 4 };
    { b_auction=2; b_bidder=5; b_price=14; b_ts=seconds 12 }; (* следующее окно *)
    { b_auction=2; b_bidder=6; b_price=15; b_ts=seconds 13 };
  ] in
  (* считаем число ставок по аукциону в окне *)
  let counts =
    Mf_event.of_list ~ts:(fun b -> b.b_ts) bids
    |> Mf_event.with_watermarks ~latency:0
    |> Pipe.window_agg (module BidByAuction) (Pipe.tumbling (seconds 10)) Agg.count
    |> Stream.to_list
    |> List.filter_map (function
       | Mf_event.Data ((auc, n), wend) -> Some (wend/1000, int_of_string auc, n)
       | _ -> None) in
  (* окно [..10]: (1→3),(2→1); окно [..20]: (2→2) *)
  let w1 = List.filter (fun (w,_,_) -> w = 10) counts in
  let max_w1 = List.fold_left (fun m (_,_,n) -> max m n) 0 w1 in
  check "window [0,10): max bid count = 3 (auction 1)" (max_w1 = 3);
  let w2 = List.filter (fun (w,_,_) -> w = 20) counts in
  let max_w2 = List.fold_left (fun m (_,_,n) -> max m n) 0 w2 in
  check "window [10,20): max bid count = 2 (auction 2)" (max_w2 = 2)

(* ── Q7: за каждый период — bid с максимальной ценой ──────── *)
(* «Highest bid in the most recent period.» tumbling-окно + max-by-price.
   Используем arg_max чтобы вернуть сам bid. *)
module AllBids = Keyed.Make (struct
  type t = bid let key _ = "all"   (* один ключ: глобальный максимум за период *)
end)

let test_q7 () =
  Printf.printf "\n-- Q7: highest-price bid per period\n";
  let bids = [
    { b_auction=1; b_bidder=1; b_price=50;  b_ts=seconds 1 };
    { b_auction=2; b_bidder=2; b_price=90;  b_ts=seconds 3 };  (* макс в [0,10) *)
    { b_auction=3; b_bidder=3; b_price=70;  b_ts=seconds 5 };
    { b_auction=4; b_bidder=4; b_price=200; b_ts=seconds 12 }; (* макс в [10,20) *)
    { b_auction=5; b_bidder=5; b_price=150; b_ts=seconds 14 };
  ] in
  let out =
    Mf_event.of_list ~ts:(fun b -> b.b_ts) bids
    |> Mf_event.with_watermarks ~latency:0
    |> Pipe.window_agg (module AllBids) (Pipe.tumbling (seconds 10))
         (Agg.arg_max (fun b -> float_of_int b.b_price))
    |> Stream.to_list
    |> List.filter_map (function
       | Mf_event.Data ((_, Some b), wend) -> Some (wend/1000, b.b_price)
       | _ -> None)
    |> List.sort compare in
  check "Q7: max price per period [10->90; 20->200]" (out = [(10, 90); (20, 200)])

(* ── Q8 (WINDOWED_JOIN): persons ⋈ auctions за период ─────── *)
(* «Users that created an auction in the last period.» Join потока новых
   персон с потоком аукционов по seller=person в общем окне. Реализуем
   через union + оконную группировку по person id: в окне, где есть и
   person, и его auction — выводим. *)
type pa = Per of person | Auc of auction
module PaByPerson = Keyed.Make (struct
  type t = pa
  let key = function Per p -> string_of_int p.p_id | Auc a -> string_of_int a.a_seller
end)

let test_q8 () =
  Printf.printf "\n-- Q8 WINDOWED_JOIN: new persons who created an auction in period\n";
  let persons = [
    { p_id=1; p_name="alice"; p_state="OR"; p_ts=seconds 1 };
    { p_id=2; p_name="bob";   p_state="CA"; p_ts=seconds 2 };
    { p_id=3; p_name="carol"; p_state="WA"; p_ts=seconds 3 };
  ] in
  let auctions = [
    { a_id=100; a_seller=1; a_category=1; a_ts=seconds 4 };  (* alice создала аукцион в [0,10) *)
    { a_id=101; a_seller=3; a_category=2; a_ts=seconds 5 };  (* carol тоже *)
    (* bob (id=2) аукцион НЕ создавал → не в результате *)
  ] in
  let ps = Mf_event.of_list ~ts:(fun p -> p.p_ts) persons |> fun s ->
    (fun () -> match s () with
       | Some (Mf_event.Data (p,t)) -> Some (Mf_event.data (Per p) t)
       | Some (Mf_event.Watermark w) -> Some (Mf_event.wm w)
       | Some (Mf_event.Retract (p,t)) -> Some (Mf_event.retract (Per p) t)
       | None -> None) in
  let aus = Mf_event.of_list ~ts:(fun a -> a.a_ts) auctions |> fun s ->
    (fun () -> match s () with
       | Some (Mf_event.Data (a,t)) -> Some (Mf_event.data (Auc a) t)
       | Some (Mf_event.Watermark w) -> Some (Mf_event.wm w)
       | Some (Mf_event.Retract (a,t)) -> Some (Mf_event.retract (Auc a) t)
       | None -> None) in
  (* окно по person id: выводим person'ов, у кого в окне есть и Per, и Auc *)
  let joined =
    Mf_event.union ps aus
    |> Mf_event.with_watermarks ~latency:0
    |> Pipe.window (module PaByPerson) (Pipe.tumbling (seconds 10))
    |> Stream.to_list
    |> List.filter_map (function
       | Mf_event.Data ((_, items), _) ->
         let has_per = List.exists (function Per _ -> true | _ -> false) items in
         let has_auc = List.exists (function Auc _ -> true | _ -> false) items in
         if has_per && has_auc then
           (* имя персоны *)
           List.find_map (function Per p -> Some p.p_name | _ -> None) items
         else None
       | _ -> None)
    |> List.sort compare in
  check "Q8: persons who created an auction = [alice, carol]"
    (joined = ["alice"; "carol"])

(* ── Q12: число ставок пользователя в global window по триггеру ─ *)
(* «How many bids does a user make within a fixed limit.» Global window +
   trigger_count: эмиссия каждые N ставок. Processing-time во Flink;
   у нас — по числу событий (count-trigger), что и есть суть Q12. *)
module BidByBidder = Keyed.Make (struct
  type t = bid let key b = string_of_int b.b_bidder
end)

let test_q12 () =
  Printf.printf "\n-- Q12: bids per user, global window fired every N bids\n";
  (* пользователь 1 делает 5 ставок; триггер каждые 3 → эмиссии на 3-й *)
  let bids = List.init 5 (fun i ->
    { b_auction=i; b_bidder=1; b_price=i*10; b_ts=seconds i }) in
  let fires =
    Mf_event.of_list ~ts:(fun b -> b.b_ts) bids
    |> Mf_event.with_watermarks ~latency:0
    |> Pipe.global_window (module BidByBidder) ~trigger:(Pipe.trigger_count 3)
    |> Stream.to_list
    |> List.filter_map (function
       | Mf_event.Data ((bidder, items), _) -> Some (bidder, List.length items)
       | _ -> None) in
  (* триггер на 3-й ставке: эмиссия с 3 накопленными (accumulating) *)
  check "Q12: fired at least once when reaching 3 bids"
    (List.exists (fun (_, n) -> n >= 3) fires)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  NEXMark correctness suite (subset)\n";
  Printf.printf "==========================================\n";
  test_q1 ();
  test_q2 ();
  test_q3 ();
  test_q5 ();
  test_q7 ();
  test_q8 ();
  test_q12 ();
  Printf.printf "\nNEXMark subset passed.\n"
