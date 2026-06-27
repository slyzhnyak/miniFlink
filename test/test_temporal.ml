open Miniflink
(* Тесты temporal join. Главное требование: корректность даже когда
   апдейты справочника приходят С ОПОЗДАНИЕМ (поздно по порядку прихода,
   но с ранним valid_from). Показание должно обогащаться значением,
   актуальным на ЕГО event-time, а не «текущим» и не «первым попавшимся». *)

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

(* событие основного потока: (id, "обогащение") *)
type reading = { rid : string; depot : string; rts : int }
(* апдейт справочника: (id, депо, с какого времени) *)
type upd = { uid : string; udepot : string; ufrom : int }

let enriched stream =
  Stream.to_list stream
  |> List.filter_map (function Mf_event.Data (r,_) -> Some r | _ -> None)

(* собрать ВСЕ события (для проверки проброса Update/Retract) *)
let all_events stream = Stream.to_list stream

(* ── temporal_join пробрасывает Update/Retract из main-потока ──
   Update — атомарная коррекция, Retract — отзыв; оба должны пройти
   насквозь (downstream разберётся), а Data — обогатиться как обычно. *)
let test_join_update_retract () =
  Printf.printf "\n-- temporal_join passes Update/Retract through, enriches Data\n";
  let updates = Stream.of_list [
    Mf_event.data { uid="B1"; udepot="north"; ufrom=0 } 0;
    Mf_event.wm 100;
  ] in
  let main = Stream.of_list [
    Mf_event.data { rid="B1"; depot="?"; rts=10 } 10;       (* → north *)
    Mf_event.update { rid="B1"; depot="old"; rts=20 }
                    { rid="B1"; depot="new"; rts=20 } 20;   (* проброс *)
    Mf_event.retract { rid="B1"; depot="gone"; rts=30 } 30; (* проброс *)
    Mf_event.wm 100;
  ] in
  let out = main
    |> Temporal.temporal_join
         ~key_main:(fun r -> r.rid) ~key_upd:(fun u -> u.uid)
         ~valid_from:(fun u -> u.ufrom)
         ~merge:(fun r d -> match d with Some u -> { r with depot=u.udepot } | None -> r)
         ~updates
    |> all_events in
  let has_update = List.exists
    (function Mf_event.Update _ -> true | _ -> false) out in
  let has_retract = List.exists
    (function Mf_event.Retract _ -> true | _ -> false) out in
  let data_enriched = List.exists
    (function Mf_event.Data (r,_) -> r.depot = "north" | _ -> false) out in
  check "Update проброшен насквозь" has_update;
  check "Retract проброшен насквозь" has_retract;
  check "Data обогащён (north)" data_enriched


(* ── as_of: чистая логика версий ──────────────────────────── *)
let test_as_of () =
  Printf.printf "\n-- as_of picks the version valid at the query time\n";
  let tbl = Temporal.create_versioned () in
  Temporal.put_version tbl ~key:"B1" ~valid_from:0   "north";
  Temporal.put_version tbl ~key:"B1" ~valid_from:100 "south";
  check "as_of 50 → north (до перевода)"  (Temporal.as_of tbl "B1" 50  = Some "north");
  check "as_of 150 → south (после)"       (Temporal.as_of tbl "B1" 150 = Some "south");
  check "as_of 0 → north (граница)"       (Temporal.as_of tbl "B1" 0   = Some "north");
  check "as_of -10 → None (до первой версии)" (Temporal.as_of tbl "B1" (-10) = None);
  check "as_of unknown → None"            (Temporal.as_of tbl "B9" 50 = None)

(* ── порядок вставки версий не важен (опоздавший апдейт) ──── *)
let test_out_of_order_versions () =
  Printf.printf "\n-- put_version keeps history sorted regardless of insert order\n";
  let tbl = Temporal.create_versioned () in
  (* вставляем НЕ по порядку: сначала поздняя версия, потом ранняя *)
  Temporal.put_version tbl ~key:"B1" ~valid_from:100 "south";
  Temporal.put_version tbl ~key:"B1" ~valid_from:0   "north";
  Temporal.put_version tbl ~key:"B1" ~valid_from:50  "east";
  check "as_of 25 → north"  (Temporal.as_of tbl "B1" 25  = Some "north");
  check "as_of 75 → east"   (Temporal.as_of tbl "B1" 75  = Some "east");
  check "as_of 150 → south" (Temporal.as_of tbl "B1" 150 = Some "south")

(* ── temporal_join: апдейт пришёл ВОВРЕМЯ ─────────────────── *)
let test_join_basic () =
  Printf.printf "\n-- temporal_join: enrich with version valid at event-time\n";
  let updates = Stream.of_list [
    Mf_event.data { uid="B1"; udepot="north"; ufrom=0 } 0;
    Mf_event.data { uid="B1"; udepot="south"; ufrom=100 } 100;
    Mf_event.wm 200;
  ] in
  let main = Stream.of_list [
    Mf_event.data { rid="B1"; depot="?"; rts=50 } 50;   (* до перевода → north *)
    Mf_event.data { rid="B1"; depot="?"; rts=150 } 150; (* после → south *)
    Mf_event.wm 200;
  ] in
  let out = main
    |> Temporal.temporal_join
         ~key_main:(fun r -> r.rid) ~key_upd:(fun u -> u.uid)
         ~valid_from:(fun u -> u.ufrom)
         ~merge:(fun r d -> match d with Some u -> { r with depot=u.udepot } | None -> r)
         ~updates
    |> enriched in
  let depots = List.map (fun r -> (r.rts, r.depot)) out |> List.sort compare in
  check "t=50 → north, t=150 → south" (depots = [(50,"north"); (150,"south")])

(* ── ГЛАВНОЕ: апдейт пришёл С ОПОЗДАНИЕМ ──────────────────── *)
let test_late_update () =
  Printf.printf "\n-- temporal_join: LATE update still enriches correctly\n";
  (* Апдейт valid_from=0 (north) приходит ПОСЛЕ апдейта valid_from=100,
     и watermark апдейтов поднимается только в конце. Показание t=50
     должно ДОЖДАТЬСЯ и получить north, а не «текущее» south и не None. *)
  let updates = Stream.of_list [
    Mf_event.data { uid="B1"; udepot="south"; ufrom=100 } 100; (* пришёл первым *)
    (* watermark ещё низкий — апдейт за t=0 ещё «в пути» *)
    Mf_event.data { uid="B1"; udepot="north"; ufrom=0 } 0;     (* ОПОЗДАВШИЙ: valid_from=0 *)
    Mf_event.wm 200;
  ] in
  let main = Stream.of_list [
    Mf_event.data { rid="B1"; depot="?"; rts=50 } 50;
    Mf_event.wm 200;
  ] in
  let out = main
    |> Temporal.temporal_join
         ~key_main:(fun r -> r.rid) ~key_upd:(fun u -> u.uid)
         ~valid_from:(fun u -> u.ufrom)
         ~merge:(fun r d -> match d with Some u -> { r with depot=u.udepot } | None -> r)
         ~updates
    |> enriched in
  check "late update applied: t=50 → north (not south, not None)"
    (List.map (fun r -> r.depot) out = ["north"])

(* ── показание ждёт пока watermark апдейтов догонит ───────── *)
let test_waits_for_watermark () =
  Printf.printf "\n-- reading waits until update-watermark covers its event-time\n";
  (* апдейт для B1 ещё не пришёл, watermark апдейтов низкий → показание
     не должно эмититься с None раньше времени; когда апдейт+wm приходят,
     получает правильное значение *)
  let updates = Stream.of_list [
    Mf_event.wm 10;   (* wm=10, апдейтов пока нет *)
    Mf_event.data { uid="B1"; udepot="north"; ufrom=20 } 20;
    Mf_event.wm 100;
  ] in
  let main = Stream.of_list [
    Mf_event.data { rid="B1"; depot="?"; rts=50 } 50;
    Mf_event.wm 100;
  ] in
  let out = main
    |> Temporal.temporal_join
         ~key_main:(fun r -> r.rid) ~key_upd:(fun u -> u.uid)
         ~valid_from:(fun u -> u.ufrom)
         ~merge:(fun r d -> match d with Some u -> { r with depot=u.udepot } | None -> r)
         ~updates
    |> enriched in
  check "t=50 enriched with north (waited for update at vf=20)"
    (List.map (fun r -> r.depot) out = ["north"])

(* ── детерминизм: перемешанный приход == упорядоченный ─────── *)
let test_determinism () =
  Printf.printf "\n-- temporal join is deterministic under scrambled arrival\n";
  (* апдейты и показания в ПРОИЗВОЛЬНОМ порядке прихода, но с честными
     event-time; результат должен совпасть с прогоном «всё по порядку» *)
  let mk_updates order =
    Stream.of_list (order @ [Mf_event.wm 1000]) in
  let upd_scrambled = [
    Mf_event.data { uid="B1"; udepot="south"; ufrom=300 } 300;
    Mf_event.data { uid="B2"; udepot="west";  ufrom=0 }   0;
    Mf_event.data { uid="B1"; udepot="north"; ufrom=0 }   0;   (* опоздал *)
    Mf_event.data { uid="B1"; udepot="east";  ufrom=150 } 150;
    Mf_event.data { uid="B2"; udepot="north"; ufrom=200 } 200;
  ] in
  let upd_sorted =
    List.sort (fun a b -> compare (Mf_event.ts a) (Mf_event.ts b)) upd_scrambled in
  let readings = [
    Mf_event.data { rid="B1"; depot="?"; rts=100 } 100;  (* B1@100 → north *)
    Mf_event.data { rid="B2"; depot="?"; rts=50 }  50;   (* B2@50  → west *)
    Mf_event.data { rid="B1"; depot="?"; rts=200 } 200;  (* B1@200 → east *)
    Mf_event.data { rid="B1"; depot="?"; rts=400 } 400;  (* B1@400 → south *)
    Mf_event.data { rid="B2"; depot="?"; rts=250 } 250;  (* B2@250 → north *)
  ] in
  let run updates =
    Stream.of_list (readings @ [Mf_event.wm 1000])
    |> Temporal.temporal_join
         ~key_main:(fun r -> r.rid) ~key_upd:(fun u -> u.uid)
         ~valid_from:(fun u -> u.ufrom)
         ~merge:(fun r d -> match d with Some u -> { r with depot=u.udepot } | None -> r)
         ~updates
    |> enriched
    |> List.map (fun r -> (r.rid, r.rts, r.depot))
    |> List.sort compare in
  let scrambled = run (mk_updates upd_scrambled) in
  let sorted = run (mk_updates upd_sorted) in
  check "scrambled updates == sorted updates" (scrambled = sorted);
  check "B1@100=north B1@200=east B1@400=south B2@50=west B2@250=north"
    (scrambled = [("B1",100,"north"); ("B1",200,"east"); ("B1",400,"south");
                  ("B2",50,"west"); ("B2",250,"north")])

let test_prune_versions () =
  Printf.printf "\n-- prune_versions_before bounds history, keeps as_of correct\n";
  let tbl = Temporal.create_versioned () in
  (* 4 версии B1: valid_from 0, 100, 200, 300 *)
  Temporal.put_version tbl ~key:"B1" ~valid_from:0   "v0";
  Temporal.put_version tbl ~key:"B1" ~valid_from:100 "v100";
  Temporal.put_version tbl ~key:"B1" ~valid_from:200 "v200";
  Temporal.put_version tbl ~key:"B1" ~valid_from:300 "v300";

  (* обрезаем до before:200 — оставляем версии >= 200 + одну
     предшествующую (100). Версия 0 удаляется. *)
  Temporal.prune_versions_before tbl ~before:200;

  (* as_of для ts >= 200 корректен *)
  check "as_of 250 → v200 (retained)" (Temporal.as_of tbl "B1" 250 = Some "v200");
  check "as_of 350 → v300 (retained)" (Temporal.as_of tbl "B1" 350 = Some "v300");
  (* as_of 150 → v100 (предшествующая сохранена для границы) *)
  check "as_of 150 → v100 (predecessor kept)" (Temporal.as_of tbl "B1" 150 = Some "v100");
  (* as_of 50 теперь даёт v100 (v0 удалена) — приемлемо, т.к. before:200
     означает что запросов для ts < 200 не будет; проверяем что не падает *)
  check "as_of 50 after prune doesn't crash"
    (match Temporal.as_of tbl "B1" 50 with Some _ | None -> true)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Temporal join (versioned / as-of)\n";
  Printf.printf "==========================================\n";
  test_as_of ();
  test_out_of_order_versions ();
  test_join_basic ();
  test_join_update_retract ();
  test_late_update ();
  test_waits_for_watermark ();
  test_determinism ();
  test_prune_versions ();
  Printf.printf "\nAll temporal join tests passed.\n"
