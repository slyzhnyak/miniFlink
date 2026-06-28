(* ============================================================
   Пример 11 — учёт присутствия людей в зонах шахты.

   Показывает случай, где из трёх «корректирующих» событий нужен
   именно {!Mf_event.Retract} — исчезновение БЕЗ замены. Это отличает
   его от ex07, где коррекции алертов — это замена old→new и потому
   выражаются атомарным {!Mf_event.Update}.

   Модель присутствия горняка:
     • появился в зоне        → [Data]   (новое присутствие)
     • перешёл в другую зону   → [Update] (замена: зона A → зона B)
     • вышел / потеря связи     → [Retract] (присутствие ИСЧЕЗАЕТ)

   Ключевая мысль: «человек покинул зону» нельзя выразить через
   [Update], потому что НЕТ нового значения — присутствия больше нет.
   [Update] требует [new_value]; здесь его не существует. Поэтому
   Retract — не легаси-приём, а единственно правильное событие.

   Почему это важно для безопасности: downstream ведёт live-карту
   «кто где в шахте». При ЧП (обрушение, газ) диспетчеру нужно точно
   знать, кто остался в зоне. Если бы выход выражался как Update на
   какое-то «пустое» значение, на карте остался бы фантомный человек.
   Retract убирает его из карты чисто.
   ============================================================ *)

open Miniflink
open Time

(* ── Модель ───────────────────────────────────────────────── *)

type presence = {
  miner : string;     (* идентификатор горняка (тег лампы) *)
  zone  : string;     (* зона шахты *)
  ts    : Time.t;
}

module ByMiner : Keyed.S with type t = presence = struct
  type t = presence
  let key p = p.miner
end

(* Сырое событие от системы позиционирования. *)
type ping =
  | Seen of string * string * Time.t   (* miner, zone, ts *)
  | Lost of string * Time.t            (* miner потерян (нет связи), ts *)

(* ── Оператор присутствия ─────────────────────────────────────
   Держит последнюю известную зону каждого горняка и переводит сырые
   ping-и в события присутствия:
     - первое появление          → Data
     - смена зоны                 → Update (атомарная замена зоны)
     - тот же зон (повтор)        → ничего (без спама)
     - Lost                       → Retract (присутствие исчезает) *)

let presence_events (raw : ping Stream.t) : presence Mf_event.t Stream.t =
  let last : (string, presence) Hashtbl.t = Hashtbl.create 64 in
  let pending : presence Mf_event.t Queue.t = Queue.create () in
  let rec next () =
    if not (Queue.is_empty pending) then Some (Queue.pop pending)
    else match raw () with
      | None -> None
      | Some (Seen (miner, zone, ts)) ->
        let cur = { miner; zone; ts } in
        (match Hashtbl.find_opt last miner with
         | None ->
           (* первое появление — новое присутствие *)
           Hashtbl.replace last miner cur;
           Some (Mf_event.data cur ts)
         | Some old when old.zone = zone ->
           (* та же зона — без спама, ничего не эмитим *)
           next ()
         | Some old ->
           (* смена зоны — атомарная ЗАМЕНА (Update), не исчезновение *)
           Hashtbl.replace last miner cur;
           Some (Mf_event.update old cur ts))
      | Some (Lost (miner, ts)) ->
        (match Hashtbl.find_opt last miner with
         | None -> next ()   (* не знали о нём — нечего отзывать *)
         | Some old ->
           (* присутствие ИСЧЕЗАЕТ — Retract. Замены нет: человека
              больше нет ни в одной зоне. Update здесь невыразим. *)
           Hashtbl.remove last miner;
           ignore ts;
           Some (Mf_event.retract old old.ts))
  in next

(* ── Downstream: live-карта «кто в какой зоне» ────────────────
   Материализует поток присутствия в текущую картину. Показывает, как
   каждый тип события влияет на карту:
     Data    → добавить/обновить
     Update  → переместить (old.zone убрать, new.zone поставить)
     Retract → убрать совсем *)

let live_map (events : presence Mf_event.t list) : (string * string) list =
  let map : (string, string) Hashtbl.t = Hashtbl.create 64 in
  List.iter (function
    | Mf_event.Data (p, _) -> Hashtbl.replace map p.miner p.zone
    | Mf_event.Update { new_value = p; _ } ->
      Hashtbl.replace map p.miner p.zone   (* атомарный переход *)
    | Mf_event.Retract (p, _) -> Hashtbl.remove map p.miner  (* исчез *)
    | Mf_event.Watermark _ -> ()) events;
  Hashtbl.fold (fun m z acc -> (m, z) :: acc) map []
  |> List.sort compare

(* ── Сценарий ─────────────────────────────────────────────── *)

let sample = [
  Seen ("miner_A", "shaft_1", seconds 1);    (* A появился в shaft_1 *)
  Seen ("miner_B", "shaft_1", seconds 2);    (* B появился в shaft_1 *)
  Seen ("miner_A", "shaft_1", seconds 3);    (* A там же — без спама *)
  Seen ("miner_A", "tunnel_3", seconds 5);   (* A перешёл → Update *)
  Seen ("miner_C", "shaft_2", seconds 6);    (* C появился в shaft_2 *)
  Lost ("miner_B", seconds 8);               (* B потерял связь → Retract *)
  Seen ("miner_A", "tunnel_3", seconds 9);   (* A там же — без спама *)
  Lost ("miner_A", seconds 12);              (* A вышел → Retract *)
]

let () =
  Printf.printf "=== Пример 11: учёт присутствия (где нужен Retract) ===\n\n";
  let events =
    Stream.of_list sample
    |> presence_events
    |> Stream.to_list in

  Printf.printf "Поток событий присутствия:\n";
  List.iter (function
    | Mf_event.Data (p, _) ->
      Printf.printf "  [Data]    %s появился в %s\n" p.miner p.zone
    | Mf_event.Update { old; new_value; _ } ->
      Printf.printf "  [Update]  %s перешёл %s → %s\n"
        new_value.miner old.zone new_value.zone
    | Mf_event.Retract (p, _) ->
      Printf.printf "  [Retract] %s покинул шахту (был в %s)\n" p.miner p.zone
    | Mf_event.Watermark _ -> ()) events;

  Printf.printf "\nИтоговая live-карта (кто где сейчас):\n";
  let map = live_map events in
  if map = [] then Printf.printf "  (пусто — все вышли)\n"
  else List.iter (fun (m, z) -> Printf.printf "  %s → %s\n" m z) map;

  Printf.printf "\nПочему здесь Retract, а не Update:\n";
  Printf.printf "  Выход из шахты — это ИСЧЕЗНОВЕНИЕ присутствия, замены нет.\n";
  Printf.printf "  Update требует new_value; для «человека больше нет» его\n";
  Printf.printf "  не существует. Retract убирает горняка с карты чисто —\n";
  Printf.printf "  при ЧП диспетчер не увидит фантомов.\n";
  Printf.printf "\n  (Сравните: смена зоны — это ЗАМЕНА, и там корректен Update,\n";
  Printf.printf "   как в ex07 для коррекции газовых алертов.)\n"
