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
     - Lost                       → Retract (присутствие исчезает)

   Реализовано через {!Pipe.process_keyed}: состояние на горняка
   (последняя зона) держит библиотека, а Retract/Update эмитятся
   штатными [ctx.emit_retract] / [ctx.emit_update] — те самые
   первичные средства, ради демонстрации которых и существует пример.
   Ключ партиционирования — miner; ping оборачивается в событие с его
   [ts], поэтому per-key состояние и эмиссии несут event-time. *)

module ByPing : Keyed.S with type t = ping = struct
  type t = ping
  let key = function Seen (m, _, _) -> m | Lost (m, _) -> m
end

(* состояние на горняка: последнее известное присутствие (или None) *)
type pstate = { mutable current : presence option }

let presence_events (raw : ping Stream.t) : presence Mf_event.t Stream.t =
  raw
  |> Stream.map (fun ping ->
       let ts = match ping with Seen (_, _, t) | Lost (_, t) -> t in
       Mf_event.data ping ts)
  |> Pipe.process_keyed (module ByPing)
       ~init:(fun () -> { current = None })
       ~on_event:(fun ctx miner st ping ->
         match ping with
         | Seen (_, zone, ts) ->
           let cur = { miner; zone; ts } in
           (match st.current with
            | None ->
              st.current <- Some cur;
              ctx.Pipe.emit cur                 (* первое появление *)
            | Some old when old.zone = zone -> () (* та же зона — без спама *)
            | Some old ->
              st.current <- Some cur;
              ctx.Pipe.emit_update ~old cur)    (* смена зоны — Update *)
         | Lost (_, _) ->
           (match st.current with
            | None -> ()                         (* не знали — нечего отзывать *)
            | Some old ->
              st.current <- None;
              ctx.Pipe.emit_retract old))        (* присутствие исчезает *)
       ~on_timer:(fun _ _ _ _ _ -> ())

(* ── Downstream: live-карта «кто в какой зоне» ────────────────
   Материализует поток присутствия в текущую картину через
   {!Pipe.materialize}: Data кладёт, Update атомарно заменяет, Retract
   убирает. Идентичность записи — miner. Раньше здесь был ручной
   Hashtbl-проход по конструкторам; materialize делает ровно это
   декларативно. *)

let live_map (events : presence Mf_event.t list) : (string * string) list =
  Stream.of_list events
  |> Pipe.materialize ~by:(fun p _ts -> p.miner)
  |> List.map (fun (miner, p) -> (miner, p.zone))
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
