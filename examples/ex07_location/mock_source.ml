(** Мок-источник пакетов фонарей шахтёров.

    Заменяемая часть пайплайна — это всё, что превращает «нашу симуляцию
    шахтёрской смены» в поток событий, готовый к обработке. В проде на
    его место станет {!Kafka_source.read} — тот же {!S}, отдающий
    {!Domain.packet}. Топология примера ([ex07_location.ml]) ничего не
    знает о происхождении пакетов.

    Источник РЕАЛИСТИЧНЫЙ по умолчанию: содержит опоздавшие пакеты и
    дубли — то, что at-least-once Kafka и UDP-радио в шахте создают
    неизбежно. Бенчмарки и regression-тесты используют именно такой
    источник: бенчмарк, прогоняющий «идеальный» поток, не отразит цены
    ретракций и dedup, которые в проде составляют существенную часть
    работы — он давал бы ложно-оптимистичные цифры и пропускал бы
    регрессии в коде ретрактов.

    Сценарий разложен на ДВА слоя в API, чтобы можно было исследовать
    вклад каждого аспекта явно:
    - [base_packets ()]: чистый поток (6 шахтёров каждые 15с);
    - [with_late_and_dups]: добавляет 4 опоздавших + 8 дублей.
    {!Default} применяет оба — это то, что видит ex07 и тесты. *)

open Miniflink
open Time
open Domain

(** Сигнатура источника пакетов: блок данных + статистика. *)
module type S = sig
  val read : unit -> packet Mf_event.t Stream.t
  val stats : unit -> string list   (** строки описания, для логирования *)
end

(** {2 Внутренности сценария} *)

let noise i k = float_of_int ((i * 7 + k * 13) mod 9) -. 4.0
(** Шум RSSI -4..+4 dB — детерминированный по [i] и [k]. *)

let steps = 25                          (* t = 0, 15, ..., 360с *)
let dt = seconds 15

let m1_voltage i = 4.0 -. 0.8 *. float_of_int i /. float_of_int (steps - 1)
(** Батарея M1 садится: 4.0В → 3.2В. К i=15 (t=225с) V≈3.5В,
    debounce 2 мин ⇒ алерт [Low_voltage] на t≈345с. *)

(** Базовая траектория шахтёра по индексу шага [i]. *)
let packets_for (lamp : string) (i : int) : packet option =
  let t = i * dt in
  let default = { lamp; ts = t; readings = [];
                  voltage = 4.0; moving = true; sos = false } in
  match lamp with
  | "M1" -> Some { default with
      readings = [ "B1", -45. +. noise i 1; "B2", -52. +. noise i 2 ];
      voltage  = m1_voltage i }
  | "M2" -> Some { default with
      readings = [ "B3", -48. +. noise i 6; "B4", -50. +. noise i 7;
                   "B5", -72. +. noise i 9 ];
      moving   = (t < seconds 120) }            (* стоит после t=120с → No_motion *)
  | "M3" ->
    Some { default with
      readings = if i * 15 <= 20
                 then [ "B1", -60. +. noise i 11 ]
                 else [] }                       (* пустые → No_readings *)
  | "M4" -> Some { default with
      readings = [ "B5", -55. +. noise i 12 ];
      sos      = (t = seconds 120) }            (* SOS-импульс *)
  | "M5" ->
    if i * 15 <= 100
    then Some { default with readings = [ "B2", -50. +. noise i 13 ] }
    else None                                   (* фонарь замолк → No_packets *)
  | "M6" ->
    (* Все четыре проблемы одновременно. Пакеты идут до конца, чтобы
       contact-FSM не подавил motion/voltage. *)
    Some { lamp; ts = t;
           readings = if t <= seconds 30
                      then [ "B3", -65. +. noise i 14 ]
                      else [];
           voltage  = 3.3;                       (* всегда ниже порога *)
           moving   = (t < seconds 120);        (* стоит после 120с *)
           sos      = (t = seconds 45) }        (* SOS на t=45с *)
  | _ -> None

let miners = ["M1"; "M2"; "M3"; "M4"; "M5"; "M6"]

(** {2 Публичный API} *)

(** Чистый поток пакетов: каждые 15с от 6 шахтёров. Используется
    бенчмарками (без аномалий канала). *)
let base_packets () : packet list =
  List.init steps (fun i ->
    List.filter_map (fun l -> packets_for l i) miners)
  |> List.concat

(** Добавить «дрожание канала»: 4 опоздавших пакета (опоздание ≥
    размера окна — гарантия retract) + 8 дублей (повторы существующих).
    Эти аномалии симулируют at-least-once Kafka + UDP-радио в шахте. *)
let with_late_and_dups (base : packet list) : packet list =
  let late_packets = [
    (* (вставить_после_ts, пакет) — четыре «дрожащих» опоздавших *)
    seconds 210, { lamp = "M1"; ts = seconds 118;
                   readings = [ "B1", -44.; "B2", -53. ];
                   voltage = m1_voltage 8; moving = true; sos = false };
    seconds 180, { lamp = "M2"; ts = seconds 88;
                   readings = [ "B3", -49.; "B4", -51.; "B5", -73. ];
                   voltage = 4.0; moving = true; sos = false };
    seconds 165, { lamp = "M3"; ts = seconds 73;
                   readings = [ "B1", -62. ]; voltage = 4.0;
                   moving = true; sos = false };
    seconds 120, { lamp = "M6"; ts = seconds 33;
                   readings = [ "B3", -64. ];
                   voltage = 3.3; moving = true; sos = false };
  ] in
  let dup_of lamp i = match packets_for lamp i with
    | Some p -> [p; p]    (* пакет + его дубль *)
    | None -> [] in
  let duplicates =
    dup_of "M1" 5 @ dup_of "M2" 10 @ dup_of "M4" 12 @ dup_of "M6" 6 in
  let with_late =
    List.fold_left (fun acc (insert_after, late) ->
      let before, after = List.partition (fun p -> p.ts <= insert_after) acc in
      before @ [late] @ after) base late_packets in
  with_late @ duplicates

(** {2 Реализация модуля {!S}} *)

(** Реалистичный источник: базовый поток + опоздавшие + дубли. ЭТО
    дефолт, потому что в проде канал ВСЕГДА даёт late-пакеты (UDP-радио)
    и дубли (Kafka at-least-once retry). Бенчмарки и тесты используют
    именно его — иначе мы измеряли бы нерепрезентативный «happy path».

    Если когда-нибудь понадобится изолировать вклад dedup или retracts —
    делать это явной параметризацией с дефолтом «как в проде», а не
    отдельным «чистым» источником: иначе кто-то наступит и опубликует
    приукрашенный бенчмарк. *)
module Default : S = struct
  let packets () = with_late_and_dups (base_packets ())
  let read () =
    Mf_event.of_list ~ts:(fun (p : packet) -> p.ts) (packets ())
  let stats () =
    let all = packets () in
    let unique = List.sort_uniq
      (fun a b -> compare (a.lamp, a.ts) (b.lamp, b.ts)) all in
    [
      Printf.sprintf "пакетов всего: %d" (List.length all);
      Printf.sprintf "из них дублей (тот же lamp+ts): %d"
        (List.length all - List.length unique);
    ]
end
