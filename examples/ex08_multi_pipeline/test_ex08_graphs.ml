(** Тест декларативных графов примера 8 на моках — БЕЗ Kafka.

    Прогоняет три пайплайна через [Stream.of_list], проверяя логику. Это
    и есть та развязка I/O от логики, ради которой графы вынесены в
    {!Pipelines}: тот же алгоритм тестируется на списках и работает на
    живом брокере. *)

open Miniflink
open Time
open Pipelines

let failed = ref false
let check name cond =
  Printf.printf "  %s %s\n" (if cond then "OK" else "FAIL") name;
  if not cond then failed := true

let r ~lamp ?(beacon="b1") ?(rssi=(-60.)) ?(voltage=3.7) ~ts () =
  { lamp; beacon; rssi; voltage; ts_ms = ts }

let ev x = Mf_event.data x x.ts_ms
let wm t = Mf_event.wm t

let collect stream = Stream.to_list stream |> List.filter_map Mf_event.value

let () =
  Printf.printf "ex08 графы (на моках, без Kafka)\n%!";

  (* ── Пайплайн 1: низкое напряжение + dedup cooldown ── *)
  Printf.printf "\n-- voltage_alerts\n";
  let out =
    Stream.of_list [
      ev (r ~lamp:"L1" ~voltage:3.0 ~ts:1000 ());   (* просадка → алерт *)
      ev (r ~lamp:"L1" ~voltage:3.0 ~ts:2000 ());   (* в cooldown → нет *)
      ev (r ~lamp:"L2" ~voltage:3.1 ~ts:2500 ());   (* другая лампа → алерт *)
      ev (r ~lamp:"L1" ~voltage:3.9 ~ts:3000 ());   (* норма → отфильтр *)
      wm 200_000;
    ]
    |> voltage_alerts |> collect in
  check "L1 и L2 дали по алерту, дубль L1 подавлен cooldown'ом"
    (out = [ Low_voltage ("L1", 3.0); Low_voltage ("L2", 3.1) ]);

  (* ── Пайплайн 2: потеря связи через on_silence ── *)
  Printf.printf "\n-- loss_alerts\n";
  let out =
    Stream.of_list [
      ev (r ~lamp:"L1" ~ts:1000 ());
      ev (r ~lamp:"L2" ~ts:1000 ());
      ev (r ~lamp:"L1" ~ts:20000 ());  (* L1 продолжает слать *)
      (* watermark 35с: L1 замолчала на 15с (<30, активна),
         L2 замолчала на 34с (>30 → No_contact) *)
      wm 35_000;
    ]
    |> loss_alerts |> collect in
  check "L2 замолчала → No_contact, L1 активна → нет"
    (List.mem (No_contact "L2") out
     && not (List.mem (No_contact "L1") out));

  (* ── Пайплайн 3: позиционирование по медиане RSSI ── *)
  Printf.printf "\n-- positioning\n";
  let out =
    Stream.of_list [
      ev (r ~lamp:"L1" ~rssi:(-60.) ~ts:1000 ());
      ev (r ~lamp:"L1" ~rssi:(-62.) ~ts:3000 ());
      ev (r ~lamp:"L1" ~rssi:(-58.) ~ts:5000 ());
      wm 100_000;
    ]
    |> positioning |> collect in
  check "позиция для L1 посчитана (окно выдало хотя бы одну оценку)"
    (List.exists (fun p -> p.p_lamp = "L1") out);

  Printf.printf "\n%s\n" (if !failed then "ЕСТЬ ПАДЕНИЯ" else "все графы прошли");
  if !failed then exit 1
