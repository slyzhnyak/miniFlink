(** Тест ex09 — синтетический сценарий, в котором triplet
    одновременно входит в опасную зону. Проверяет что multi-condition
    triple срабатывает корректно. *)

open Miniflink
open Ex09_complex_trigger_lib

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

(* Прямые updates в combined-формат, минуя реальные items.
   Это позволяет точно контролировать triplet и проверить только
   логику триггера. *)
let run_with combined_events =
  combined_events
  |> Stream.of_list
  |> Trigger.of_stream Triggers.evacuation

let () =
  Printf.printf "=== ex09 trigger test ===\n";

  (* ── Сценарий 1: все три условия выполнены сразу, держатся
     дольше problem_for=1мин → один алерт. ────────────────── *)
  Printf.printf "\n-- 1. All three conditions met, sustained → alert\n";

  let mk_combined co v rssi : Domain.combined =
    { co_ppm = co; voltage = v; avg_rssi = rssi;
      has_co = true; has_voltage = true; has_rssi = true } in

  let critical = mk_combined 100. 3.2 (-80.) in
  let evs = [
    Mf_event.data ("M1", critical) 0;
    Mf_event.data ("M1", critical) 30_000;
    Mf_event.data ("M1", critical) 60_000;
    Mf_event.data ("M1", critical) 90_000;    (* фаер по дозреванию *)
    Mf_event.wm 120_000;
  ] in
  let stream = run_with evs in
  let datas = ref 0 in
  let rec loop () = match stream () with
    | None -> ()
    | Some (Mf_event.Data _) -> incr datas; loop ()
    | Some _ -> loop ()
  in loop ();
  check (Printf.sprintf "alert fired (got %d Data events)" !datas)
    (!datas = 1);

  (* ── Сценарий 2: одно из условий не выполнено → нет алерта.
     CO высокое, voltage низкое, но rssi сильный (-50). ─── *)
  Printf.printf "\n-- 2. Strong RSSI breaks AND condition → no alert\n";

  let safe_rssi = mk_combined 100. 3.2 (-50.) in
  let evs = [
    Mf_event.data ("M1", safe_rssi) 0;
    Mf_event.data ("M1", safe_rssi) 60_000;
    Mf_event.data ("M1", safe_rssi) 90_000;
    Mf_event.wm 120_000;
  ] in
  let stream = run_with evs in
  let datas = ref 0 in
  let rec loop () = match stream () with
    | None -> ()
    | Some (Mf_event.Data _) -> incr datas; loop ()
    | Some _ -> loop ()
  in loop ();
  check (Printf.sprintf "no alert (got %d)" !datas) (!datas = 0);

  (* ── Сценарий 3: все три условия, но has_co=false → нет алерта.
     Это проверка инициализации: если данных по CO нет вообще,
     init-значение co_ppm=0 не должно вводить в заблуждение. ─── *)
  Printf.printf "\n-- 3. Triplet incomplete (no CO data) → no alert\n";

  let no_co : Domain.combined =
    { co_ppm = 0.; voltage = 3.2; avg_rssi = -80.;
      has_co = false; has_voltage = true; has_rssi = true } in
  let evs = [
    Mf_event.data ("M1", no_co) 0;
    Mf_event.data ("M1", no_co) 60_000;
    Mf_event.data ("M1", no_co) 90_000;
    Mf_event.wm 120_000;
  ] in
  let stream = run_with evs in
  let datas = ref 0 in
  let rec loop () = match stream () with
    | None -> ()
    | Some (Mf_event.Data _) -> incr datas; loop ()
    | Some _ -> loop ()
  in loop ();
  check (Printf.sprintf "no alert (got %d)" !datas) (!datas = 0);

  (* ── Сценарий 4: критическая ситуация дозревает (нужен watermark
     чтобы сработал problem-таймер), потом recovery (упало
     одно из условий, дозревает recovery-таймер). ────── *)
  Printf.printf "\n-- 4. Recovery when ONE condition clears\n";

  let critical2 = mk_combined 100. 3.2 (-80.) in
  let recovered = mk_combined 100. 3.8 (-80.) in  (* voltage поднялось *)
  let evs = [
    Mf_event.data ("M1", critical2) 0;
    Mf_event.wm 70_000;                            (* фаер problem-таймера на 60000 *)
    Mf_event.data ("M1", critical2) 90_000;
    Mf_event.data ("M1", recovered) 120_000;       (* recovery start *)
    Mf_event.wm 200_000;                           (* фаер recovery-таймера на 150000 *)
  ] in
  let stream = run_with evs in
  let data_count = ref 0 in
  let retract_count = ref 0 in
  let rec loop () = match stream () with
    | None -> ()
    | Some (Mf_event.Data _) -> incr data_count; loop ()
    | Some (Mf_event.Retract _) -> incr retract_count; loop ()
    | Some _ -> loop ()
  in loop ();
  check (Printf.sprintf "alert + recovery: 2 Data, 1 Retract (got %dD %dR)"
           !data_count !retract_count)
    (!data_count = 2 && !retract_count = 1);

  Printf.printf "\nAll ex09 tests passed.\n"
