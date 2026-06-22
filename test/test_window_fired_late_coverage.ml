(** Покрытие window_fold веток для late событий на ЗАКРЫТЫЕ (FFired)
    окна — самая нетривиальная часть атомарной Update-семантики:
    - late Retract на FFired окно → atomic Update (откат);
    - late Update на FFired окно (retractable) → atomic Update;
    - late Update при non-retractable агрегате → best-effort (только new). *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

module K = struct type t = string * float let key (k,_) = k end

(* Собрать все события (Data + Update) после прогона. *)
let collect s =
  let acc = ref [] in
  let rec go () = match s () with
    | None -> ()
    | Some (Mf_event.Data ((_, r), _)) -> acc := `D r :: !acc; go ()
    | Some (Mf_event.Update { old = (_, o); new_value = (_, n); _ }) ->
      acc := `U (o, n) :: !acc; go ()
    | Some _ -> go ()
  in go (); List.rev !acc

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  window_fold: late events on Fired windows\n";
  Printf.printf "==========================================\n";

  (* ── 1. late Retract на закрытое окно → atomic Update ───────── *)
  Printf.printf "\n-- 1. late Retract on Fired window → atomic Update\n";
  (* sum, tumbling 10s, allowed_lateness 30s. Окно [0,10000) набирает
     10+20=30, закрывается на wm 15000 (эмит Data 30). Затем late
     Retract 10 (ts 5000, ещё в allowed_lateness) → откат до 20,
     atomic Update 30→20. *)
  let events1 = [
    Mf_event.data ("A", 10.) 0;
    Mf_event.data ("A", 20.) 1000;
    Mf_event.wm 15000;                  (* окно [0,10000) fires: Data 30 *)
    Mf_event.retract ("A", 10.) 5000;   (* late retract → Update 30→20 *)
    Mf_event.wm 20000;
  ] in
  let out1 = events1 |> Stream.of_list
    |> Pipe.window_agg (module K) ~allowed_lateness:(Time.seconds 30)
         (Pipe.tumbling (Time.seconds 10)) (Agg.sum snd)
    |> collect in
  Printf.printf "  events: %s\n%!"
    (String.concat "; " (List.map (function
       | `D r -> Printf.sprintf "D%.0f" r
       | `U (o,n) -> Printf.sprintf "U%.0f→%.0f" o n) out1));
  check "fired Data 30, then late Retract → Update 30→20"
    (out1 = [`D 30.; `U (30., 20.)]);

  (* ── 2. late Data на закрытое окно (retractable) → Update ───── *)
  Printf.printf "\n-- 2. late Data on Fired window → atomic Update\n";
  (* То же окно, но late ДОБАВЛЕНИЕ (Data) после закрытия → коррекция
     вверх: 30 → 35, atomic Update. *)
  let events2 = [
    Mf_event.data ("B", 10.) 0;
    Mf_event.data ("B", 20.) 1000;
    Mf_event.wm 15000;                  (* fires: Data 30 *)
    Mf_event.data ("B", 5.) 5000;       (* late add → Update 30→35 *)
    Mf_event.wm 20000;
  ] in
  let out2 = events2 |> Stream.of_list
    |> Pipe.window_agg (module K) ~allowed_lateness:(Time.seconds 30)
         (Pipe.tumbling (Time.seconds 10)) (Agg.sum snd)
    |> collect in
  check "fired Data 30, then late Data → Update 30→35"
    (out2 = [`D 30.; `U (30., 35.)]);

  (* ── 3. non-retractable агрегат: late на FFired → best-effort ─ *)
  Printf.printf "\n-- 3. non-retractable agg: late Data on Fired → best-effort Update\n";
  (* min_by не retractable, результат — float option. После закрытия
     late меньшее значение всё равно должно обновить min через
     emit_update (add-путь), а не потеряться молча. *)
  let collect_opt s =
    let acc = ref [] in
    let rec go () = match s () with
      | None -> ()
      | Some (Mf_event.Data ((_, r), _)) -> acc := `D r :: !acc; go ()
      | Some (Mf_event.Update { old = (_, o); new_value = (_, n); _ }) ->
        acc := `U (o, n) :: !acc; go ()
      | Some _ -> go ()
    in go (); List.rev !acc
  in
  let events3 = [
    Mf_event.data ("C", 50.) 0;
    Mf_event.data ("C", 30.) 1000;
    Mf_event.wm 15000;                  (* fires: min = 30 *)
    Mf_event.data ("C", 10.) 5000;      (* late smaller → Update 30→10 *)
    Mf_event.wm 20000;
  ] in
  let out3 = events3 |> Stream.of_list
    |> Pipe.window_agg (module K) ~allowed_lateness:(Time.seconds 30)
         (Pipe.tumbling (Time.seconds 10)) (Agg.min_by snd)
    |> collect_opt in
  check "non-retractable: late smaller value updates min 30→10"
    (out3 = [`D (Some 30.); `U (Some 30., Some 10.)]);

  Printf.printf "\nwindow_fold Fired-window late-event coverage passed.\n"
