(** Прицельные тесты на ветки операторов Pipe, не покрытые остальным
    набором (выявлено через bisect_ppx): обработка не-Data событий
    (Watermark/Update) в stateful и sink. Эти ветки — ровно тот класс
    «не-happy-path», который остальные тесты обходили потоками из одних
    Data. *)

open Miniflink

let pass n = Printf.printf "  OK %s\n%!" n
let fail n = Printf.printf "  FAIL %s\n%!" n; exit 1
let check n c = if c then pass n else fail n

(* ── stateful: watermark должен проходить насквозь, не трогая f ── *)
let test_stateful_watermark_passthrough () =
  (* f считает Data; watermark не должен инкрементить и должен пройти *)
  let seen_data = ref 0 in
  let stream =
    [ Mf_event.data 1 0;
      Mf_event.wm 100;            (* watermark между Data *)
      Mf_event.data 2 110 ]
    |> Stream.of_list
    |> Pipe.stateful ~init:0
         ~f:(fun s ev -> match ev with
           | Mf_event.Data (v, ts) ->
             incr seen_data;
             (s + v, [ Mf_event.data (s + v) ts ])
           | _ -> (s, []))  (* не-Data сюда не доходит: stateful сам
                               пропускает Watermark мимо f *) in
  let out = ref [] in
  Stream.iter (fun ev -> out := ev :: !out) stream;
  let outs = List.rev !out in
  (* watermark должен присутствовать в выходе (прошёл насквозь) *)
  let has_wm = List.exists
    (function Mf_event.Watermark _ -> true | _ -> false) outs in
  check "stateful: watermark прошёл насквозь" has_wm;
  check "stateful: f вызвана только на Data (2 раза)" (!seen_data = 2)

(* ── sink: должен извлекать значение и из Data, и из Update ── *)
let test_sink_data_and_update () =
  let collected = ref [] in
  let stream =
    [ Mf_event.data 10 0;
      Mf_event.update 20 21 100;   (* Update: sink берёт new_value *)
      Mf_event.wm 200 ]            (* Watermark: sink игнорирует *)
    |> Stream.of_list in
  Pipe.sink (fun v -> collected := v :: !collected) stream;
  let vals = List.rev !collected in
  check "sink: собрал Data + Update.new_value (2 значения)"
    (List.length vals = 2);
  check "sink: Update дал new_value=21" (List.mem 21 vals);
  check "sink: Watermark проигнорирован" (not (List.mem 200 vals))

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Pipe operators: non-Data branches\n";
  Printf.printf "==========================================\n\n";
  test_stateful_watermark_passthrough ();
  test_sink_data_and_update ();
  Printf.printf "\nPipe non-Data branch tests passed.\n"
