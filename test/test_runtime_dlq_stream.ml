(** Прицельные тесты на DLQ-aware источник Runtime, не покрытые
    остальным набором (выявлено через bisect): make_stream_with_dlq и
    safe_decode на хорошем/битом payload, в разных режимах конфига
    (для покрытия и log-DLQ, и noop-DLQ веток make_dlq). Сетевой код
    (Prometheus endpoint, reporter-поток) намеренно не трогаем — он
    хрупок в тестах; здесь только синхронная логика декодирования. *)

open Miniflink

let pass n = Printf.printf "  OK %s\n%!" n
let fail n = Printf.printf "  FAIL %s\n%!" n; exit 1
let check n c = if c then pass n else fail n

(* codec: bytes "N" -> int N; всё прочее — ошибка декодирования *)
let int_codec (b : bytes) : (int, string) result =
  match int_of_string_opt (Bytes.to_string b) with
  | Some n -> Ok n
  | None -> Error ("bad int: " ^ Bytes.to_string b)

let drain s =
  let acc = ref [] in
  let rec go () = match s () with
    | None -> () | Some (Mf_event.Data (v,_)) -> acc := v :: !acc; go ()
    | Some _ -> go ()
  in go (); List.rev !acc

(* источник из списка (topic, payload) *)
let source_of items =
  let r = ref items in
  fun () -> match !r with
    | [] -> None
    | x :: rest -> r := rest; Some x

let run_mode cfg label =
  (* Хорошие payload'ы декодируются; битый в СЕРЕДИНЕ обрывает поток.
     Это фактическое поведение make_stream_with_dlq: на ошибке
     декодирования возвращается None, что Stream трактует как конец
     потока, поэтому хвост после битого элемента не читается. (Поведение
     спорное — битое сообщение в середине глотает остаток, — но тест
     фиксирует реальность; изменение семантики — отдельное решение.) *)
  let items = [ ("t", Bytes.of_string "10");
                ("t", Bytes.of_string "20");
                ("t", Bytes.of_string "oops");   (* битый → DLQ + конец *)
                ("t", Bytes.of_string "30") ] in
  let stream =
    Runtime.make_stream_with_dlq cfg
      ~topic:"t" ~codec:int_codec ~ts_of:(fun _ -> 0)
      (source_of items) in
  let got = drain stream in
  check (label ^ ": хороший префикс декодирован до битого (10,20)")
    (got = [10; 20])

(* отдельно: одиночный битый payload в начале → пустой поток *)
let run_bad_first cfg label =
  let items = [ ("t", Bytes.of_string "nope") ] in
  let stream =
    Runtime.make_stream_with_dlq cfg
      ~topic:"t" ~codec:int_codec ~ts_of:(fun _ -> 0)
      (source_of items) in
  check (label ^ ": битый первым → пустой поток") (drain stream = [])

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Runtime: DLQ-aware stream + safe_decode\n";
  Printf.printf "==========================================\n\n";
  (* log_cfg → ветка Log в make_dlq (Dlq_log) *)
  run_mode Runtime.log_cfg "log";
  (* noop → ветка _ в make_dlq (Dlq_noop) *)
  run_mode Runtime.noop "noop";
  run_bad_first Runtime.log_cfg "log";
  run_bad_first Runtime.noop "noop";
  Printf.printf "\nRuntime DLQ-stream tests passed.\n"
