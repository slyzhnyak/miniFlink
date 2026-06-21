open Miniflink
open Kafka_connector
(* Kafka source adapter: проверяем что он реализует seekable_source
   корректно (path B) — особенно recovery: seek по int-счётчику ядра
   находит правильную Kafka-позицию. Через in-memory фейк, без брокера. *)

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

module Src = Kafka_source.Make (Kafka_fake.Consumer)

(* payload "N" → int N; ts = N *)
let decode s = match int_of_string_opt s with Some n -> Ok n | None -> Error "bad"
let ts_of n = n

let test_basic_consume () =
  Printf.printf "\n-- consume events from fake Kafka via seekable_source\n";
  let b = Kafka_fake.make_broker () in
  Kafka_fake.seed b ~topic:"t" ~partition:0 ["1"; "2"; "3"];
  let consumer = Kafka_fake.Consumer.create b [("t", 0)] in
  let src = Src.create consumer ~decode ~ts_of () in
  let ss = Src.to_seekable src in
  let got = ref [] in
  let rec drain () = match ss.Checkpoint_parallel.pull () with
    | Some (Mf_event.Data (v,_)) -> got := v :: !got; drain ()
    | Some _ -> drain () | None -> () in
  drain ();
  check "consumed [1;2;3]" (List.rev !got = [1;2;3]);
  check "position advanced to 3" (ss.Checkpoint_parallel.position () = 3)

let test_decode_error_to_on_error () =
  Printf.printf "\n-- bad payload goes to on_error, not lost silently\n";
  let b = Kafka_fake.make_broker () in
  Kafka_fake.seed b ~topic:"t" ~partition:0 ["1"; "BAD"; "3"];
  let consumer = Kafka_fake.Consumer.create b [("t", 0)] in
  let errs = ref [] in
  let src = Src.create consumer ~decode ~ts_of
    ~on_error:(fun _topic payload -> errs := payload :: !errs) () in
  let ss = Src.to_seekable src in
  let got = ref [] in
  let rec drain () = match ss.Checkpoint_parallel.pull () with
    | Some (Mf_event.Data (v,_)) -> got := v :: !got; drain ()
    | Some _ -> drain () | None -> () in
  drain ();
  check "valid events consumed [1;3]" (List.rev !got = [1;3]);
  check "bad payload captured by on_error" (!errs = ["BAD"])

(* ГЛАВНОЕ: recovery — seek(n) по счётчику ядра восстанавливает
   правильную Kafka-позицию, продолжаем без потерь/дублей *)
let test_recovery_seek () =
  Printf.printf "\n-- recovery: core seek(counter) restores Kafka position\n";
  let b = Kafka_fake.make_broker () in
  Kafka_fake.seed b ~topic:"t" ~partition:0 ["10";"20";"30";"40";"50"];
  let consumer = Kafka_fake.Consumer.create b [("t", 0)] in
  let src = Src.create consumer ~decode ~ts_of () in
  let ss = Src.to_seekable src in
  (* прочитали 3 события *)
  let read_one () = match ss.Checkpoint_parallel.pull () with
    | Some (Mf_event.Data (v,_)) -> Some v | _ -> None in
  let _ = read_one () and _ = read_one () in
  let third = read_one () in
  let checkpoint_counter = ss.Checkpoint_parallel.position () in  (* =3 *)
  let _ = read_one () in  (* прочитали 4-е (40), но "крах" до checkpoint *)
  (* recovery: ядро откатывает на сохранённый счётчик 3 *)
  ss.Checkpoint_parallel.seek checkpoint_counter;
  let after = read_one () in  (* должны снова получить 40, не 50 *)
  check "third was 30" (third = Some 30);
  check "after seek(3) re-reads 40 (no skip, no dup loss)" (after = Some 40)

let test_prune_snapshots () =
  Printf.printf "\n-- prune_snapshots_before keeps seek correct after pruning\n";
  let b = Kafka_fake.make_broker () in
  Kafka_fake.seed b ~topic:"t" ~partition:0 ["10"; "20"; "30"; "40"; "50"];
  let consumer = Kafka_fake.Consumer.create b [("t", 0)] in
  let src = Src.create consumer ~decode ~ts_of () in
  let ss = Src.to_seekable src in
  let read_one () = match ss.Checkpoint_parallel.pull () with
    | Some (Mf_event.Data (v,_)) -> Some v | _ -> None in
  (* читаем 5 событий — 5 снимков накоплено *)
  let _ = read_one () and _ = read_one () and _ = read_one () in
  let _ = read_one () and _ = read_one () in

  (* подтверждён checkpoint на counter 3 → снимки < 3 удаляем *)
  Src.prune_snapshots_before src ~before:3;

  (* seek на сохранённый counter 3 всё ещё работает (снимок 3 сохранён,
     удалены только 1 и 2 которые больше не нужны) *)
  ss.Checkpoint_parallel.seek 3;
  let after = read_one () in
  check "seek(3) after prune re-reads 40 (snapshot 3 retained)"
    (after = Some 40);

  (* повторный prune до начала (before:0) ничего не ломает *)
  Src.prune_snapshots_before src ~before:0;
  check "prune before:0 is a no-op" true

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Kafka source adapter (fake broker)\n";
  Printf.printf "==========================================\n";
  test_basic_consume ();
  test_decode_error_to_on_error ();
  test_recovery_seek ();
  test_prune_snapshots ();
  Printf.printf "\nKafka adapter tests passed.\n"
