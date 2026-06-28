(** Property: exactly-once при восстановлении (разблокировано фиксом
    §4.1).

    Главный инвариант системы чекпойнтинга: после краша восстановление
    из последнего чекпойнта плюс доигрывание остатка потока даёт ровно
    то же состояние, что бескрашевый прогон — каждое Data-событие
    обработано РОВНО один раз (без потерь и дублей).

    Раньше это нельзя было проверить property-тестом: cp_offset был
    рассинхронизирован с источником при watermark-ах, и recovery
    промахивался. После фикса (cp_offset согласован со снапшотом) проверяем
    для ЛЮБЫХ потоков:

    P1 (recovery+replay = n): для случайного потока Data вперемешку с
        Watermark/Retract, run_exactly_once → recover → доиграть остаток
        от cp_offset даёт суммарно ровно столько обработанных Data,
        сколько Data во входе. Ни потерь, ни дублей.

    Это не точечный пример (как существующие EO-тесты), а свойство для
    тысяч случайных комбинаций длины потока, числа воркеров, частоты
    чекпойнтов и расположения не-Data событий. *)

open Miniflink
module CP = Checkpoint_parallel
open QCheck

(* Счёт обработанных Data на ключ — состояние, по которому сверяем. *)
let count_process backend ev =
  match ev with
  | Mf_event.Data (key, _) ->
    let cur = match State_backend_memory.get backend key with
      | Some b -> int_of_string (Bytes.to_string b) | None -> 0 in
    State_backend_memory.set backend key
      (Bytes.of_string (string_of_int (cur + 1)));
    [key]
  | _ -> []  (* Watermark/Retract не меняют счётчик, но двигают позицию *)

(* поток: Data вперемешку с Watermark и Retract (последние двигают
   position источника, но не processed — именно это ломало §4.1) *)
let gen_events : string Mf_event.t list Gen.t =
  let open Gen in
  let key = map (fun i -> Printf.sprintf "k%d" i) (int_range 0 3) in
  sized_size (int_range 0 60) (fun n ->
    let rec build i ts acc =
      if i >= n then return (List.rev acc)
      else
        int_range 0 30 >>= fun dt ->
        let ts' = ts + dt in
        frequency [
          (5, key >>= fun k -> return (Mf_event.data k ts'));
          (2, return (Mf_event.wm ts'));
          (1, key >>= fun k -> return (Mf_event.retract k ts'));
        ] >>= fun ev -> build (i+1) ts' (ev :: acc)
    in build 0 0 [])

(* варьируем число воркеров и частоту чекпойнтов *)
let arb = make
  ~print:(fun (evs, w, cp) ->
    let nd = List.length (List.filter
      (function Mf_event.Data _ -> true | _ -> false) evs) in
    Printf.sprintf "n_data=%d workers=%d checkpoint_every=%d" nd w cp)
  Gen.(triple gen_events (int_range 1 4) (int_range 1 20))

let n_data evs =
  List.length (List.filter (function Mf_event.Data _ -> true | _ -> false) evs)

(* полный цикл: прогон → recover → доиграть остаток → суммарно
   обработанных Data *)
let recovery_replay_total evs workers checkpoint_every =
  let store = CP.make_store () in
  CP.run_exactly_once
    ~workers ~capacity:64 ~checkpoint_every
    ~key_of:(fun k -> k)
    ~make_state:State_backend_memory.create
    ~process:count_process
    ~source:(CP.seekable_of_list evs)
    ~sink:(CP.idempotent_sink (fun _ -> ()))
    ~store ();
  let src2 = CP.seekable_of_list evs in
  let backends = CP.recover ~workers
    ~make_state:State_backend_memory.create ~source:src2 store in
  let rec replay () = match src2.CP.pull () with
    | None -> ()
    | Some ev ->
      (match ev with
       | Mf_event.Data (k, _) ->
         ignore (count_process backends.(CP.hash_key k workers) ev)
       | _ -> ());
      replay () in
  replay ();
  Array.fold_left (fun acc b ->
    List.fold_left (fun a k -> match State_backend_memory.get b k with
      | Some v -> a + int_of_string (Bytes.to_string v) | None -> a)
      acc (State_backend_memory.keys b)) 0 backends

let prop_exactly_once_recovery =
  Test.make ~count:1000
    ~name:"exactly-once: recovery+replay = n_data (без потерь/дублей)"
    arb
    (fun (evs, workers, checkpoint_every) ->
       recovery_replay_total evs workers checkpoint_every = n_data evs)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Property: exactly-once recovery\n";
  Printf.printf "==========================================\n";
  Log.set_level Log.Error;
  QCheck_runner.run_tests_main [ prop_exactly_once_recovery ]
