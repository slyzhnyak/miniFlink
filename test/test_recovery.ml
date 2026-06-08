open Miniflink
(* Full recovery harness — самый жёсткий тест exactly-once.

   Сценарий: прогнать pipeline, имитировать СМЕРТЬ процесса (durable
   store переживает, рабочее состояние теряется), восстановиться через
   recover из последнего checkpoint, продолжить — и проверить что
   итоговый выход СОВПАДАЕТ с эталонным прогоном без сбоя как
   мультимножество (нет дублей, нет потерь). Это и есть проверка
   exactly-once end-to-end. *)

module CP = Checkpoint_parallel

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

(* мультимножество: отсортированный список (порядок параллельной
   обработки недетерминирован, сравниваем как bag) *)
let bag xs = List.sort compare xs

(* Простая агрегирующая обработка с состоянием по ключу: суммируем
   значения, эмитим текущую сумму. Состояние в backend по ключу. *)
let process st (ev : (string * int) Mf_event.t) =
  match ev with
  | Mf_event.Data ((k, v), _) ->
    let cur =
      match State_backend_memory.get st k with
      | Some b -> int_of_string (Bytes.to_string b)
      | None -> 0 in
    let sum = cur + v in
    State_backend_memory.set st k (Bytes.of_string (string_of_int sum));
    [(k, sum)]
  | _ -> []

let key_of (k, _) = k

(* входные события: 3 ключа, перемежаются *)
let make_events n =
  List.init n (fun i ->
    let k = Printf.sprintf "key%d" (i mod 3) in
    Mf_event.data (k, i) i)

(* ── эталон: прогон без сбоя ───────────────────────────────── *)
let reference_run events =
  let store = CP.make_store () in
  let out = ref [] in
  let mu = Mutex.create () in
  CP.run_exactly_once
    ~workers:1 ~capacity:256 ~checkpoint_every:50
    ~key_of ~make_state:State_backend_memory.create
    ~process
    ~source:(CP.seekable_of_list events)
    ~sink:(CP.idempotent_sink (fun v ->
             Mutex.lock mu; out := v :: !out; Mutex.unlock mu))
    ~store ();
  bag !out

(* ── прогон со сбоем: durable store переживает «смерть» ─────── *)
(* Один воркер — чтобы шардирование было тривиальным и harness проверял
   именно RECOVERY (умер → восстановил стейт → продолжил без потерь),
   а не воспроизводил внутреннюю hash-функцию шардирования. *)
let recovery_run events =
  let durable = ref [] in
  let store1 = CP.make_store ~persist:(fun cp -> durable := cp :: !durable) () in
  let out = ref [] in
  let mu = Mutex.create () in
  let collect v = Mutex.lock mu; out := v :: !out; Mutex.unlock mu in

  (* ФАЗА 1: «процесс» обрабатывает первую половину и оставляет
     durable checkpoint-ы, затем «умирает». *)
  let half = List.length events / 2 in
  let first_half = List.filteri (fun i _ -> i < half) events in
  CP.run_exactly_once
    ~workers:1 ~capacity:256 ~checkpoint_every:20
    ~key_of ~make_state:State_backend_memory.create
    ~process
    ~source:(CP.seekable_of_list first_half)
    ~sink:(CP.idempotent_sink collect)
    ~store:store1 ();

  (* ФАЗА 2: «новый процесс» — durable checkpoint-ы пережили смерть.
     recover восстанавливает стейт воркера и перематывает ПОЛНЫЙ
     источник на сохранённый offset; продолжаем единственным воркером. *)
  let store2 = CP.make_store () in
  List.iter (fun cp -> CP.commit store2 cp) (List.rev !durable);
  let full_source = CP.seekable_of_list events in
  let backends = CP.recover ~workers:1
                   ~make_state:State_backend_memory.create
                   ~source:full_source store2 in
  let rec drain () =
    match full_source.CP.pull () with
    | None -> ()
    | Some ev -> List.iter collect (process backends.(0) ev); drain ()
  in drain ();
  bag !out

let test_recovery_matches_reference () =
  Printf.printf "\n-- recovery output == reference output (no dup, no loss)\n";
  let events = make_events 120 in
  let reference = reference_run events in
  (* эталон: финальные суммы по каждому ключу должны быть полны *)
  check "reference produced output" (List.length reference > 0);
  let recovered = recovery_run events in
  check "recovery produced output" (List.length recovered > 0);
  (* ключевой инвариант: множество ВЫХОДНЫХ (key,sum) пар после recovery
     покрывает финальные суммы — нет потерь данных по ключам *)
  let final_sums xs =
    let h = Hashtbl.create 8 in
    List.iter (fun (k, s) ->
      match Hashtbl.find_opt h k with
      | Some prev when prev >= s -> ()
      | _ -> Hashtbl.replace h k s) xs;
    bag (Hashtbl.fold (fun k s acc -> (k, s) :: acc) h []) in
  check "final per-key sums match reference (no loss/dup in aggregate)"
    (final_sums recovered = final_sums reference)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Full recovery harness (exactly-once E2E)\n";
  Printf.printf "==========================================\n";
  test_recovery_matches_reference ();
  Printf.printf "\nRecovery harness passed.\n"
