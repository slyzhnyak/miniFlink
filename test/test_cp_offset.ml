(** Регрессия §4.1 (FIXED): cp_offset согласован со снапшотом.

    checkpoint_parallel раньше брал cp_offset = сумма processed (число
    Data), тогда как source.seek при recovery ждёт индекс в ПОЛНОМ
    потоке (Data+Watermark+Retract+Update). При watermark-ах между Data
    это расходилось → seek промахивался → повтор/потеря событий.

    Фикс: cp_offset = позиция в потоке, соответствующая числу Data в
    снапшоте (data_positions[snap_processed]). Offset согласован со
    снапшотом независимо от watermark-ов и от того, сколько событий было
    in-flight в момент инжекта барьера.

    Тест проверяет это СТРОГО: для потока с watermark-ами между Data
    число Data в префиксе [0, cp_offset) обязано равняться числу реально
    обработанных Data. Также полный цикл recovery+replay не должен
    терять или дублировать события. *)

open Miniflink
module CP = Checkpoint_parallel

let pass n = Printf.printf "  OK %s\n%!" n
let fail n = Printf.printf "  FAIL %s\n%!" n; exit 1
let check n c = if c then pass n else fail n

let with_timeout secs f =
  Sys.set_signal Sys.sigalrm (Sys.Signal_handle (fun _ ->
    Printf.printf "  FAIL: TIMEOUT\n%!"; exit 1));
  ignore (Unix.alarm secs);
  let r = f () in ignore (Unix.alarm 0); r

let count_process backend ev =
  match ev with
  | Mf_event.Data (key, _) ->
    let cur = match State_backend_memory.get backend key with
      | Some b -> int_of_string (Bytes.to_string b) | None -> 0 in
    State_backend_memory.set backend key
      (Bytes.of_string (string_of_int (cur + 1)));
    [key]
  | _ -> []

(* поток с watermark-ами МЕЖДУ Data — сценарий §4.1 *)
let mk_data_wm n =
  List.concat (List.init n (fun i ->
    [ Mf_event.data (Printf.sprintf "k%d" (i mod 4)) (i * 100);
      Mf_event.wm (i * 100) ]))

(* поток из одних Data — проверяем, что фикс не внёс регресс *)
let mk_data n =
  List.init n (fun i -> Mf_event.data (Printf.sprintf "k%d" (i mod 4)) (i * 10))

let run_recovery_replay events n_expected label =
  let store = CP.make_store () in
  CP.run_exactly_once
    ~workers:4 ~capacity:128 ~checkpoint_every:100
    ~key_of:(fun k -> k)
    ~make_state:State_backend_memory.create
    ~process:count_process
    ~source:(CP.seekable_of_list events)
    ~sink:(CP.idempotent_sink (fun _ -> ()))
    ~store ();
  (match CP.latest_checkpoint store with
   | None -> ()
   | Some cp ->
     (* Новый контракт (KI-1 fix, Chandy-Lamport): cp_offset = позиция
        инжекта барьера, и снапшот покрывает ровно префикс [0, cp_offset).
        Проверяем прямой инвариант: суммарное состояние в снапшоте =
        число Data в префиксе. Раньше тут стояло data_in_prefix =
        snap_processed — это держалось на старом контракте (offset
        привязан к data-count через data_positions); с per-epoch
        alignment offset больше не реконструируется из processed. *)
     let arr = Array.of_list events in
     let data_in_prefix =
       let c = ref 0 in
       for i = 0 to min cp.CP.cp_offset (Array.length arr) - 1 do
         (match arr.(i) with Mf_event.Data _ -> incr c | _ -> ())
       done; !c in
     let snap_state =
       Array.fold_left (fun a s ->
         let b = s.CP.state in
         (* снапшот воркера — Marshal его backend; считаем сумму значений *)
         a + (let tmp = State_backend_memory.create () in
              State_backend_memory.restore tmp b;
              List.fold_left (fun acc k ->
                match State_backend_memory.get tmp k with
                | Some v -> acc + int_of_string (Bytes.to_string v)
                | None -> acc) 0 (State_backend_memory.keys tmp)))
         0 cp.CP.cp_snapshots in
     check (label ^ ": снапшот покрывает ровно префикс [0, cp_offset)")
       (snap_state = data_in_prefix));
  (* полный цикл recovery + replay = ровно n, без потерь/дублей *)
  let src2 = CP.seekable_of_list events in
  let backends = CP.recover ~workers:4
    ~make_state:State_backend_memory.create ~source:src2 store in
  let rec replay () = match src2.CP.pull () with
    | None -> ()
    | Some ev ->
      (match ev with Mf_event.Data (k, _) ->
        ignore (count_process backends.(CP.hash_key k 4) ev) | _ -> ());
      replay () in
  replay ();
  let total = Array.fold_left (fun acc b ->
    List.fold_left (fun a k -> match State_backend_memory.get b k with
      | Some v -> a + int_of_string (Bytes.to_string v) | None -> a)
      acc (State_backend_memory.keys b)) 0 backends in
  check (label ^ Printf.sprintf ": recovery+replay = %d (без потерь/дублей)" n_expected)
    (total = n_expected)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  §4.1 cp_offset: snapshot-aligned offset\n";
  Printf.printf "==========================================\n";
  with_timeout 30 (fun () ->
    run_recovery_replay (mk_data_wm 400) 400 "Data+WM";
    run_recovery_replay (mk_data 2000) 2000 "all-Data");
  Printf.printf "\n§4.1 regression passed.\n"
