(** Тест: Supervisor.supervise_result не теряет результаты при
    параллельном завершении многих пайплайнов.

    Проверка корректности: после fix'а data race все результаты
    должны быть на своих местах с правильными label.

    Раньше (без mutex) этот тест прошёл бы случайно за счёт GIL
    в Thread, но это была удача, а не корректность. *)

open Miniflink

let () =
  Printf.printf "Test: Supervisor parallel completion — no lost results\n%!";

  (* 20 пайплайнов которые быстро завершаются, каждый со своим
     уникальным label. После supervise_result в результатах должны
     быть ВСЕ 20 уникальных label'ов. *)
  let n = 20 in
  let specs = List.init n (fun i ->
    let label = Printf.sprintf "pipeline_%02d" i in
    Supervisor.{
      label;
      run = (fun () ->
        (* Лёгкая работа чтобы потоки могли реально пересечься *)
        let acc = ref 0 in
        for j = 1 to 100 do acc := !acc + j done;
        ignore !acc);
      on_failure = Supervisor.Isolate;
    }
  ) in

  (* Запускаем несколько раз чтобы повысить шанс race detection
     если он есть *)
  for run = 1 to 10 do
    let results = Supervisor.supervise_result specs in

    if List.length results <> n then begin
      Printf.printf "  FAIL run %d: got %d results, expected %d\n%!"
        run (List.length results) n;
      exit 1
    end;

    (* Все label'ы должны быть в результатах *)
    let expected = List.init n (fun i ->
      Printf.sprintf "pipeline_%02d" i) in
    let got = List.map fst results in
    let got_sorted = List.sort compare got in
    let expected_sorted = List.sort compare expected in

    if got_sorted <> expected_sorted then begin
      Printf.printf "  FAIL run %d: labels mismatch\n" run;
      Printf.printf "    got:      %s\n" (String.concat "," got_sorted);
      Printf.printf "    expected: %s\n" (String.concat "," expected_sorted);
      exit 1
    end;

    (* Все статусы должны быть `Ok *)
    let all_ok = List.for_all (fun (_, st) -> st = `Ok) results in
    if not all_ok then begin
      Printf.printf "  FAIL run %d: not all `Ok\n" run;
      exit 1
    end
  done;

  Printf.printf "  OK 10 runs × %d pipelines, all results correct\n" n;
  Printf.printf "Test passed.\n"
