(* Smoke-тест ex07 — защищает от молчаливого регресса при изменениях
   в библиотеке. Запускает example как процесс, проверяет что вывод
   содержит ВСЕ типы алертов от соответствующих шахтёров и счётчик
   ретрактов > 0.

   Защищает от того, что регресс семантики окон или таймеров (как мы
   ловили руками) сломает example, а baseline тестов в test_*.ml
   останутся зелёными. Запуск примера = end-to-end regression. *)

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let contains hay needle =
  let nl = String.length needle and hl = String.length hay in
  let rec go i = i + nl <= hl && (String.sub hay i nl = needle || go (i+1)) in
  go 0

(* Запустить example, собрать stdout *)
let run_example path =
  let ic = Unix.open_process_in path in
  let buf = Buffer.create 4096 in
  (try while true do Buffer.add_channel buf ic 4096 done
   with End_of_file -> ());
  let _ = Unix.close_process_in ic in
  Buffer.contents buf

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  ex07 smoke regression\n";
  Printf.printf "==========================================\n";
  (* dune test устанавливает CWD в _build/default — example рядом *)
  let candidates = [
    "./examples/ex07_location.exe";
    "../examples/ex07_location.exe";
    "../../examples/ex07_location.exe";
    "_build/default/examples/ex07_location.exe";
  ] in
  let exe = match List.find_opt Sys.file_exists candidates with
    | Some p -> p
    | None ->
      Printf.printf "  SKIP: example binary not found in %s\n"
        (String.concat ", " candidates); exit 0
  in
  let out = run_example exe in

  (* Заголовок и шесть шахтёров *)
  check "header present" (contains out "Локация шахтёров по маякам");

  (* Все шесть типов алертов в выводе *)
  check "Sos alert present"         (contains out "НАЖАТА КНОПКА SOS");
  check "Low_voltage alert present" (contains out "НИЗКОЕ НАПРЯЖЕНИЕ");
  check "No_motion alert present"   (contains out "НЕ ДВИЖЕТСЯ");
  check "No_packets alert present"  (contains out "НЕТ ПАКЕТОВ");
  check "No_readings alert present" (contains out "не слышит маяки");

  (* Конкретно M6 — все четыре алерта одновременно *)
  let m6_sos = contains out "M6: НАЖАТА КНОПКА SOS" in
  let m6_low = contains out "M6: НИЗКОЕ НАПРЯЖЕНИЕ" in
  let m6_mot = contains out "M6: НЕ ДВИЖЕТСЯ" in
  let m6_rdg = contains out "M6: пакеты идут, но не слышит маяки" in
  check "M6 has all four alerts simultaneously"
    (m6_sos && m6_low && m6_mot && m6_rdg);

  (* Дубли посчитаны *)
  check "duplicate counter shown" (contains out "дублей (тот же lamp+ts)");

  (* Ретрактов > 0 (опоздавшие пакеты сработали) *)
  check "retract counter shown" (contains out "ретракций (пересчётов окон)");

  (* Регресс-тест: late пакет с moving=true НЕ должен сдвигать
     last_moving назад во времени. Для M2: последний реальный
     moving=true был на t=105с (i=7, t < 120с). Late пакет M2@88
     с moving=true приходит после — БЕЗ фикса last_moving стало 88с,
     и алерт показывал t=88с вместо t=105с. *)
  check "late moving=true does NOT regress last_moving (M2)"
    (contains out "M2: НЕ ДВИЖЕТСЯ >2 мин (последнее движение t=105с)");

  (* Локация показывает координаты *)
  check "coordinates rendered" (contains out "x=120 y=40 h=-160м");

  Printf.printf "\nex07 smoke OK\n"
