(** Тест: исключение в источнике fan_out НЕ оставляет mutex
    заблокированным (без deadlock всех выходов).

    Регрессия для fix/mutex-unlock-safety: advance вызывает
    t.source () под mutex; раньше исключение источника вешало
    весь fan_out. *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let () =
  Printf.printf "Test: fan_out unlock safety on source exception\n%!";

  (* ── 1. Источник бросает на 3-м pull: выход не виснет ── *)
  Printf.printf "\n-- 1. Throwing source doesn't deadlock the outlet\n";
  let n = ref 0 in
  let source : int Mf_event.t Stream.t = fun () ->
    incr n;
    if !n >= 3 then failwith "simulated source failure"
    else Some (Mf_event.data !n 0)
  in
  let outlets = [
    { Fan_out.name = "out1"; buffer_cap = 10;
      on_pressure = Fan_out.Drop_newest };
  ] in
  let streams = Fan_out.fan_out source outlets in
  let s = List.hd streams in

  (* Первые два pull успешны *)
  let v1 = s () in
  let v2 = s () in
  check "first two pulls succeed"
    (match v1, v2 with
     | Some (Mf_event.Data (1, _)), Some (Mf_event.Data (2, _)) -> true
     | _ -> false);

  (* Третий pull: источник бросает. КЛЮЧЕВОЕ: исключение
     проброшено, mutex освобождён. *)
  (try
     let _ = s () in
     fail "expected exception from source"
   with Failure _ -> pass "third pull raised (source exception propagated)");

  (* КЛЮЧЕВАЯ ПРОВЕРКА: mutex освобождён — следующий pull не виснет.
     Если бы mutex остался заблокированным, этот вызов завис бы
     навечно (тест повис бы по таймауту). *)
  (try
     let _ = s () in
     pass "subsequent pull returns (mutex was released)"
   with Failure _ -> pass "subsequent pull raised again (mutex was released)");

  Printf.printf "\nFan_out unlock safety verified.\n"
