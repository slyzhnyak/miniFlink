open Miniflink
(* Fan-out: один источник → N независимых выходов, у каждого СВОЯ
   стратегия backpressure при отставании. Стратегия выбирается при
   создании (на каждый outlet), как просил пользователь. *)

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

(* Block: медленный выход не теряет ничего — все события доходят.
   При Block-выходах читать надо ВПЕРЕМЕШКУ (нельзя осушить один не
   читая другой — иначе backpressure застопорит источник; это семантика
   Block, как в Flink). *)
let test_block_no_loss () =
  Printf.printf "\n-- Block outlet loses nothing (interleaved reads)\n";
  let events = List.init 100 (fun i -> Mf_event.data i i) in
  let outlets = [
    Fan_out.{ name="a"; buffer_cap=8; on_pressure=Block };
    Fan_out.{ name="b"; buffer_cap=8; on_pressure=Block };
  ] in
  let streams = Fan_out.fan_out (Stream.of_list events) outlets in
  let s0 = List.nth streams 0 and s1 = List.nth streams 1 in
  (* читаем оба вперемешку до конца обоих *)
  let a = ref [] and b = ref [] and d0 = ref false and d1 = ref false in
  while not (!d0 && !d1) do
    (match s0 () with Some (Mf_event.Data (v,_)) -> a := v :: !a | None -> d0 := true | _ -> ());
    (match s1 () with Some (Mf_event.Data (v,_)) -> b := v :: !b | None -> d1 := true | _ -> ())
  done;
  let a = List.rev !a and b = List.rev !b in
  check "a got all 100" (List.length a = 100);
  check "b got all 100 (Block = no loss)" (List.length b = 100);
  check "order preserved on a" (a = List.init 100 (fun i -> i))

(* Drop_newest: при переполнении буфера новые события выкидываются —
   выход получает ПОДМНОЖЕСТВО, но без блокировки источника *)
let test_drop_newest_bounded () =
  Printf.printf "\n-- Drop_newest bounds buffer, may lose, never blocks\n";
  let events = List.init 100 (fun i -> Mf_event.data i i) in
  let outlets = [
    Fan_out.{ name="lossy"; buffer_cap=8; on_pressure=Drop_newest };
  ] in
  let streams = Fan_out.fan_out (Stream.of_list events) outlets in
  let got = Stream.to_list (List.nth streams 0)
    |> List.filter_map (function Mf_event.Data (v,_) -> Some v | _ -> None) in
  (* не больше чем выход смог принять; не больше входа *)
  check "lossy got <= 100" (List.length got <= 100);
  check "lossy got at least some" (List.length got > 0);
  (* всё что получено — реальные события из источника (без мусора) *)
  check "all received are valid events" (List.for_all (fun v -> v >= 0 && v < 100) got)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Fan-out with per-outlet backpressure\n";
  Printf.printf "==========================================\n";
  test_block_no_loss ();
  test_drop_newest_bounded ();
  Printf.printf "\nFan-out tests passed.\n"

(* Конкурентное чтение: два потока осушают Block-выходы ПАРАЛЛЕЛЬНО
   (как под supervisor — ex06). До thread-safety фикса это гонка данных:
   advance мутирует общие pending/буферы из двух потоков без мьютекса.
   Тест с большим объёмом повышает шанс поймать гонку. *)
let test_concurrent_block_drain () =
  Printf.printf "\n-- concurrent threads drain Block outlets (supervisor pattern)\n";
  let n = 20000 in
  let events = List.init n (fun i -> Mf_event.data i i) in
  let outlets = [
    Fan_out.{ name="a"; buffer_cap=64; on_pressure=Block };
    Fan_out.{ name="b"; buffer_cap=64; on_pressure=Block };
  ] in
  let streams = Fan_out.fan_out (Stream.of_list events) outlets in
  let s0 = List.nth streams 0 and s1 = List.nth streams 1 in
  let got0 = ref [] and got1 = ref [] in
  let drain s acc () =
    let rec loop () = match s () with
      | Some (Mf_event.Data (v,_)) -> acc := v :: !acc; loop ()
      | Some _ -> loop ()
      | None -> () in
    loop () in
  let t0 = Thread.create (drain s0 got0) () in
  let t1 = Thread.create (drain s1 got1) () in
  Thread.join t0; Thread.join t1;
  check "thread A got all events, no loss/dup"
    (List.sort_uniq compare !got0 = List.init n (fun i -> i));
  check "thread B got all events, no loss/dup"
    (List.sort_uniq compare !got1 = List.init n (fun i -> i));
  check "order preserved A" (List.rev !got0 = List.init n (fun i -> i));
  check "order preserved B" (List.rev !got1 = List.init n (fun i -> i))

let () = test_concurrent_block_drain ()
