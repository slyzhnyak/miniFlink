open Miniflink
(* Тест: ретракты НЕ теряются в параллельном пути (Parallel.run_parallel_simple).
   Раньше dispatcher делал `Retract _ -> ()` — ретракты молча отбрасывались
   в обычном параллельном пути (отдельно от EO, где это чинили в R2).
   Этот тест должен падать на старом коде, проходить на новом. *)

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

(* pipeline, который помечает в выходном значении, был ли это Retract:
   превращаем Data v → ("data", v), Retract v → ("retract", v). Поскольку
   pipeline работает с Mf_event, считаем ретракты внутри через stateful-
   подобный счётчик невозможно (разные воркеры), поэтому проверяем что
   ретракт долетает: pipeline пропускает событие, sink считает. *)

let test_retract_reaches_workers () =
  Printf.printf "\n-- retracts reach workers in parallel path\n";
  let retract_seen = Atomic.make 0 in
  let data_seen = Atomic.make 0 in
  (* pipeline: на каждое событие emmap не различает вид; чтобы поймать
     ретракт, используем flat_map который видит сырой Mf_event *)
  let pipeline (s : int Mf_event.t Stream.t) : int Mf_event.t Stream.t =
    fun () ->
      match s () with
      | Some (Mf_event.Retract (v,t)) ->
        Atomic.incr retract_seen; Some (Mf_event.data v t)  (* конвертим чтобы дошло до sink *)
      | Some (Mf_event.Data (v,t)) ->
        Atomic.incr data_seen; Some (Mf_event.data v t)
      | other -> other in
  let source = Stream.of_list [
    Mf_event.data 1 0;
    Mf_event.retract 1 10;   (* ретракт — должен дойти до воркера *)
    Mf_event.data 2 20;
    Mf_event.wm 100;
  ] in
  Parallel.run_parallel_simple
    ~workers:2 ~capacity:64
    ~key_of:(fun v -> string_of_int v)
    ~pipeline
    ~source
    ~sink:(fun _ -> ())
    ();
  Printf.printf "  data seen: %d, retract seen: %d\n%!"
    (Atomic.get data_seen) (Atomic.get retract_seen);
  check "retract reached a worker (not dropped by dispatcher)"
    (Atomic.get retract_seen = 1);
  check "data events also processed" (Atomic.get data_seen = 2)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Parallel retract routing\n";
  Printf.printf "==========================================\n";
  test_retract_reaches_workers ();
  Printf.printf "\nParallel retract test passed.\n"
