open Miniflink
(* Общий keyed-state с TTL: записи автоматически истекают через ttl
   (по event-time) после последнего обновления. Обобщение TTL, который
   был только у Table, на произвольное состояние оператора. *)

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let test_basic_put_get () =
  Printf.printf "\n-- put/get within ttl\n";
  let s = Ttl_state.create ~ttl:100 () in
  Ttl_state.put s ~now:0 "a" 1;
  Ttl_state.put s ~now:10 "b" 2;
  check "get a within ttl" (Ttl_state.get s ~now:50 "a" = Some 1);
  check "get b within ttl" (Ttl_state.get s ~now:50 "b" = Some 2);
  check "get unknown = None" (Ttl_state.get s ~now:50 "z" = None)

let test_expiry () =
  Printf.printf "\n-- entries expire after ttl (event-time)\n";
  let s = Ttl_state.create ~ttl:100 () in
  Ttl_state.put s ~now:0 "a" 1;
  (* на now=101 запись 'a' (последнее обновление now=0) истекла: 0+100 < 101 *)
  check "expired entry reads as None" (Ttl_state.get s ~now:101 "a" = None);
  check "still alive at exactly ttl boundary" (Ttl_state.get s ~now:100 "a" = Some 1)

let test_refresh_on_update () =
  Printf.printf "\n-- updating a key refreshes its ttl\n";
  let s = Ttl_state.create ~ttl:100 () in
  Ttl_state.put s ~now:0 "a" 1;
  Ttl_state.put s ~now:80 "a" 2;       (* обновили — ttl считается от 80 *)
  check "alive at now=150 (80+100>150)" (Ttl_state.get s ~now:150 "a" = Some 2);
  check "expired at now=181 (80+100<181)" (Ttl_state.get s ~now:181 "a" = None)

let test_advance_evicts () =
  Printf.printf "\n-- advance bounds memory by evicting expired keys\n";
  let s = Ttl_state.create ~ttl:10 () in
  for i = 0 to 99 do Ttl_state.put s ~now:i (string_of_int i) i done;
  check "100 keys before advance" (Ttl_state.size s = 100);
  Ttl_state.advance s ~now:200;        (* все истекли (i+10 < 200) *)
  check "all evicted after advance past all ttls" (Ttl_state.size s = 0)

let test_get_does_not_resurrect () =
  Printf.printf "\n-- get past ttl returns None even before advance\n";
  let s = Ttl_state.create ~ttl:5 () in
  Ttl_state.put s ~now:0 "a" 1;
  (* без advance: get учитывает now и не отдаёт истёкшее *)
  check "logically expired on read" (Ttl_state.get s ~now:10 "a" = None)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  TTL state\n";
  Printf.printf "==========================================\n";
  test_basic_put_get ();
  test_expiry ();
  test_refresh_on_update ();
  test_advance_evicts ();
  test_get_does_not_resurrect ();
  Printf.printf "\nTTL state tests passed.\n"
