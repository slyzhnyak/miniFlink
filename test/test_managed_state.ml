(** Тест фундамента ортогональной persistence: managed-state
    работает идентично в ephemeral и durable, durable переживает
    «рестарт». *)
open Miniflink

let pass n = Printf.printf "  OK %s\n%!" n
let fail n = Printf.printf "  FAIL %s\n%!" n; exit 1
let check n c = if c then pass n else fail n

let () =
  Printf.printf "Managed_state: ephemeral vs durable\n%!";

  (* ── 1. Ephemeral: обычная keyed-map ── *)
  Printf.printf "\n-- 1. ephemeral basics\n";
  let st = Managed_state.create_string ~name:"counters" () in
  Managed_state.set st "a" 1;
  Managed_state.set st "b" 2;
  check "get a = 1" (Managed_state.get st "a" = Some 1);
  check "size = 2" (Managed_state.size st = 2);
  Managed_state.checkpoint st;  (* noop в ephemeral, не должен падать *)
  check "checkpoint in ephemeral is noop-safe" true;

  (* ── 2. Durable: тот же код, но в durable-контексте ── *)
  Printf.printf "\n-- 2. durable persists on checkpoint\n";
  let tbl = Hashtbl.create 16 in
  let backend = Persistence_backend.of_memory tbl in
  let ctx = Runtime_context.durable backend in
  Runtime_context.with_context ctx (fun () ->
    let st = Managed_state.create_string ~name:"counters" () in
    Managed_state.set st "x" 10;
    Managed_state.set st "y" 20;
    Managed_state.checkpoint st;
    check "durable: backend got 2 keys" (List.length (backend.Persistence_backend.keys ()) = 2));

  (* ── 3. «Рестарт»: новый state из того же backend видит данные ── *)
  Printf.printf "\n-- 3. restart: new state restores from backend\n";
  Runtime_context.with_context ctx (fun () ->
    let st2 = Managed_state.create_string ~name:"counters" () in
    check "restored x = 10" (Managed_state.get st2 "x" = Some 10);
    check "restored y = 20" (Managed_state.get st2 "y" = Some 20);
    check "restored size = 2" (Managed_state.size st2 = 2));

  (* ── 4. Namespace изолирует разные операторы ── *)
  Printf.printf "\n-- 4. namespaces don't collide\n";
  Runtime_context.with_context ctx (fun () ->
    let a = Managed_state.create_string ~name:"opA" () in
    let b = Managed_state.create_string ~name:"opB" () in
    Managed_state.set a "k" 100;
    Managed_state.set b "k" 200;
    Managed_state.checkpoint a;
    Managed_state.checkpoint b;
    let a2 = Managed_state.create_string ~name:"opA" () in
    let b2 = Managed_state.create_string ~name:"opB" () in
    check "opA k = 100 (not 200)" (Managed_state.get a2 "k" = Some 100);
    check "opB k = 200 (not 100)" (Managed_state.get b2 "k" = Some 200));

  (* ── 5. evict убирает из памяти и backend ── *)
  Printf.printf "\n-- 5. evict removes from backend too\n";
  Runtime_context.with_context ctx (fun () ->
    let st = Managed_state.create_string ~name:"ev" () in
    Managed_state.set st "gone" 1;
    Managed_state.checkpoint st;
    Managed_state.evict st "gone";
    let st2 = Managed_state.create_string ~name:"ev" () in
    check "evicted key not restored" (Managed_state.get st2 "gone" = None));

  Printf.printf "\nManaged_state foundation OK.\n"
