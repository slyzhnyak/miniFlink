(** Тест: Table.build — eager preload таблицы из потока. *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

type config = {
  cfg_id     : string;
  threshold  : float;
}

let () =
  Printf.printf "Test: Table.build\n%!";

  (* ── 1. Базовое использование ─────────────────────── *)
  Printf.printf "\n-- 1. Basic preload\n";
  let configs = [
    Mf_event.data { cfg_id = "ch4";  threshold = 2.0 } 0;
    Mf_event.data { cfg_id = "co2";  threshold = 5000.0 } 100;
    Mf_event.data { cfg_id = "co";   threshold = 50.0 } 200;
  ] in
  let table = Table.build
    ~key:(fun c -> c.cfg_id)
    (Stream.of_list configs) in
  check "ch4 found"  (match table "ch4" with
    | Some c -> c.threshold = 2.0 | None -> false);
  check "co2 found"  (match table "co2" with
    | Some c -> c.threshold = 5000.0 | None -> false);
  check "co found"   (match table "co" with
    | Some c -> c.threshold = 50.0 | None -> false);
  check "unknown gas → None" (table "h2s" = None);

  (* ── 2. Последнее значение по ключу wins ──────────── *)
  Printf.printf "\n-- 2. Last value per key wins\n";
  let configs = [
    Mf_event.data { cfg_id = "x"; threshold = 1.0 } 0;
    Mf_event.data { cfg_id = "x"; threshold = 2.0 } 1000;
    Mf_event.data { cfg_id = "x"; threshold = 3.0 } 2000;
  ] in
  let table = Table.build
    ~key:(fun c -> c.cfg_id)
    (Stream.of_list configs) in
  check "x = 3.0 (last value)"
    (match table "x" with Some c -> c.threshold = 3.0 | None -> false);

  (* ── 3. Watermark/Retract игнорируются ────────────── *)
  Printf.printf "\n-- 3. Watermark and Retract ignored\n";
  let events = [
    Mf_event.data { cfg_id = "a"; threshold = 1.0 } 0;
    Mf_event.wm 1000;
    Mf_event.retract { cfg_id = "a"; threshold = 1.0 } 1500;
    Mf_event.data { cfg_id = "b"; threshold = 2.0 } 2000;
  ] in
  let table = Table.build
    ~key:(fun c -> c.cfg_id)
    (Stream.of_list events) in
  check "a present (Retract ignored)"
    (match table "a" with Some c -> c.threshold = 1.0 | None -> false);
  check "b present"
    (match table "b" with Some c -> c.threshold = 2.0 | None -> false);

  (* ── 4. Пустой stream ─────────────────────────────── *)
  Printf.printf "\n-- 4. Empty stream\n";
  let table = Table.build
    ~key:(fun c -> c.cfg_id)
    Stream.empty in
  check "empty → all lookups None"
    (table "anything" = None);

  Printf.printf "\nTest passed.\n"
