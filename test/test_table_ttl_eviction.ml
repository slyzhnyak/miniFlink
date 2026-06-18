(** Тест: Table.of_stream_ttl корректно эвиктит просроченные записи,
    включая случай когда продвижение времени происходит через
    Watermark, а не через Data. *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let () =
  Printf.printf "Test: Table.of_stream_ttl + Watermark eviction\n%!";

  (* ── 1. Старая запись эвиктится когда Watermark продвигает время ── *)
  Printf.printf "\n-- 1. Old entry evicted via Watermark\n";
  let events = [
    Mf_event.data ("A", "value_A") 0;       (* at t=0 *)
    Mf_event.data ("B", "value_B") 100;     (* at t=100 *)
    Mf_event.wm 10_000;                     (* time advanced to 10s *)
    (* TTL = 5000. Записи в t=0 и t=100 теперь просрочены *)
  ] in
  let src = Stream.of_list events in
  let table =
    Table.of_stream_ttl ~key:fst ~ttl:5000 src in
  (* Lookup — триггерит pump + evict *)
  let a = table "A" in
  let b = table "B" in
  Printf.printf "  A: %s\n" (match a with Some _ -> "Some" | None -> "None");
  Printf.printf "  B: %s\n" (match b with Some _ -> "Some" | None -> "None");
  check "A evicted (was at t=0, ttl=5000, now=10000)"
    (a = None);
  check "B evicted (was at t=100, ttl=5000, now=10000)"
    (b = None);

  (* ── 2. Свежие записи остаются ────────────────────── *)
  Printf.printf "\n-- 2. Fresh entries survive\n";
  let events = [
    Mf_event.data ("X", "old") 0;
    Mf_event.wm 6000;
    Mf_event.data ("X", "new") 7000;
    Mf_event.wm 8000;
  ] in
  let src = Stream.of_list events in
  let table = Table.of_stream_ttl ~key:fst ~ttl:5000 src in
  let result = table "X" in
  Printf.printf "  X = %s\n" (match result with
    | Some (_, v) -> v | None -> "None");
  check "X survives with 'new' (t=7000 > 8000-5000)"
    (result = Some ("X", "new"));

  (* ── 3. Без Watermarks — старое поведение сохранено ── *)
  Printf.printf "\n-- 3. Backwards compat: no Watermarks\n";
  let events = [
    Mf_event.data ("A", "a") 0;
    Mf_event.data ("B", "b") 1000;
    (* max_ts продвигается через Data → 1000 *)
  ] in
  let src = Stream.of_list events in
  let table = Table.of_stream_ttl ~key:fst ~ttl:5000 src in
  let a = table "A" in
  let b = table "B" in
  check "A present (Data-driven max_ts, not evicted)"
    (a = Some ("A", "a"));
  check "B present"
    (b = Some ("B", "b"));

  Printf.printf "\nTest passed.\n"
