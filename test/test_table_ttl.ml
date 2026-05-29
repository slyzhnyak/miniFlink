(* Тест 1.2: state eviction в table через TTL *)
open Time

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

(* Простой тип: (id, ts) *)
let ev id t = Mf_event.data (id, t) t
let key (id, _) = id

let test_ttl_eviction () =
  Printf.printf "\n-- of_stream_ttl evicts stale keys\n";
  (* События с уникальными ключами, растущее время.
     TTL=10s: ключи старше (max_ts - 10s) удаляются. *)
  let events = [
    ev "k1" 1000;
    ev "k2" 2000;
    ev "k3" 50000;   (* max_ts прыгает до 50s → k1,k2 (< 40s) устаревают *)
  ] in
  let tbl = Table.of_stream_ttl ~key ~ttl:(seconds 10)
    (Stream.of_list events) in
  (* Первый lookup осушает источник и применяет eviction *)
  let k3 = tbl "k3" in
  let k1 = tbl "k1" in
  let k2 = tbl "k2" in
  check "fresh key k3 present" (k3 <> None);
  check "stale key k1 evicted" (k1 = None);
  check "stale key k2 evicted" (k2 = None)

let test_ttl_keeps_fresh () =
  Printf.printf "\n-- of_stream_ttl keeps within-TTL keys\n";
  let events = [
    ev "a" 1000;
    ev "b" 5000;
    ev "c" 9000;   (* max=9s, ttl=10s → ничего не устаревает (все > -1s) *)
  ] in
  let tbl = Table.of_stream_ttl ~key ~ttl:(seconds 10) (Stream.of_list events) in
  let _ = tbl "c" in
  check "a present (within ttl)" (tbl "a" <> None);
  check "b present (within ttl)" (tbl "b" <> None);
  check "c present" (tbl "c" <> None)

let test_of_stream_bounded_by_keys () =
  Printf.printf "\n-- of_stream: size bounded by unique keys, not events\n";
  (* 1000 событий, но только 3 ключа → таблица хранит 3 *)
  let events = List.init 1000 (fun i ->
    ev (Printf.sprintf "k%d" (i mod 3)) (i * 100)) in
  let tbl = Table.of_stream ~key (Stream.of_list events) in
  let _ = tbl "k0" in
  (* все 3 ключа на месте, значение — последнее *)
  check "k0 present" (tbl "k0" <> None);
  check "k1 present" (tbl "k1" <> None);
  check "k2 present" (tbl "k2" <> None)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Table TTL eviction (roadmap 1.2)\n";
  Printf.printf "==========================================\n";
  test_ttl_eviction ();
  test_ttl_keeps_fresh ();
  test_of_stream_bounded_by_keys ();
  Printf.printf "\nAll table TTL tests passed.\n"
