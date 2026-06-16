(** Тесты для {!Pipe.keyed_join}.

    Структура:
    1. Один stream — просто пересылает в формате (k, [Some v])
    2. Два streams, разные ключи — каждый сам по себе
    3. Два streams, общий ключ — snapshot обновляется
    4. Три streams — N-streams работает
    5. Пустой list streams — Stream.empty
    6. Stream с Retract — игнорируется
    7. Watermarks — синхронизированы (min) *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

(* Простой типа: (key, value). Keyed по строковому ключу. *)
type kv = string * float

module By_key : Keyed.S with type t = kv = struct
  type t = kv
  let key (k, _) = k
end

(* Удобный collect: собирает только Data в (key, options list). *)
let collect_data stream =
  let acc = ref [] in
  let rec drain () = match stream () with
    | None -> ()
    | Some (Mf_event.Data (v, _)) -> acc := v :: !acc; drain ()
    | Some _ -> drain ()
  in drain ();
  List.rev !acc

let pp_options opts =
  String.concat ";"
    (List.map (function
      | None -> "-"
      | Some (_, f) -> Printf.sprintf "%.0f" f) opts)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Pipe.keyed_join\n";
  Printf.printf "==========================================\n";

  (* ── 1. Один stream — proxy для consistency ──────────── *)
  Printf.printf "\n-- 1. Single stream\n";
  let s1 = [Mf_event.data ("A", 10.0) 0; Mf_event.data ("A", 20.0) 1000]
           |> Stream.of_list in
  let joined = Pipe.keyed_join (module By_key) [s1] in
  let result = collect_data joined in
  check (Printf.sprintf "got %d emissions" (List.length result))
    (List.length result = 2);
  let (k1, opts1) = List.hd result in
  check (Printf.sprintf "first key=%s" k1) (k1 = "A");
  check (Printf.sprintf "first options=[%s]" (pp_options opts1))
    (opts1 = [Some ("A", 10.0)]);

  (* ── 2. Два streams, разные ключи ─────────────────────── *)
  Printf.printf "\n-- 2. Two streams, disjoint keys\n";
  let s1 = [Mf_event.data ("A", 1.0) 0] |> Stream.of_list in
  let s2 = [Mf_event.data ("B", 2.0) 100] |> Stream.of_list in
  let joined = Pipe.keyed_join (module By_key) [s1; s2] in
  let result = collect_data joined in
  Printf.printf "  got %d emissions\n" (List.length result);
  List.iter (fun (k, opts) ->
    Printf.printf "  %s: [%s]\n" k (pp_options opts)) result;
  check "A: stream1 has value, stream2 doesn't"
    (List.exists (fun (k, opts) ->
       k = "A" && opts = [Some ("A", 1.0); None]) result);
  check "B: stream1 doesn't, stream2 has value"
    (List.exists (fun (k, opts) ->
       k = "B" && opts = [None; Some ("B", 2.0)]) result);

  (* ── 3. Два streams, общий ключ — snapshot обновляется ── *)
  Printf.printf "\n-- 3. Two streams, shared key\n";
  let s1 = [
    Mf_event.data ("X", 1.0) 0;     (* X из s1 *)
    Mf_event.data ("X", 3.0) 2000;  (* X из s1 обновился *)
  ] |> Stream.of_list in
  let s2 = [
    Mf_event.data ("X", 2.0) 1000;  (* X из s2 пришёл между *)
  ] |> Stream.of_list in
  let joined = Pipe.keyed_join (module By_key) [s1; s2] in
  let result = collect_data joined in
  (* Order по ts: 0, 1000, 2000 *)
  Printf.printf "  got %d emissions:\n" (List.length result);
  List.iter (fun (k, opts) ->
    Printf.printf "  %s: [%s]\n" k (pp_options opts)) result;
  check "3 emissions" (List.length result = 3);
  (match result with
   | (_, [Some _; None])      :: (_, [Some _; Some _]) :: (_, [Some _; Some _]) :: [] ->
     pass "snapshots: (s1=1,s2=-) → (s1=1,s2=2) → (s1=3,s2=2)"
   | _ -> fail "unexpected snapshot order");

  (* ── 4. Три streams — multi-source ─────────────────────── *)
  Printf.printf "\n-- 4. Three streams\n";
  let voltage = [Mf_event.data ("M1", 3.8) 0] |> Stream.of_list in
  let co      = [Mf_event.data ("M1", 50.0) 1000] |> Stream.of_list in
  let rssi    = [Mf_event.data ("M1", -75.0) 2000] |> Stream.of_list in
  let joined = Pipe.keyed_join (module By_key) [voltage; co; rssi] in
  let result = collect_data joined in
  check "3 emissions for M1" (List.length result = 3);
  (* Последний snapshot должен иметь все 3 значения *)
  let (_, last_opts) = List.nth result 2 in
  check "last snapshot has all Some"
    (List.for_all Option.is_some last_opts);
  check "last snapshot list length = 3"
    (List.length last_opts = 3);

  (* ── 5. Пустой list streams ────────────────────────────── *)
  Printf.printf "\n-- 5. Empty streams list\n";
  let joined = Pipe.keyed_join (module By_key) [] in
  check "empty join" (joined () = None);

  (* ── 6. Stream с Retract игнорируется ───────────────────── *)
  Printf.printf "\n-- 6. Retract is ignored\n";
  let s1 = [
    Mf_event.data ("K", 1.0) 0;
    Mf_event.retract ("K", 1.0) 100;   (* должен игнорироваться *)
    Mf_event.data ("K", 5.0) 200;
  ] |> Stream.of_list in
  let joined = Pipe.keyed_join (module By_key) [s1] in
  let result = collect_data joined in
  Printf.printf "  got %d emissions (expect 2, retract ignored)\n"
    (List.length result);
  check "retract ignored: 2 Data emissions"
    (List.length result = 2);

  (* ── 7. Watermark проходит через ──────────────────────── *)
  Printf.printf "\n-- 7. Watermark passes through\n";
  let s1 = [
    Mf_event.data ("A", 1.0) 0;
    Mf_event.wm 1000;
  ] |> Stream.of_list in
  let s2 = [
    Mf_event.data ("B", 2.0) 500;
    Mf_event.wm 1500;
  ] |> Stream.of_list in
  let joined = Pipe.keyed_join (module By_key) [s1; s2] in
  let wm_count = ref 0 in
  joined |> Pipe.iter_events (fun ev ->
    match ev with
    | Mf_event.Watermark _ -> incr wm_count
    | _ -> ());
  check (Printf.sprintf "got %d watermarks (>= 1)" !wm_count)
    (!wm_count >= 1);

  Printf.printf "\nAll keyed_join tests passed.\n"
