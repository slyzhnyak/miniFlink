(** Покрытие веток persistence-инфраструктуры:
    - Runtime_context.codec_for на Registry (schema-evolution path);
    - Managed_state.mem / fold (публичный API, не задет happy-path);
    - window invalid_arg (tumbling/sliding с неположительным размером). *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Persistence infra coverage\n";
  Printf.printf "==========================================\n";

  (* ── 1. Runtime_context.codec_for: Registry path ───────────── *)
  Printf.printf "\n-- 1. Registry codec_for\n";
  let custom : Runtime_context.codec = {
    to_bytes = (fun o -> Bytes.of_string ("X" ^ Marshal.to_string o []));
    of_bytes = (fun b ->
      let s = Bytes.to_string b in
      Marshal.from_string (String.sub s 1 (String.length s - 1)) 0);
  } in
  let policy = Runtime_context.Registry (fun name ->
    if name = "known" then Some custom else None) in
  (* Registry возвращает наш codec для known *)
  let c1 = Runtime_context.codec_for policy "known" in
  let b = c1.to_bytes (Obj.repr 42) in
  check "Registry: known → custom codec used (X-prefix)"
    (Bytes.length b > 0 && Bytes.get b 0 = 'X');
  (* Registry для неизвестного имени → fallback на Marshal *)
  let c2 = Runtime_context.codec_for policy "unknown" in
  let b2 = c2.to_bytes (Obj.repr 7) in
  check "Registry: unknown → Marshal fallback (no X-prefix)"
    (Bytes.length b2 = 0 || Bytes.get b2 0 <> 'X');
  (* Marshal_codec policy *)
  let c3 = Runtime_context.codec_for Runtime_context.Marshal_codec "any" in
  check "Marshal_codec: round-trips int"
    (Obj.obj (c3.of_bytes (c3.to_bytes (Obj.repr 99))) = 99);

  (* Registry codec реально применяется durable-оператором: значение
     должно round-trip'иться через custom codec при restore. *)
  Printf.printf "\n-- 2. Registry codec round-trips through Managed_state\n";
  let tbl = Hashtbl.create 16 in
  let backend = Persistence_backend.of_memory tbl in
  let ctx = Runtime_context.durable
    ~codecs:(Runtime_context.Registry (fun _ -> Some custom)) backend in
  Runtime_context.with_context ctx (fun () ->
    let st = Managed_state.create_string ~name:"reg_test" () in
    Managed_state.set st "k" 12345;
    Managed_state.checkpoint st);
  (* записанные байты должны иметь X-префикс (наш codec) *)
  let has_x = Hashtbl.fold (fun _ v acc -> acc || (Bytes.length v > 0 && Bytes.get v 0 = 'X'))
                tbl false in
  check "durable write used custom codec (X-prefix in backend)" has_x;
  (* restore через тот же codec возвращает значение *)
  let restored = Runtime_context.with_context ctx (fun () ->
    let st = Managed_state.create_string ~name:"reg_test" () in
    Managed_state.get st "k") in
  check "restore via custom codec returns 12345" (restored = Some 12345);

  (* ── 3. Managed_state.mem / fold ───────────────────────────── *)
  Printf.printf "\n-- 3. Managed_state mem / fold (ephemeral)\n";
  Runtime_context.with_context Runtime_context.ephemeral (fun () ->
    let st = Managed_state.create_string ~name:"api_test" () in
    Managed_state.set st "a" 1;
    Managed_state.set st "b" 2;
    Managed_state.set st "c" 3;
    check "mem: present key" (Managed_state.mem st "a");
    check "mem: absent key" (not (Managed_state.mem st "zzz"));
    let sum = Managed_state.fold st (fun _k v acc -> acc + v) 0 in
    check "fold: sum of values = 6" (sum = 6);
    check "size = 3" (Managed_state.size st = 3));

  (* ── 4. window invalid_arg ─────────────────────────────────── *)
  Printf.printf "\n-- 4. window spec validation\n";
  check "tumbling 0 → invalid_arg"
    (try ignore (Pipe.tumbling 0); false with Invalid_argument _ -> true);
  check "tumbling -5 → invalid_arg"
    (try ignore (Pipe.tumbling (-5)); false with Invalid_argument _ -> true);
  check "sliding 0 _ → invalid_arg"
    (try ignore (Pipe.sliding 0 10); false with Invalid_argument _ -> true);
  check "sliding _ 0 → invalid_arg"
    (try ignore (Pipe.sliding 10 0); false with Invalid_argument _ -> true);

  Printf.printf "\nPersistence infra coverage passed.\n"
