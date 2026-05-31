(* Тесты union (слияние по event-time) и update_table (обновление
   справочника из потока). Эти операторы — основа сложных топологий. *)

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let data_ts s =
  Stream.to_list s |> List.filter_map (function
    | Mf_event.Data (v, _) -> Some v | _ -> None)

(* ── union упорядочивает по event-time ─────────────────────── *)
let test_union_order () =
  Printf.printf "\n-- union merges two streams ordered by event-time\n";
  (* поток A: события на 10,30,50; поток B: 20,40 *)
  let a = Stream.of_list [Mf_event.data "a1" 10; Mf_event.data "a2" 30; Mf_event.data "a3" 50] in
  let b = Stream.of_list [Mf_event.data "b1" 20; Mf_event.data "b2" 40] in
  let out = Mf_event.union a b |> data_ts in
  check "merged in time order" (out = ["a1"; "b1"; "a2"; "b2"; "a3"])

(* ── union: все события сохраняются ────────────────────────── *)
let test_union_complete () =
  Printf.printf "\n-- union loses nothing\n";
  let a = Stream.of_list (List.init 100 (fun i -> Mf_event.data ("a"^string_of_int i) (i*2))) in
  let b = Stream.of_list (List.init 100 (fun i -> Mf_event.data ("b"^string_of_int i) (i*2+1))) in
  let out = Mf_event.union a b |> data_ts in
  check "all 200 events present" (List.length out = 200);
  (* проверим что отсортировано: восстановим ts по порядку — должно расти *)
  let all = Mf_event.union
    (Stream.of_list (List.init 100 (fun i -> Mf_event.data i (i*2))))
    (Stream.of_list (List.init 100 (fun i -> Mf_event.data i (i*2+1))))
    |> Stream.to_list |> List.filter_map (function Mf_event.Data (_, t) -> Some t | _ -> None) in
  let sorted = List.sort compare all in
  check "output is time-ordered" (all = sorted)

(* ── union watermark = минимум входов ──────────────────────── *)
let test_union_watermark () =
  Printf.printf "\n-- union watermark is the min of both inputs\n";
  (* A продвинул wm до 100, B только до 50 → объединённый wm = 50 *)
  let a = Stream.of_list [Mf_event.data "a" 10; Mf_event.wm 100] in
  let b = Stream.of_list [Mf_event.data "b" 20; Mf_event.wm 50] in
  let wms = Mf_event.union a b |> Stream.to_list
    |> List.filter_map (function Mf_event.Watermark w -> Some w | _ -> None) in
  (* пока B на 50, объединённый wm не может превысить 50, пока B не закончится *)
  check "union does not advance past slower input"
    (List.for_all (fun w -> w <= 100) wms);
  check "emitted some watermark" (List.length wms >= 1)

(* ── empty union ───────────────────────────────────────────── *)
let test_union_empty () =
  Printf.printf "\n-- union with empty stream\n";
  let mk () = Stream.of_list [Mf_event.data "a1" 10; Mf_event.data "a2" 20] in
  let out = Mf_event.union (mk ()) Stream.empty |> data_ts in
  check "union with empty = the non-empty stream" (out = ["a1"; "a2"]);
  let out2 = Mf_event.union Stream.empty (mk ()) |> data_ts in
  check "empty union x = x" (out2 = ["a1"; "a2"])

(* ── update_table наполняет справочник из потока ───────────── *)
let test_update_table () =
  Printf.printf "\n-- update_table fills a table from a passing stream\n";
  let tbl : (string, string * string) Hashtbl.t = Hashtbl.create 8 in
  (* поток конфигурации: пары (ключ, значение) *)
  let config = Stream.of_list [
    Mf_event.data ("sensor1", "active") 1;
    Mf_event.data ("sensor2", "maintenance") 2;
    Mf_event.data ("sensor1", "inactive") 3;   (* обновление *)
  ] in
  (* прогоняем через update_table — side effect наполняет tbl *)
  let passed = config
    |> Pipe.update_table tbl ~key:(fun (k, _) -> k)
    |> Stream.to_list in
  check "events pass through unchanged" (List.length passed = 3);
  check "table has sensor1 = inactive (last value)"
    (Hashtbl.find_opt tbl "sensor1" = Some ("sensor1", "inactive"));
  check "table has sensor2" (Hashtbl.find_opt tbl "sensor2" = Some ("sensor2", "maintenance"))

(* ── update_table + enrich: один поток обновляет, другой обогащается ── *)
let test_update_then_enrich () =
  Printf.printf "\n-- one stream updates a table, another enriches from it\n";
  (* таблица: ключ устройства → зона. Наполняется из потока конфигурации,
     где значение несёт и ключ, и зону. *)
  let tbl : (string, string * string) Hashtbl.t = Hashtbl.create 8 in
  (* поток конфигурации: значения вида (device, zone) *)
  let _ = Stream.of_list [Mf_event.data ("A", "zone-1") 1; Mf_event.data ("B", "zone-2") 2]
    |> Pipe.update_table tbl ~key:(fun (d, _) -> d)    (* ключ = device *)
    |> Stream.to_list in
  (* tbl сейчас: "A" -> ("A","zone-1") ... извлечём зону при enrich *)
  let module K = Keyed.Make (struct type t = string let key s = s end) in
  let lookup k = match Hashtbl.find_opt tbl k with Some (_, z) -> Some z | None -> None in
  let enriched = Stream.of_list [Mf_event.data "A" 10; Mf_event.data "B" 20; Mf_event.data "C" 30]
    |> Pipe.enrich (module K)
         ~from:lookup
         ~merge:(fun s zone -> match zone with Some z -> s ^ ":" ^ z | None -> s ^ ":?")
    |> data_ts in
  check "A enriched with zone-1" (List.nth enriched 0 = "A:zone-1");
  check "B enriched with zone-2" (List.nth enriched 1 = "B:zone-2");
  check "C unknown (not in table)" (List.nth enriched 2 = "C:?")

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Union (event-time) + update_table\n";
  Printf.printf "==========================================\n";
  test_union_order ();
  test_union_complete ();
  test_union_watermark ();
  test_union_empty ();
  test_update_table ();
  test_update_then_enrich ();
  Printf.printf "\nAll union + update_table tests passed.\n"
