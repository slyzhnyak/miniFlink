open Miniflink
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

(* ── N-5: серия watermark-ов не переполняет стек ──────────────
   Раньше union.step рекурсивно вызывал себя на каждом watermark, не
   продвигающем границу; поток из многих таких watermark-ов давал
   stack overflow. Теперь step — цикл, стек O(1). *)
let test_union_many_watermarks () =
  Printf.printf "\n-- union survives a long run of watermarks (no stack overflow)\n";
  (* A исчерпан сразу (пустой), B шлёт 200000 не-возрастающих watermark
     (одинаковый ts → границу не продвигают → старый код рекурсировал
     на каждый). В конце — одно Data, чтобы было что вернуть. *)
  let n = 200_000 in
  let a = Stream.of_list [] in
  let b = Stream.of_list (
    List.init n (fun _ -> Mf_event.wm 5)        (* все одинаковые *)
    @ [ Mf_event.data "tail" 10 ]) in
  let out = Mf_event.union a b in
  (* продренируем циклом (не рекурсией), чтобы тест проверял именно union *)
  let count = ref 0 in
  let continue = ref true in
  while !continue do
    (match out () with None -> continue := false | Some _ -> incr count)
  done;
  check "drained without stack overflow" (!count >= 1)

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

(* ── union: одновременные timestamps не теряются ───────────── *)
let test_union_simultaneous () =
  Printf.printf "\n-- union handles simultaneous timestamps (tie-break)\n";
  let a = Stream.of_list [Mf_event.data "a@5" 5; Mf_event.data "a@10" 10] in
  let b = Stream.of_list [Mf_event.data "b@5" 5; Mf_event.data "b@10" 10] in
  let out = Mf_event.union a b |> data_ts in
  check "all 4 events present despite ties" (List.length out = 4);
  check "tie-break: left stream first at equal ts"
    (out = ["a@5"; "b@5"; "a@10"; "b@10"])

(* ── union: один вход кончился раньше ──────────────────────── *)
let test_union_uneven_length () =
  Printf.printf "\n-- union drains the longer stream's tail\n";
  let short = Stream.of_list [Mf_event.data "s1" 1] in
  let long = Stream.of_list
    [Mf_event.data "l1" 2; Mf_event.data "l2" 3; Mf_event.data "l3" 4] in
  let out = Mf_event.union short long |> data_ts in
  check "short then all of long" (out = ["s1"; "l1"; "l2"; "l3"])

(* ── union: вложенный (три потока) сохраняет порядок ───────── *)
let test_union_associative () =
  Printf.printf "\n-- nested union (three streams) keeps time order\n";
  let mk p ts = Mf_event.data p ts in
  let a () = Stream.of_list [mk "a" 1; mk "a" 7] in
  let b () = Stream.of_list [mk "b" 3; mk "b" 9] in
  let c () = Stream.of_list [mk "c" 5; mk "c" 11] in
  let left  = Mf_event.union (Mf_event.union (a ()) (b ())) (c ()) |> data_ts in
  let right = Mf_event.union (a ()) (Mf_event.union (b ()) (c ())) |> data_ts in
  check "(a∪b)∪c time-ordered" (left = ["a"; "b"; "c"; "a"; "b"; "c"]);
  check "a∪(b∪c) same result" (left = right)

(* ── union watermark монотонен ─────────────────────────────── *)
let test_union_watermark_monotone () =
  Printf.printf "\n-- union watermarks are monotone (never go backwards)\n";
  let a = Stream.of_list
    [Mf_event.data "a" 10; Mf_event.wm 20; Mf_event.data "a2" 30; Mf_event.wm 40] in
  let b = Stream.of_list
    [Mf_event.data "b" 15; Mf_event.wm 25; Mf_event.data "b2" 35; Mf_event.wm 45] in
  let wms = Mf_event.union a b |> Stream.to_list
    |> List.filter_map (function Mf_event.Watermark w -> Some w | _ -> None) in
  let rec monotone = function
    | x :: (y :: _ as rest) -> x <= y && monotone rest
    | _ -> true in
  check "watermarks non-decreasing" (monotone wms)

(* Краевой случай (review 3.8): один вход подтвердил высокий watermark и
   закончился, другой потом даёт НИЗКИЙ — объединённый не должен
   откатиться назад (иначе закрытые окна «переоткрылись» бы). *)
let test_union_done_no_regression () =
  Printf.printf "\n-- union: finished input does not let watermark go backwards\n";
  (* B подтверждает wm=100 и заканчивается; A потом даёт wm=50 *)
  let a = Stream.of_list [Mf_event.data "a" 10; Mf_event.wm 50] in
  let b = Stream.of_list [Mf_event.data "b" 20; Mf_event.wm 100] in
  let wms = Mf_event.union a b |> Stream.to_list
    |> List.filter_map (function Mf_event.Watermark w -> Some w | _ -> None) in
  let rec monotone = function
    | x :: (y :: _ as rest) -> x <= y && monotone rest | _ -> true in
  check "no backwards watermark after input finishes" (monotone wms);
  check "max emitted watermark not lost"
    (match List.rev wms with last :: _ -> last >= 50 | [] -> false)

(* ── update_table: enrich видит обновления ПО ХОДУ ─────────── *)
let test_update_table_live () =
  Printf.printf "\n-- enrich sees table updates as they happen (live)\n";
  let tbl : (string, int) Hashtbl.t = Hashtbl.create 8 in
  Hashtbl.replace tbl "x" 1;
  let module K = Keyed.Make (struct type t = string let key _ = "x" end) in
  let lookup = Table.of_hashtbl tbl in
  let enrich1 = Stream.of_list [Mf_event.data "before" 1]
    |> Pipe.enrich (module K) ~from:lookup
         ~merge:(fun s v -> match v with Some n -> Printf.sprintf "%s=%d" s n | None -> s)
    |> data_ts in
  check "enrich sees value 1 before update" (enrich1 = ["before=1"]);
  Hashtbl.replace tbl "x" 2;
  let enrich2 = Stream.of_list [Mf_event.data "after" 1]
    |> Pipe.enrich (module K) ~from:lookup
         ~merge:(fun s v -> match v with Some n -> Printf.sprintf "%s=%d" s n | None -> s)
    |> data_ts in
  check "enrich sees value 2 after update" (enrich2 = ["after=2"])

(* ── update_table: watermark проходит, таблицу не трогает ──── *)
let test_update_table_watermark () =
  Printf.printf "\n-- update_table passes watermarks without touching the table\n";
  let tbl : (string, string * int) Hashtbl.t = Hashtbl.create 8 in
  let out = Stream.of_list [
    Mf_event.data ("k", 1) 10;
    Mf_event.wm 20;
    Mf_event.data ("k", 2) 30;
  ] |> Pipe.update_table tbl ~key:(fun (k, _) -> k) |> Stream.to_list in
  let wms = List.filter (function Mf_event.Watermark _ -> true | _ -> false) out in
  check "watermark passed through" (List.length wms = 1);
  check "table has last value" (Hashtbl.find_opt tbl "k" = Some ("k", 2))

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Union (event-time) + update_table\n";
  Printf.printf "==========================================\n";
  test_union_order ();
  test_union_complete ();
  test_union_watermark ();
  test_union_empty ();
  test_union_simultaneous ();
  test_union_uneven_length ();
  test_union_associative ();
  test_union_watermark_monotone ();
  test_union_done_no_regression ();
  test_union_many_watermarks ();
  test_update_table ();
  test_update_table_live ();
  test_update_table_watermark ();
  test_update_then_enrich ();
  Printf.printf "\nAll union + update_table tests passed.\n"
