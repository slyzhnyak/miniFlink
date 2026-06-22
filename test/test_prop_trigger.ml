(** Property-тесты: инварианты состояний Trigger.

    Без debounce (problem_for=0) триггер — детерминированный конечный
    автомат per ключ. Главные инварианты для ЛЮБОГО потока значений:

    1. «Активный alert» (Data alert минус Retract того же alert) для
       ключа равен 1, если последнее по времени значение ключа
       удовлетворяет condition, иначе 0. Триггер не «залипает».

    2. Каждый Retract парный к ранее эмитированному Data (нельзя
       отозвать то, что не эмитили) — alert'ы сбалансированы.

    3. Out-of-order значения (ts ниже уже виденного) игнорируются —
       финальное состояние зависит только от значения с максимальным
       ts per ключ. *)

open Miniflink
open QCheck

(* alert = (key, value) чтобы по выходу видеть, чей и какой. *)
let spec () =
  Trigger.create
    ~name:"prop"
    ~condition:(Trigger.greater_than 50.)
    ~produce_alert:(fun ~key ~value ~ts:_ -> `Alert (key, value))
    ~produce_recovery:(fun ~key ~ts:_ -> `Recovery key)
    ()

(* Поток (key, value, ts) с возрастающим ts (in-order) — для
   инварианта 1/2 без усложнения out-of-order. *)
let gen_in_order : (string * float * int) list Gen.t =
  let open Gen in
  let keys = [| "A"; "B"; "C" |] in
  sized_size (int_range 0 50) (fun n ->
    list_repeat n (tup2 (int_range 0 2) (float_range 0. 100.))
    >>= fun pairs ->
    return (List.mapi (fun i (ki, v) -> (keys.(ki), v, i * 10)) pairs))

let arb_in_order = make ~print:(fun evs ->
  String.concat " " (List.map (fun (k,v,t) -> Printf.sprintf "%s=%.0f@%d" k v t) evs))
  gen_in_order

let run_trigger events =
  let stream =
    (List.map (fun (k,v,ts) -> Mf_event.data (k, v) ts) events
     @ [Mf_event.wm 1_000_000])
    |> Stream.of_list |> Trigger.of_stream (spec ()) in
  let acc = ref [] in
  let rec go () = match stream () with
    | None -> ()
    | Some (Mf_event.Data (a, _)) -> acc := `D a :: !acc; go ()
    | Some (Mf_event.Retract (a, _)) -> acc := `R a :: !acc; go ()
    | Some _ -> go ()
  in go (); List.rev !acc

(* Oracle: для каждого ключа — удовлетворяет ли его ПОСЛЕДНЕЕ
   (по ts) значение условию (v > 50). *)
let oracle_active events =
  let last : (string, float) Hashtbl.t = Hashtbl.create 8 in
  List.iter (fun (k, v, _) -> Hashtbl.replace last k v) events;
  Hashtbl.fold (fun k v acc -> if v > 50. then k :: acc else acc) last []
  |> List.sort compare

(* Из выхода считаем активные alert'ы: Data `Alert минус Retract. *)
let active_from_output out =
  let bal : (string, int) Hashtbl.t = Hashtbl.create 8 in
  List.iter (function
    | `D (`Alert (k, _)) ->
      Hashtbl.replace bal k (1 + (try Hashtbl.find bal k with Not_found -> 0))
    | `R (`Alert (k, _)) ->
      Hashtbl.replace bal k ((try Hashtbl.find bal k with Not_found -> 0) - 1)
    | _ -> ()) out;
  Hashtbl.fold (fun k n acc -> if n > 0 then k :: acc else acc) bal []
  |> List.sort compare

let prop_active_matches_oracle =
  Test.make ~count:1000 ~name:"trigger: active alerts = keys with last value > threshold"
    arb_in_order
    (fun events -> active_from_output (run_trigger events) = oracle_active events)

(* Инвариант 2: баланс — для каждого ключа Retract'ов не больше, чем
   Data, в любой префиксной точке (нельзя отозвать неэмитированное). *)
let prop_balanced =
  Test.make ~count:1000 ~name:"trigger: retracts never exceed prior data (per key)"
    arb_in_order
    (fun events ->
       let out = run_trigger events in
       let bal : (string, int) Hashtbl.t = Hashtbl.create 8 in
       let ok = ref true in
       List.iter (function
         | `D (`Alert (k, _)) ->
           Hashtbl.replace bal k (1 + (try Hashtbl.find bal k with Not_found -> 0))
         | `R (`Alert (k, _)) ->
           let cur = try Hashtbl.find bal k with Not_found -> 0 in
           if cur <= 0 then ok := false;  (* отзыв без активного alert *)
           Hashtbl.replace bal k (cur - 1)
         | _ -> ()) out;
       !ok)

(* Инвариант 2b: активный alert никогда не превышает 1 на ключ
   (триггер не эмитит дубли alert для уже-Problem ключа). *)
let prop_at_most_one_active =
  Test.make ~count:1000 ~name:"trigger: at most one active alert per key at any point"
    arb_in_order
    (fun events ->
       let out = run_trigger events in
       let bal : (string, int) Hashtbl.t = Hashtbl.create 8 in
       let ok = ref true in
       List.iter (function
         | `D (`Alert (k, _)) ->
           let cur = 1 + (try Hashtbl.find bal k with Not_found -> 0) in
           if cur > 1 then ok := false;
           Hashtbl.replace bal k cur
         | `R (`Alert (k, _)) ->
           Hashtbl.replace bal k ((try Hashtbl.find bal k with Not_found -> 0) - 1)
         | _ -> ()) out;
       !ok)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Property: Trigger state invariants\n";
  Printf.printf "==========================================\n";
  let suites = [ prop_active_matches_oracle; prop_balanced; prop_at_most_one_active ] in
  let ok = QCheck_runner.run_tests ~verbose:false suites in
  if ok = 0 then (Printf.printf "\nAll Trigger properties passed.\n"; exit 0)
  else (Printf.printf "\nSOME PROPERTIES FAILED.\n"; exit 1)
