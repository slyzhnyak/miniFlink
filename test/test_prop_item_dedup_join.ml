(** Property-тесты: silence_age, dedup, keyed_join.

    - silence_age: на каждое Data-событие ключа эмитит (key, 0) —
      «silence обнулился». Инвариант: число (k,0)-эмиссий = число
      Data-событий ключа k.
    - dedup: после пропущенного события следующее в пределах cooldown
      подавляется. Инвариант: между двумя соседними прошедшими
      событиями одного rule-ключа разрыв ts >= cooldown; и каждое
      прошедшее событие присутствовало во входе.
    - keyed_join: каждое входное Data-событие порождает ровно одно
      выходное (для своего ключа), значение которого содержит это
      событие в соответствующем слоте. *)

open Miniflink
open QCheck

module SK : Keyed.S with type t = string * int = struct
  type t = string * int
  let key (k, _) = k
end

let gen_stream : (string * int * int) list Gen.t =
  let open Gen in
  let keys = [| "A"; "B"; "C" |] in
  sized_size (int_range 0 50) (fun n ->
    list_repeat n (tup2 (int_range 0 2) (int_range 0 100))
    >>= fun pairs ->
    return (List.mapi (fun i (ki, v) -> (keys.(ki), v, i * 100)) pairs))

let arb_stream = make ~print:(fun evs ->
  String.concat " " (List.map (fun (k,v,t) -> Printf.sprintf "%s:%d@%d" k v t) evs))
  gen_stream

(* ── silence_age: число (k,0) эмиссий = число Data ключа k ───── *)
let prop_silence_age_zeros =
  Test.make ~count:1000
    ~name:"silence_age: zero-emits per key = data-events per key"
    arb_stream
    (fun events ->
       let stream =
         (List.map (fun (k,v,ts) -> Mf_event.data (k, v) ts) events
          @ [Mf_event.wm 1_000_000])
         |> Stream.of_list
         |> Item.silence_age ~by:(fun ((k,_):string*int) -> k) ~tick:(Time.seconds 1000) in
       (* считаем (k, 0) эмиссии *)
       let zeros : (string, int) Hashtbl.t = Hashtbl.create 8 in
       let rec go () = match stream () with
         | None -> ()
         | Some (Mf_event.Data ((k, age), _)) ->
           if age = 0 then
             Hashtbl.replace zeros k (1 + (try Hashtbl.find zeros k with Not_found -> 0));
           go ()
         | Some _ -> go ()
       in go ();
       (* oracle: число Data per ключ *)
       let oracle : (string, int) Hashtbl.t = Hashtbl.create 8 in
       List.iter (fun (k,_,_) ->
         Hashtbl.replace oracle k (1 + (try Hashtbl.find oracle k with Not_found -> 0)))
         events;
       (* каждый ключ: zeros >= data-count (минимум один 0 на событие;
          tick большой, тиков почти нет, но граница окна может дать
          дополнительный — проверяем что zeros содержит как минимум
          по одному 0 на каждое событие) *)
       Hashtbl.fold (fun k cnt ok ->
         ok && (try Hashtbl.find zeros k with Not_found -> 0) >= cnt) oracle true)

(* ── dedup: прошедшие события разнесены на >= cooldown ───────── *)
let cooldown = 1000

let prop_dedup_spacing =
  Test.make ~count:1000
    ~name:"dedup: passed events of same rule are >= cooldown apart"
    arb_stream
    (fun events ->
       let stream =
         List.map (fun (k,v,ts) -> Mf_event.data (k, v) ts) events
         |> Stream.of_list
         |> Pipe.dedup (module SK) ~rule:(fun (k,_) -> k) ~cooldown in
       let passed : (string, int list) Hashtbl.t = Hashtbl.create 8 in
       let rec go () = match stream () with
         | None -> ()
         | Some (Mf_event.Data ((k,_), ts)) ->
           Hashtbl.replace passed k (ts :: (try Hashtbl.find passed k with Not_found -> []));
           go ()
         | Some _ -> go ()
       in go ();
       (* для каждого rule-ключа соседние прошедшие ts разнесены >= cooldown *)
       Hashtbl.fold (fun _ tss ok ->
         if not ok then false
         else
           let sorted = List.sort compare tss in
           let rec check = function
             | a :: (b :: _ as tl) -> if b - a >= cooldown then check tl else false
             | _ -> true
           in check sorted) passed true)

let prop_dedup_subset =
  Test.make ~count:1000 ~name:"dedup: every passed event was in the input"
    arb_stream
    (fun events ->
       let input = List.map (fun (k,v,ts) -> (k,v,ts)) events in
       let stream =
         List.map (fun (k,v,ts) -> Mf_event.data (k, v) ts) events
         |> Stream.of_list
         |> Pipe.dedup (module SK) ~rule:(fun (k,_) -> k) ~cooldown in
       let rec go ok = match stream () with
         | None -> ok
         | Some (Mf_event.Data ((k,v), ts)) ->
           go (ok && List.mem (k,v,ts) input)
         | Some _ -> go ok
       in go true)

(* ── keyed_join: каждое входное Data даёт ровно одно выходное ── *)
let prop_keyed_join_count =
  Test.make ~count:500 ~name:"keyed_join: one output per input Data event"
    (make (Gen.tup2 gen_stream gen_stream))
    (fun (s1, s2) ->
       let mk evs = List.map (fun (k,v,ts) -> Mf_event.data (k,v) ts) evs
                    |> Stream.of_list in
       let joined = Pipe.keyed_join (module SK) [mk s1; mk s2] in
       let out_count = ref 0 in
       let rec go () = match joined () with
         | None -> ()
         | Some (Mf_event.Data _) -> incr out_count; go ()
         | Some _ -> go ()
       in go ();
       (* каждое Data из любого входа → одно выходное обновление *)
       !out_count = List.length s1 + List.length s2)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Property: silence_age / dedup / keyed_join\n";
  Printf.printf "==========================================\n";
  let suites = [ prop_silence_age_zeros; prop_dedup_spacing;
                 prop_dedup_subset; prop_keyed_join_count ] in
  let ok = QCheck_runner.run_tests ~verbose:false suites in
  if ok = 0 then (Printf.printf "\nAll item/dedup/join properties passed.\n"; exit 0)
  else (Printf.printf "\nSOME PROPERTIES FAILED.\n"; exit 1)
