(** Property-тест: прозрачность crash-recovery.

    Инвариант ортогональной persistence: для ЛЮБОГО потока событий и
    ЛЮБОЙ точки «краша», durable-прогон (фаза 1 до краша →
    восстановление из backend → фаза 2) даёт тот же результат, что
    непрерывный ephemeral-прогон того же потока. Краш «невидим».

    ВАЖНО про watermark: фаза 1 НЕ шлёт запредельный watermark — она
    продвигает его лишь чуть дальше последнего своего события. Иначе
    окна, выходящие за allowed_lateness, эвиктнулись бы из backend и
    стали невосстановимы — это был бы нереалистичный «краш» (в
    реальности wm не прыгает в бесконечность). Фаза 2 (новый instance,
    общий backend) досматривает хвост и шлёт уже финальный wm. *)

open Miniflink
open QCheck

module K : Keyed.S with type t = string * float = struct
  type t = string * float
  let key (k, _) = k
end

let win_size = 1000

let gen_stream : (string * float * int) list Gen.t =
  let open Gen in
  let keys = [| "A"; "B"; "C" |] in
  sized_size (int_range 1 40) (fun n ->
    list_repeat n (tup2 (int_range 0 2) (float_range 0. 100.))
    >>= fun pairs ->
    return (List.mapi (fun i (ki, v) -> (keys.(ki), v, i * 300)) pairs))

let arb_split = make
    ~print:(fun (evs, sp) ->
      Printf.sprintf "split@%d: %s" sp
        (String.concat " " (List.map (fun (k,v,t) ->
           Printf.sprintf "%s:%.0f@%d" k v t) evs)))
    (Gen.tup2 gen_stream (Gen.int_range 0 40))

let run ?wm ctx result events =
  let evs =
    List.map (fun (k,v,ts) -> Mf_event.data (k,v) ts) events
    @ (match wm with Some w -> [Mf_event.wm w] | None -> []) in
  let agg = Agg.contramap snd (Agg.sum (fun x -> x)) in
  let go () =
    let out = evs |> Stream.of_list
      |> Pipe.window_agg (module K) ~allowed_lateness:(win_size * 1000)
           (Pipe.tumbling win_size) agg in
    let rec drain () = match out () with
      | None -> ()
      | Some (Mf_event.Data ((k, r), stop)) ->
        Hashtbl.replace result (k, (stop-1)/win_size) r; drain ()
      | Some (Mf_event.Update { new_value = (k, r); ts = stop; _ }) ->
        Hashtbl.replace result (k, (stop-1)/win_size) r; drain ()
      | Some _ -> drain ()
    in drain ()
  in
  (match ctx with
   | None -> go ()
   | Some c -> Runtime_context.with_context c (fun () -> go ()))

let tbl_eq a b =
  let close x y = Float.abs (x -. y) < 1e-6 in
  Hashtbl.length a = Hashtbl.length b &&
  Hashtbl.fold (fun k v ok ->
    ok && (match Hashtbl.find_opt b k with Some v2 -> close v v2 | None -> false))
    a true

let last_ts evs = List.fold_left (fun m (_,_,t) -> max m t) 0 evs

let prop_crash_invisible =
  Test.make ~count:500
    ~name:"window_agg durable: phase1+crash+phase2 = continuous ephemeral"
    arb_split
    (fun (events, split_raw) ->
       let n = List.length events in
       if n = 0 then true
       else begin
         let split = split_raw mod (n + 1) in
         let rec take k = function
           | [] -> ([], [])
           | x :: xs when k > 0 -> let (a,b) = take (k-1) xs in (x::a, b)
           | xs -> ([], xs) in
         let phase1, phase2 = take split events in

         let final_wm = last_ts events + win_size * 10 in
         let baseline = Hashtbl.create 16 in
         run ~wm:final_wm None baseline events;

         let crash = Hashtbl.create 16 in
         let backend = Persistence_backend.of_memory (Hashtbl.create 16) in
         let ctx = Runtime_context.durable backend in
         (match phase1 with
          | [] -> ()
          | _ -> run ~wm:(last_ts phase1 + win_size / 2) (Some ctx) crash phase1);
         run ~wm:final_wm (Some ctx) crash phase2;

         tbl_eq baseline crash
       end)

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Property: persistence crash-recovery\n";
  Printf.printf "==========================================\n";
  let suites = [ prop_crash_invisible ] in
  let ok = QCheck_runner.run_tests ~verbose:false suites in
  if ok = 0 then (Printf.printf "\nPersistence property passed.\n"; exit 0)
  else (Printf.printf "\nPROPERTY FAILED.\n"; exit 1)
