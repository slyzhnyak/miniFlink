(** End-to-end test: Window emits Update, keyed_join handles atomically.

    Это **критический** тест архитектурной цели Phase 1-3 refactor'а:
    late event correction должен идти end-to-end атомарно от Window до
    downstream snapshot-based операторов, БЕЗ промежуточного None
    flicker'а в keyed_join slot. *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

type sensor = { s_id : string; s_value : float }

module ByLamp : Keyed.S with type t = (string * float) = struct
  type t = string * float
  let key (lamp, _) = lamp
end

let () =
  Printf.printf "Test: end-to-end Window → keyed_join atomic Update\n%!";

  (* Сценарий:
     - Один Window на sensor A с tumbling(1000)
     - Window закрывается на wm=2000 → emits Data(("L1", sum=10))
     - Late event arrives → Window emits Update(old=10, new=15)
     - keyed_join видит ОДИН Update — НЕ создаёт промежуточный None
       slot snapshot
     - Downstream видит: snapshot [Some ("L1", 10)] → snapshot [Some ("L1", 15)]
       НЕ: [Some 10] → [None] → [Some 15] *)

  let events = [
    Mf_event.data { s_id = "L1"; s_value = 10.0 } 100;
    Mf_event.wm 2000;          (* закрывает window [0,1000), emit *)
    Mf_event.data { s_id = "L1"; s_value = 5.0 } 500;  (* late, попадает в [0,1000) *)
    Mf_event.wm 3000;
  ] in

  let module ByLampIn : Keyed.S with type t = sensor = struct
    type t = sensor
    let key (s : t) = s.s_id
  end in

  (* Window output: (lamp, sum) per window *)
  let window_out = events |> Stream.of_list
    |> Pipe.window_agg (module ByLampIn)
         ~allowed_lateness:5000
         (Pipe.tumbling 1000)
         (Agg.sum (fun s -> s.s_value))
  in

  (* Подадим в keyed_join — он ожидает stream'ы по (string, value) пар *)
  let joined = Pipe.keyed_join (module ByLamp) [window_out] in

  (* Собираем все snapshot'ы — каждый snapshot это (key, [option list]) *)
  let snapshots = ref [] in
  let rec drain () = match joined () with
    | None -> ()
    | Some (Mf_event.Data ((key, opts), _)) ->
      snapshots := (key, opts) :: !snapshots; drain ()
    | Some (Mf_event.Watermark _)
    | Some (Mf_event.Retract _)
    | Some (Mf_event.Update _) -> drain ()
  in drain ();
  let snaps = List.rev !snapshots in

  Printf.printf "  emitted snapshots: %d\n" (List.length snaps);
  List.iteri (fun i (key, opts) ->
    Printf.printf "  [%d] key=%s slots=[%s]\n" i key
      (String.concat ";"
         (List.map (function
            | None -> "None"
            | Some (_, v) -> Printf.sprintf "Some(%.1f)" v) opts))
  ) snaps;

  (* Атомарность: должны быть РОВНО 2 snapshot'а:
     1. После закрытия первого window: Some(10)
     2. После Update'а от late event: Some(15)
     НЕ должно быть промежуточного None! *)
  check "2 snapshots emitted (initial + atomic update)"
    (List.length snaps = 2);

  (match snaps with
   | [(_, [Some (_, v1)]); (_, [Some (_, v2)])] ->
     check (Printf.sprintf "first snapshot = 10.0 (got %.1f)" v1)
       (Float.abs (v1 -. 10.0) < 0.001);
     check (Printf.sprintf "second snapshot = 15.0 = 10+5 (got %.1f)" v2)
       (Float.abs (v2 -. 15.0) < 0.001)
   | _ -> fail "snapshots not in expected [Some;Some] form — flicker?");

  (* Sanity check: НЕТ None snapshot между Some'ами *)
  let has_none_between = List.exists (fun (_, opts) ->
    List.exists (function None -> true | Some _ -> false) opts) snaps in
  check "no None flicker between snapshots — atomic correction"
    (not has_none_between);

  Printf.printf "\nEnd-to-end atomic Update verified.\n"
