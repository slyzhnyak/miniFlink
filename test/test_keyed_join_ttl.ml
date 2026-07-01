(** Тест C-3: TTL-очистка состояния в {!Pipe.keyed_join}.

    Без [?ttl] состояние per-key живёт вечно (утечка на неограниченном
    пространстве ключей). С [?ttl] ключ, не обновлявшийся дольше
    [wm - ttl], удаляется при watermark.

    Проверяем поведенчески (states — внутренняя структура): после
    eviction ключа его состояние сбрасывается. Если ключ K имел значение
    в slot 0, был выселен по watermark, а затем снова получил событие в
    slot 1 — снапшот покажет [None; Some new] (чистый старт), а НЕ
    [Some old; Some new] (что было бы, если бы состояние сохранилось).

    Контрольный случай без ttl: то же самое сохраняет старое значение. *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

type kv = string * float
module By_key : Keyed.S with type t = kv = struct
  type t = kv
  let key (k, _) = k
end

(* поток из явного списка событий *)
let stream_of events =
  let l = ref events in
  fun () -> match !l with [] -> None | x :: rest -> l := rest; Some x

let collect_data stream =
  let acc = ref [] in
  let rec drain () = match stream () with
    | None -> ()
    | Some (Mf_event.Data (v, _)) -> acc := v :: !acc; drain ()
    | Some _ -> drain ()
  in drain ();
  List.rev !acc

(* последний снапшот для ключа k *)
let last_snapshot_for k data =
  List.fold_left (fun acc (key, opts) -> if key = k then Some opts else acc)
    None data

let opt_floats opts =
  List.map (function None -> None | Some (_, f) -> Some f) opts

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  C-3: keyed_join TTL eviction\n";
  Printf.printf "==========================================\n";

  (* ── 1. С ttl: ключ выселяется, состояние сбрасывается ──── *)
  Printf.printf "\n-- 1. with ttl: stale key evicted, state resets\n";
  (* union продвигает watermark только до min по всем входам, поэтому
     watermark=100 должны выдать ОБА входа, иначе он не дойдёт до
     keyed_join. k1 в slot 0 при ts=10; оба входа дают wm=100 (ttl=20 →
     порог 80, ключ 10 < 80 выселяется); затем k1 в slot 1 при ts=110. *)
  let s0 = stream_of [
    Mf_event.data ("k1", 1.0) 10;
    Mf_event.wm 100;
  ] in
  let s1 = stream_of [
    Mf_event.wm 100;
    Mf_event.data ("k1", 2.0) 110;
  ] in
  let joined = Pipe.keyed_join (module By_key) ~ttl:20 [s0; s1] in
  let data = collect_data joined in
  (match last_snapshot_for "k1" data with
   | Some opts ->
     (* после eviction slot 0 должен быть None (старое выселено),
        slot 1 = Some 2.0 *)
     check "выселенный k1 стартует с чистого состояния [None; Some 2]"
       (opt_floats opts = [None; Some 2.0])
   | None -> fail "нет снапшота для k1");

  (* ── 2. Без ttl: состояние сохраняется (контроль) ───────── *)
  Printf.printf "\n-- 2. without ttl: state persists (control)\n";
  let s0 = stream_of [
    Mf_event.data ("k1", 1.0) 10;
    Mf_event.wm 100;
  ] in
  let s1 = stream_of [
    Mf_event.wm 100;
    Mf_event.data ("k1", 2.0) 110;
  ] in
  let joined = Pipe.keyed_join (module By_key) [s0; s1] in
  let data = collect_data joined in
  (match last_snapshot_for "k1" data with
   | Some opts ->
     (* без eviction slot 0 сохраняет старое значение 1.0 *)
     check "без ttl k1 сохраняет [Some 1; Some 2]"
       (opt_floats opts = [Some 1.0; Some 2.0])
   | None -> fail "нет снапшота для k1");

  (* ── 3. С ttl: свежий ключ НЕ выселяется ─────────────────── *)
  Printf.printf "\n-- 3. with ttl: fresh key survives watermark\n";
  (* k1 обновлён при ts=90, watermark=100, ttl=20 → порог 80, 90 > 80,
     ключ выживает *)
  let s0 = stream_of [
    Mf_event.data ("k1", 1.0) 90;
    Mf_event.wm 100;
  ] in
  let s1 = stream_of [
    Mf_event.wm 100;
    Mf_event.data ("k1", 2.0) 110;
  ] in
  let joined = Pipe.keyed_join (module By_key) ~ttl:20 [s0; s1] in
  let data = collect_data joined in
  (match last_snapshot_for "k1" data with
   | Some opts ->
     check "свежий k1 сохраняет состояние [Some 1; Some 2]"
       (opt_floats opts = [Some 1.0; Some 2.0])
   | None -> fail "нет снапшота для k1");

  Printf.printf "\nC-3 keyed_join eviction tests passed.\n"
