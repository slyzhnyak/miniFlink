(* Kafka source adapter — реализует Checkpoint_parallel.seekable_source
   (путь B: ядро видит обычный int-счётчик, Kafka-специфика спрятана).

   Как это работает:
   - наружу (для ядра EO) отдаём seekable_source с position = int
     (монотонный счётчик прочитанных событий);
   - внутри ведём маппинг счётчик → Kafka partition_offset-ы, обновляя
     его на каждом poll;
   - на seek(n) ядра находим Kafka-позиции, актуальные для счётчика n, и
     делаем реальный seek в брокере по каждой партиции.

   Так ядро exactly-once не меняется, а multi-partition offset живёт в
   адаптере. decode-ошибки идут в on_error (DLQ-совместимо), не теряются
   молча. *)

open Miniflink
open Kafka_client

module Make (C : CONSUMER) = struct
  type 'a t = {
    consumer  : C.t;
    decode    : string -> ('a, string) result;
    ts_of     : 'a -> Time.t;
    on_error  : string -> string -> unit;   (* topic, payload при ошибке decode *)
    (* счётчик → снимок партиционных позиций на этом счётчике *)
    mutable counter : int;
    snapshots : (int, partition_offset list) Hashtbl.t;
    (* текущая позиция каждой партиции (последняя прочитанная) *)
    positions : (string * int, partition_offset) Hashtbl.t;
  }

  let create consumer ~decode ~ts_of ?(on_error = fun _ _ -> ()) () = {
    consumer; decode; ts_of; on_error;
    counter = 0;
    snapshots = Hashtbl.create 64;
    positions = Hashtbl.create 8;
  }

  let cur_positions t =
    Hashtbl.fold (fun _ po acc -> po :: acc) t.positions []

  (* один pull для seekable_source.pull: вернуть следующее Data-событие
     или None если в брокере сейчас пусто *)
  let rec pull t () : 'a Mf_event.t option =
    match C.poll t.consumer ~timeout_ms:100 with
    | None -> None
    | Some msg ->
      (* обновляем позицию партиции и счётчик *)
      Hashtbl.replace t.positions (msg.m_pos.topic, msg.m_pos.partition)
        { msg.m_pos with offset = Int64.add msg.m_pos.offset 1L };
      t.counter <- t.counter + 1;
      Hashtbl.replace t.snapshots t.counter (cur_positions t);
      (match t.decode msg.m_payload with
       | Ok v -> Some (Mf_event.data v (t.ts_of v))
       | Error _ ->
         t.on_error msg.m_pos.topic msg.m_payload;
         pull t ())   (* пропускаем битое, тянем следующее *)

  (* seekable_source для ядра: position = счётчик, seek по счётчику *)
  let to_seekable (t : 'a t) : 'a Checkpoint_parallel.seekable_source =
    {
      pull = pull t;
      position = (fun () -> t.counter);
      seek = (fun n ->
        (* найти снимок партиционных позиций для счётчика n;
           если точного нет — ближайший не больше n *)
        let snap =
          match Hashtbl.find_opt t.snapshots n with
          | Some s -> Some s
          | None ->
            Hashtbl.fold (fun k v acc ->
              if k <= n then match acc with
                | Some (bk, _) when bk >= k -> acc
                | _ -> Some (k, v)
              else acc) t.snapshots None
            |> Option.map snd
        in
        (match snap with
         | Some positions ->
           List.iter (fun po -> C.seek t.consumer po) positions;
           List.iter (fun po ->
             Hashtbl.replace t.positions (po.topic, po.partition) po) positions
         | None ->
           (* нет снимка (откат в начало) — позиции на 0 *)
           Hashtbl.clear t.positions);
        t.counter <- n);
    }

  (* зафиксировать offset-ы в брокере (вызывать после подтверждённого
     checkpoint — это и есть Kafka-часть exactly-once на входной стороне) *)
  let commit_positions t =
    List.iter (fun po -> C.commit t.consumer po) (cur_positions t)
end
