(* In-memory фейк Kafka — для тестов адаптера без живого брокера.
   Хранит сообщения по (topic, partition), отдаёт по offset, поддерживает
   seek/commit и транзакционный producer (буфер до commit_txn). *)

open Kafka_client

(* ── Брокер: общее in-memory хранилище ───────────────────── *)
type broker = {
  (* (topic, partition) → массив payload-ов (offset = индекс) *)
  logs : (string * int, string array) Hashtbl.t;
}

let make_broker () = { logs = Hashtbl.create 8 }

let topic_log b ~topic ~partition =
  match Hashtbl.find_opt b.logs (topic, partition) with
  | Some a -> a | None -> [||]

(* засеять данные в партицию (для подготовки теста) *)
let seed b ~topic ~partition (payloads : string list) =
  Hashtbl.replace b.logs (topic, partition) (Array.of_list payloads)

(* ── Consumer ────────────────────────────────────────────── *)
module Consumer = struct
  type t = {
    broker : broker;
    parts  : (string * int) list;        (* какие (topic,part) читаем *)
    cursor : (string * int, int) Hashtbl.t;  (* следующий offset на чтение *)
    committed : (string * int, int64) Hashtbl.t;
  }

  let create broker parts =
    let cursor = Hashtbl.create 8 in
    List.iter (fun tp -> Hashtbl.replace cursor tp 0) parts;
    { broker; parts; cursor; committed = Hashtbl.create 8 }

  let poll t ~timeout_ms:_ =
    (* round-robin по партициям: первая с непрочитанным сообщением *)
    let rec find = function
      | [] -> None
      | (topic, partition) :: rest ->
        let log = topic_log t.broker ~topic ~partition in
        let cur = match Hashtbl.find_opt t.cursor (topic, partition) with
          | Some c -> c | None -> 0 in
        if cur < Array.length log then begin
          Hashtbl.replace t.cursor (topic, partition) (cur + 1);
          Some { m_pos = { topic; partition; offset = Int64.of_int cur };
                 m_key = None; m_payload = log.(cur) }
        end else find rest
    in find t.parts

  let seek t (po : partition_offset) =
    Hashtbl.replace t.cursor (po.topic, po.partition) (Int64.to_int po.offset)

  let commit t (po : partition_offset) =
    Hashtbl.replace t.committed (po.topic, po.partition) po.offset

  let close _ = ()
end

(* ── Producer ────────────────────────────────────────────── *)
module Producer = struct
  type t = {
    broker      : broker;
    mutable txn : (string * int * string) list;  (* буфер открытой транзакции *)
    mutable in_txn : bool;
  }

  let create broker = { broker; txn = []; in_txn = false }

  let append_msg b ~topic ~partition ~payload =
    let log = topic_log b ~topic ~partition in
    Hashtbl.replace b.logs (topic, partition)
      (Array.append log [| payload |])

  let produce t ~topic ~partition ~key:_ ~payload =
    let p = if partition < 0 then 0 else partition in
    if t.in_txn then t.txn <- (topic, p, payload) :: t.txn
    else append_msg t.broker ~topic ~partition:p ~payload

  let flush _ ~timeout_ms:_ = ()
  let begin_txn t = t.in_txn <- true; t.txn <- []
  let commit_txn t =
    List.iter (fun (topic, p, payload) ->
      append_msg t.broker ~topic ~partition:p ~payload) (List.rev t.txn);
    t.txn <- []; t.in_txn <- false
  let abort_txn t = t.txn <- []; t.in_txn <- false
  let close _ = ()
end
