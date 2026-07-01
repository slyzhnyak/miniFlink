(* dlq_log: пишет в stderr с контекстом *)
(* entry type comes from Dlq_noop *)
type entry = Dlq_noop.entry = {
  topic: string; payload: bytes; error: string; ts: int; attempt: int
}
type t     = { mutable n : int; mu : Mutex.t }

let create () = { n = 0; mu = Mutex.create () }

let send t e =
  Mutex.lock t.mu;
  t.n <- t.n + 1;
  Mutex.unlock t.mu;
  (* через Log.warn — единый JSON-формат с остальными логами (иначе
     текстовые DLQ-строки проваливаются мимо JSON-парсеров агрегаторов
     вроде Loki/Vector). Log.warn сам экранирует поля. *)
  let payload =
    if Bytes.length e.payload > 200
    then Bytes.sub_string e.payload 0 200 ^ "..."
    else Bytes.to_string e.payload in
  Log.warn ~fields:[
    ("event", "dlq_message");
    ("topic", e.topic);
    ("attempt", string_of_int e.attempt);
    ("error", e.error);
    ("payload", payload);
    ("ts", string_of_int e.ts);
  ] "dead letter"

let flush _ = ()

(* count берёт тот же mutex, что и send: чтение int при конкурентной
   записи из другого домена без синхронизации — data race (UB на
   OCaml 5). Иначе финальный счётчик DLQ мог быть неточным. *)
let count t =
  Mutex.lock t.mu;
  let n = t.n in
  Mutex.unlock t.mu;
  n
