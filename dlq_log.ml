(* dlq_log: пишет в stderr с контекстом *)
type entry = { topic: string; payload: bytes; error: string; ts: int; attempt: int }
type t     = { mutable n : int; mu : Mutex.t }

let create () = { n = 0; mu = Mutex.create () }

let send t e =
  Mutex.lock t.mu;
  t.n <- t.n + 1;
  Printf.eprintf "[DLQ] topic=%s attempt=%d error=%s payload=%S ts=%d\n%!"
    e.topic e.attempt e.error
    (if Bytes.length e.payload > 200
     then Bytes.sub_string e.payload 0 200 ^ "..."
     else Bytes.to_string e.payload)
    e.ts;
  Mutex.unlock t.mu

let flush _ = ()
let count t = t.n
