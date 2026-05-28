(* dlq_noop: молча выбрасывает *)
type t     = { mutable n : int }
type entry = { topic: string; payload: bytes; error: string; ts: int; attempt: int }
let create ()   = { n = 0 }
let send t _    = t.n <- t.n + 1
let flush _     = ()
let count t     = t.n
