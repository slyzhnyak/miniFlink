(* Заглушка: все операции — no-op, нулевой overhead *)
type counter   = unit
type gauge     = unit
type histogram = unit

let counter   ~name:_ ~labels:_ = ()
let gauge     ~name:_ ~labels:_ = ()
let histogram ~name:_ ~labels:_ = ()

let incr      _   = ()
let add       _ _ = ()
let set_gauge _ _ = ()
let observe   _ _ = ()
let dump      ()  = ""
let start_reporter ?stop:_ ~interval_s:_ () = ()
