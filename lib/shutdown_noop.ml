(* shutdown_noop: ничего не делает *)
let register ~on_shutdown:_ = ()
let is_requested ()         = false
let wait ()                 = ()
let request ()              = ()
