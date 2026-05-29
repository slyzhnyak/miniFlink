(* /dev/null: всё выбрасывается. Для тестов без стейта. *)
type t = unit
let create ()  = ()
let get _ _    = None
let set _ _ _  = ()
let delete _ _ = ()
let keys _     = []
let size _     = 0
let snapshot _ = Bytes.empty
let restore _ _= ()
