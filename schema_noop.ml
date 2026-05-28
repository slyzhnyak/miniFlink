(* schema_noop: без версионирования — decode-or-fail *)
type version = int
type 'a t = { enc: 'a -> bytes; dec: bytes -> ('a, string) result; ver: version }

let make ~version ~encode ~decode ?migrate:_ () =
  { enc = encode; dec = decode; ver = version }

let encode t v = t.enc v
let decode t b = t.dec b
let current_version t = t.ver
