(* In-memory: текущее поведение. Быстро, теряется при рестарте. *)
type t = (string, bytes) Hashtbl.t

let create ()  = Hashtbl.create 64
let get t k    = Hashtbl.find_opt t k
let set t k v  = Hashtbl.replace t k v
let delete t k = Hashtbl.remove t k
let keys t     = Hashtbl.fold (fun k _ a -> k :: a) t []
let size t     = Hashtbl.length t

let snapshot t =
  Marshal.to_bytes (Hashtbl.fold (fun k v a -> (k,v)::a) t []) []

let restore t b =
  Hashtbl.clear t;
  let pairs : (string * bytes) list = Marshal.from_bytes b 0 in
  List.iter (fun (k,v) -> Hashtbl.replace t k v) pairs
