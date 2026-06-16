(* Persistence_backend — общий KV-интерфейс для stateful операторов. *)

type t = {
  get    : string -> bytes option;
  set    : string -> bytes -> unit;
  delete : string -> unit;
  keys   : unit -> string list;
}

let of_memory (tbl : (string, bytes) Hashtbl.t) : t = {
  get    = (fun k   -> Hashtbl.find_opt tbl k);
  set    = (fun k v -> Hashtbl.replace tbl k v);
  delete = (fun k   -> Hashtbl.remove tbl k);
  keys   = (fun ()  -> Hashtbl.fold (fun k _ a -> k :: a) tbl []);
}

type 'v persist = {
  backend     : t;
  name        : string;
  serialize   : 'v -> Yojson.Safe.t;
  deserialize : Yojson.Safe.t -> 'v;
}
