(* Replayable_source — тонкая обёртка над list. *)

type 'a t = {
  log : 'a array;  (* immutable после создания *)
}

let of_list xs = { log = Array.of_list xs }

let length t = Array.length t.log

let read_from ?(offset = 0) (t : 'a t) : 'a Stream.t * (unit -> int) =
  if offset < 0 || offset > Array.length t.log then
    invalid_arg (Printf.sprintf
      "Replayable_source.read_from: offset %d out of bounds [0..%d]"
      offset (Array.length t.log));
  let pos = ref offset in
  let stream () =
    if !pos >= Array.length t.log then None
    else begin
      let v = t.log.(!pos) in
      incr pos;
      Some v
    end
  in
  let get_offset () = !pos in
  (stream, get_offset)
