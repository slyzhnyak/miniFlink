(* См. hash.mli. djb2-хеш для детерминированного шардирования. *)

let key (s : string) (n : int) : int =
  if n <= 0 then invalid_arg "Hash.key: число шардов должно быть > 0";
  let h = ref 5381 in
  String.iter (fun ch -> h := !h * 33 + Char.code ch) s;
  (!h land max_int) mod n
