type 'a t = {
  encode : 'a -> bytes;
  decode : bytes -> ('a, string) result;
  name   : string;
}

let json ~encode ~decode = {
  encode = (fun v -> Bytes.of_string (Yojson.Safe.to_string (encode v)));
  decode = (fun b ->
    match Yojson.Safe.from_string (Bytes.to_string b) with
    | j -> decode j | exception Yojson.Json_error e -> Error e);
  name = "json";
}

let protobuf ~encode ~decode = {
  encode;
  decode = (fun b -> match decode b with v -> Ok v
            | exception e -> Error (Printexc.to_string e));
  name = "protobuf";
}
