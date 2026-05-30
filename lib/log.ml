type level = Debug | Info | Warning | Error

type event = {
  level   : level;
  message : string;
  fields  : (string * string) list;
  ts_ms   : int;
}

let level_str = function
  | Debug -> "debug" | Info -> "info" | Warning -> "warn" | Error -> "error"

let level_rank = function
  | Debug -> 0 | Info -> 1 | Warning -> 2 | Error -> 3

(* Экранирование строки для JSON *)
let json_escape s =
  let buf = Buffer.create (String.length s + 2) in
  String.iter (fun c ->
    match c with
    | '"'  -> Buffer.add_string buf "\\\""
    | '\\' -> Buffer.add_string buf "\\\\"
    | '\n' -> Buffer.add_string buf "\\n"
    | '\r' -> Buffer.add_string buf "\\r"
    | '\t' -> Buffer.add_string buf "\\t"
    | c when Char.code c < 0x20 ->
      Buffer.add_string buf (Printf.sprintf "\\u%04x" (Char.code c))
    | c -> Buffer.add_char buf c) s;
  Buffer.contents buf

let to_json e =
  let buf = Buffer.create 128 in
  Buffer.add_string buf "{";
  Buffer.add_string buf (Printf.sprintf "\"ts\":%d," e.ts_ms);
  Buffer.add_string buf (Printf.sprintf "\"level\":\"%s\"," (level_str e.level));
  Buffer.add_string buf (Printf.sprintf "\"msg\":\"%s\"" (json_escape e.message));
  List.iter (fun (k, v) ->
    Buffer.add_string buf
      (Printf.sprintf ",\"%s\":\"%s\"" (json_escape k) (json_escape v)))
    e.fields;
  Buffer.add_string buf "}";
  Buffer.contents buf

(* Sink по умолчанию: JSON в stderr *)
let default_sink e = Printf.eprintf "%s\n%!" (to_json e)

let sink = ref default_sink
let min_level = ref Info
let mu = Mutex.create ()

let set_sink f = sink := f
let set_level l = min_level := l

let now_ms () = int_of_float (Unix.gettimeofday () *. 1000.)

let emit level ?(fields = []) message =
  if level_rank level >= level_rank !min_level then begin
    let e = { level; message; fields; ts_ms = now_ms () } in
    Mutex.lock mu;
    (try !sink e with _ -> ());   (* sink приложения не должен ронять пайплайн *)
    Mutex.unlock mu
  end

let debug ?fields m = emit Debug ?fields m
let info  ?fields m = emit Info  ?fields m
let warn  ?fields m = emit Warning ?fields m
let error ?fields m = emit Error ?fields m
