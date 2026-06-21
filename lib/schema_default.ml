(* Версионированный codec: первые 2 байта = version (big-endian uint16) *)
type version = int
type 'a t = {
  enc     : 'a -> bytes;
  dec     : bytes -> ('a, string) result;
  ver     : version;
  migrate : (version -> bytes -> bytes) option;
}

let make ~version ~encode ~decode ?migrate () =
  (* version пишется в 2 байта (uint16 big-endian). Значение вне
     [0, 65535] молча потеряло бы старшие биты при encode, и decode
     прочитал бы другую версию — тихая рассинхронизация схемы.
     Падаем явно при создании codec'а. *)
  if version < 0 || version > 0xFFFF then
    invalid_arg (Printf.sprintf
      "Schema.make: version %d вне диапазона uint16 [0, 65535]" version);
  { enc = encode; dec = decode; ver = version; migrate }

let encode t v =
  let payload = t.enc v in
  let result  = Bytes.create (2 + Bytes.length payload) in
  Bytes.set result 0 (Char.chr ((t.ver lsr 8) land 0xFF));
  Bytes.set result 1 (Char.chr (t.ver land 0xFF));
  Bytes.blit payload 0 result 2 (Bytes.length payload);
  result

let decode t b =
  if Bytes.length b < 2 then Error "payload too short"
  else
    let ver     = (Char.code (Bytes.get b 0) lsl 8)
                  lor Char.code (Bytes.get b 1) in
    let payload = Bytes.sub b 2 (Bytes.length b - 2) in
    if ver = t.ver then t.dec payload
    else match t.migrate with
      | Some f -> t.dec (f ver payload)
      | None ->
        (* Неизвестная версия и нет миграции — это НЕ тихий проброс.
           Молча декодировать payload чужой версии = риск порчи данных.
           Явная ошибка: вызывающий узнает что схема рассинхронизирована. *)
        Error (Printf.sprintf
          "unknown schema version %d (current %d), no migration provided"
          ver t.ver)

let current_version t = t.ver
