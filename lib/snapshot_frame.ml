(* Snapshot_frame — самоописывающая рамка вокруг снапшот-байтов
   (аудит 2026-07, находка N-9).

   Проблема: backend'ы и checkpoint декодировали снапшот сырым
   [Marshal.from_bytes b 0]. Marshal в OCaml НЕ безопасен на
   недоверенном/повреждённом вводе: подтверждённо на практике вход
   [Marshal.to_bytes [(1,2);(3,4)]] (список пар int вместо
   (string*bytes)) проходит restore без ошибки, а первое же обращение к
   «ключу» как к строке даёт SIGSEGV (type confusion читает int как
   указатель).

   Рамка отсекает реальный сценарий порчи — битый диск, обрезанные
   байты, перепутанный формат, чужой снапшот — ДО опасного from_bytes:
     MAGIC(4) | version(1) | payload_len(4, BE) | md5(payload)(16) | payload
   restore проверяет магию, длину и контрольную сумму; несовпадение →
   [Failure] с внятным сообщением, а не segfault.

   Ограничение (честно): рамка защищает от СЛУЧАЙНОЙ порчи, не от
   НАМЕРЕННОЙ атаки — злоумышленник, знающий формат, пересчитает md5.
   Полная защита требует смены Marshal на типобезопасный сериализатор
   (свой формат). Рамка — дешёвый барьер, закрывающий практический риск;
   намеренная защита — отдельная задача (см. FUZZING.md). *)

let magic = "MFS1"                (* miniFlink snapshot v1 *)
let version = 1
let header_len = 4 + 1 + 4 + 16   (* magic + ver + len + md5 *)

(* Обернуть сырые marshal-байты в рамку. *)
let wrap (payload : bytes) : bytes =
  let plen = Bytes.length payload in
  let out = Bytes.create (header_len + plen) in
  Bytes.blit_string magic 0 out 0 4;
  Bytes.set out 4 (Char.chr version);
  (* payload_len, big-endian uint32 *)
  Bytes.set out 5 (Char.chr ((plen lsr 24) land 0xFF));
  Bytes.set out 6 (Char.chr ((plen lsr 16) land 0xFF));
  Bytes.set out 7 (Char.chr ((plen lsr 8)  land 0xFF));
  Bytes.set out 8 (Char.chr (plen land 0xFF));
  (* md5 полезной нагрузки *)
  let digest = Digest.bytes payload in   (* 16 байт *)
  Bytes.blit_string digest 0 out 9 16;
  Bytes.blit payload 0 out header_len plen;
  out

(* Проверить рамку и вернуть сырые marshal-байты.
   @raise Failure если магия/версия/длина/контрольная сумма не сошлись. *)
let unwrap (framed : bytes) : bytes =
  let total = Bytes.length framed in
  if total < header_len then
    failwith "Snapshot_frame: слишком короткий вход (нет заголовка)";
  if Bytes.sub_string framed 0 4 <> magic then
    failwith "Snapshot_frame: неверная магия (чужой формат или порча)";
  let ver = Char.code (Bytes.get framed 4) in
  if ver <> version then
    failwith (Printf.sprintf "Snapshot_frame: версия %d, ожидалась %d" ver version);
  let plen =
    (Char.code (Bytes.get framed 5) lsl 24)
    lor (Char.code (Bytes.get framed 6) lsl 16)
    lor (Char.code (Bytes.get framed 7) lsl 8)
    lor (Char.code (Bytes.get framed 8)) in
  if plen <> total - header_len then
    failwith (Printf.sprintf
      "Snapshot_frame: заявленная длина payload %d не совпадает с фактической %d"
      plen (total - header_len));
  let payload = Bytes.sub framed header_len plen in
  let expected = Bytes.sub_string framed 9 16 in
  if Digest.bytes payload <> expected then
    failwith "Snapshot_frame: контрольная сумма payload не сошлась (порча)";
  payload

(* Удобные обёртки для типичного snapshot/restore. *)
let marshal_wrap (v : 'a) : bytes = wrap (Marshal.to_bytes v [])

(* unwrap + Marshal.from_bytes. Marshal остаётся последним шагом, но
   вызывается только на данных, прошедших магию+длину+crc — случайный
   мусор до него не дойдёт. *)
let marshal_unwrap (framed : bytes) : 'a =
  let payload = unwrap framed in
  Marshal.from_bytes payload 0
