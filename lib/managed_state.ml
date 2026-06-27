(* ============================================================
   Managed_state — keyed-состояние оператора с ПРОЗРАЧНОЙ
   persistence.

   Оператор использует это ВМЕСТО сырого Hashtbl. Снаружи это
   обычная keyed-map (get/set/remove/fold). Но:

   - При создании managed-state читает ambient Runtime_context.
   - Если контекст Ephemeral — это просто Hashtbl в памяти.
   - Если Durable — restore из backend на старте, и snapshot в
     backend по команде [checkpoint] (оператор зовёт её на
     watermark-barrier).

   Сериализация — по codec-политике контекста (Marshal по дефолту).
   Оператор НЕ пишет сериализаторов и НЕ ветвит логику по наличию
   persistence: один и тот же код работает в обоих режимах.

   Namespace: имя [~name] задаётся оператором (стабильно при
   рестарте), пользователь его не видит. Ключ в backend —
   "{name}:{user_key}".

   ── Про Obj.repr/Obj.obj (type erasure) ──
   Внутри значения хранятся в [Hashtbl] как типизированное [v], но
   codec контекста ([to_bytes]/[of_bytes]) работает с [Obj.t] —
   единым стёртым типом, чтобы одна реализация persistence годилась
   для любого пользовательского [v]. Поэтому на границе с codec стоят
   [Obj.repr v] (запись) и [Obj.obj bytes-result] (чтение).

   Почему это безопасно здесь, несмотря на формальную
   неспецифицированность Obj для immediate-значений:
   - Приведение заперто в этом модуле; наружу торчит типобезопасный
     API ([get]/[set]/[fold]), вызывающий код Obj не видит.
   - [Obj.repr] — тождество на уровне представления (boxed и
     immediate), а [to_bytes] по умолчанию [Marshal], который
     корректно сериализует любое представление.
   - На чтении тип [v] восстанавливается тем же codec'ом того же
     namespace, что и при записи, — рассогласования типов быть не
     может, пока [~name] стабилен (а он стабилен по контракту).
   Альтернатива (полиморфный codec через GADT) усложнила бы тип ради
   устранения теоретического, не наблюдаемого на практике риска;
   осознанно оставлено как есть. Замена, если понадобится,
   локализована в этом модуле.
   ============================================================ *)

type ('k, 'v) t = {
  tbl       : (string, 'v) Hashtbl.t;   (* строковый ключ — нормализованный 'k *)
  key_str   : 'k -> string;             (* как сериализовать ключ в строку *)
  key_unstr : string -> 'k;             (* обратно (для fold по 'k) *)
  name      : string;                   (* namespace оператора *)
  durable   : Persistence_backend.t option;
  codec     : Runtime_context.codec;
}

(* Префикс ключей этого состояния в backend. *)
let prefix t = t.name ^ ":"

let backend_key t k = prefix t ^ t.key_str k

(* Создать managed-state. Читает текущий Runtime_context:
   - Ephemeral → пустой Hashtbl, durable = None.
   - Durable   → restore всех записей из backend по префиксу name.

   [~name] — namespace оператора (стабильный).
   [~key_str]/[~key_unstr] — биекция ключа в строку (для backend и
   fold). Для строковых ключей это identity. *)
let create (type k v)
    ~(name : string)
    ~(key_str : k -> string)
    ~(key_unstr : string -> k)
    () : (k, v) t =
  let ctx = Runtime_context.get () in
  let tbl : (string, v) Hashtbl.t = Hashtbl.create 64 in
  let durable, codec =
    match ctx.Runtime_context.mode with
    | Runtime_context.Ephemeral ->
      (None, Runtime_context.marshal_codec)
    | Runtime_context.Durable { backend; codecs } ->
      (Some backend, Runtime_context.codec_for codecs name)
  in
  let t = { tbl; key_str; key_unstr; name; durable; codec } in
  (* Restore на старте, если durable. *)
  (match durable with
   | None -> ()
   | Some be ->
     let pfx = prefix t in
     let plen = String.length pfx in
     List.iter (fun bk ->
       if String.length bk >= plen && String.sub bk 0 plen = pfx then begin
         match be.Persistence_backend.get bk with
         | None -> ()
         | Some bytes ->
           (* Obj.obj: стёртый тип codec'а → типизированное v; safe, см.
              заголовок модуля «Про Obj.repr/Obj.obj» *)
           let v : v = Obj.obj (t.codec.Runtime_context.of_bytes bytes) in
           let ukey = String.sub bk plen (String.length bk - plen) in
           Hashtbl.replace t.tbl ukey v
       end
     ) (be.Persistence_backend.keys ()));
  t

(* ── Операции (как у Hashtbl, оператор не видит persistence) ── *)

let get t k = Hashtbl.find_opt t.tbl (t.key_str k)

let set t k v = Hashtbl.replace t.tbl (t.key_str k) v

let remove t k = Hashtbl.remove t.tbl (t.key_str k)

let mem t k = Hashtbl.mem t.tbl (t.key_str k)

(* fold по (k, v). Восстанавливаем 'k из строки. *)
let fold t f init =
  Hashtbl.fold (fun ks v acc -> f (t.key_unstr ks) v acc) t.tbl init

let iter t f = Hashtbl.iter (fun ks v -> f (t.key_unstr ks) v) t.tbl

let size t = Hashtbl.length t.tbl

(* ── Checkpoint (зовёт ОПЕРАТОР на barrier; noop если ephemeral) ──

   Записывает текущее состояние в backend. В ephemeral-режиме —
   ничего не делает, поэтому оператор может звать checkpoint
   безусловно: один и тот же код в обоих режимах. *)
let checkpoint t =
  match t.durable with
  | None -> ()
  | Some be ->
    Hashtbl.iter (fun ks v ->
      let bk = prefix t ^ ks in
      be.Persistence_backend.set bk (t.codec.Runtime_context.to_bytes (Obj.repr v))
    ) t.tbl

(* Снапшот ОДНОГО ключа в backend (точечный checkpoint). Для
   операторов, которые персистят per-key на каждое изменение
   (process_keyed, silence_age), а не batch'ем на watermark.
   В ephemeral — noop. *)
let checkpoint_key t k =
  match t.durable with
  | None -> ()
  | Some be ->
    let ks = t.key_str k in
    match Hashtbl.find_opt t.tbl ks with
    | Some v -> be.Persistence_backend.set (prefix t ^ ks)
                  (t.codec.Runtime_context.to_bytes (Obj.repr v))
    | None -> be.Persistence_backend.delete (prefix t ^ ks)

(* Удалить запись и из backend (для eviction старых окон/ключей). *)
let evict t k =
  let ks = t.key_str k in
  Hashtbl.remove t.tbl ks;
  match t.durable with
  | None -> ()
  | Some be -> be.Persistence_backend.delete (prefix t ^ ks)

(* Durable ли это состояние (для оператора — нужно ли вообще
   делать checkpoint-работу; обычно не нужно проверять, checkpoint
   сам noop). *)
let is_durable t = t.durable <> None

(* Хелпер: managed-state со строковым ключом (частый случай). *)
let create_string ~name () : (string, 'v) t =
  create ~name ~key_str:Fun.id ~key_unstr:Fun.id ()
