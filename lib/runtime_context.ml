(* ============================================================
   Runtime_context — ambient-политика persistence.

   Цель: пайплайн НЕ упоминает persistence. Оператор объявляет
   managed-state по имени; решение «хранить ли его durable и как
   сериализовать» принимается ЗДЕСЬ, на краю деплоя, один раз.

   Это аналог Flink RuntimeContext: оператор делает
   getRuntimeContext().getState(descriptor), а хранение/checkpoint —
   забота runtime. У нас контекст устанавливается через
   [with_context] (динамический scope), а managed-state его читает.

   Два режима:
   - Ephemeral — состояние только в памяти, ничего не пишется
     (поведение «без persistence»).
   - Durable — состояние снапшотится в backend на checkpoint-barrier
     и восстанавливается на старте. Сериализация — по codec-политике.
   ============================================================ *)

(* Политика сериализации managed-state.

   Marshal — дефолт: сериализует ЛЮБОЕ замкнуто-свободное значение
   автоматически, пользователь не пишет ни строки. Идеален для
   crash-recovery (тот же бинарник). НЕ устойчив к смене типа между
   версиями — для schema evolution используйте [Codec_registry].

   Codec_registry — сериализаторы, объявленные по имени состояния на
   краю деплоя (не в пайплайне). Для тех кому нужна эволюция схемы. *)
type codec = {
  to_bytes   : Obj.t -> bytes;          (* 'v -> bytes (через Obj, см. ниже) *)
  of_bytes   : bytes -> Obj.t;          (* bytes -> 'v *)
}

type codec_policy =
  | Marshal_codec                       (* авто для любого 'v *)
  | Registry of (string -> codec option) (* name -> codec; None → Marshal *)

type mode =
  | Ephemeral
  | Durable of {
      backend : Persistence_backend.t;
      codecs  : codec_policy;
    }

type t = { mode : mode }

(* Дефолтный контекст — ephemeral. Если пайплайн запущен без
   [with_context], всё работает как «без persistence». *)
let default = { mode = Ephemeral }

(* Ambient-контекст хранится в Ctx_store — на OCaml 5 это
   domain-local (каждый домен свой контекст, без гонок), на OCaml 4 —
   простой ref. Ctx_store — функтор по типу ячейки, инстанцируем его
   нашим типом [t], поэтому приведений через Obj больше нет (A-2). *)
module Store = Ctx_store.Make (struct type nonrec t = t end)

let get () : t =
  match Store.get () with
  | Some ctx -> ctx
  | None -> default

let set_current (ctx : t) : unit =
  Store.set (Some ctx)

(* Выполнить [f] с заданным контекстом, восстановив предыдущий после
   (в т.ч. при исключении). Вложенность поддерживается. *)
let with_context ctx f =
  let saved = Store.get () in            (* t option — типизировано, без Obj *)
  set_current ctx;
  Fun.protect ~finally:(fun () -> Store.set saved) f

(* ── Конструкторы режимов ──────────────────────────────────── *)

let ephemeral = { mode = Ephemeral }

let durable ?(codecs = Marshal_codec) backend =
  { mode = Durable { backend; codecs } }

(* ── Marshal-кодек (дефолт) ────────────────────────────────── *)

(* Marshal любого значения. Состояние операторов замкнуто-свободно
   (Map/float/list/record/variant), поэтому это безопасно. Closures
   бросили бы Marshal — но операторы их в состоянии не держат. *)
let marshal_codec : codec = {
  to_bytes = (fun v -> Marshal.to_bytes v []);
  of_bytes = (fun b -> Marshal.from_bytes b 0);
}

(* Разрешить кодек для состояния с именем [name] по текущей политике. *)
let codec_for policy name : codec =
  match policy with
  | Marshal_codec -> marshal_codec
  | Registry lookup ->
    (match lookup name with Some c -> c | None -> marshal_codec)
