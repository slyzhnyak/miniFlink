(* ============================================================
   Keyed.ml — type class "имеет ключ"

   Любой тип реализующий KEYED автоматически работает
   с window, enrich, dedup без явного ~key: параметра.

   Это убирает повторение (fun (t:T.telemetry) -> t.T.device_id)
   в каждом операторе.
   ============================================================ *)

module type S = sig
  type t
  val key : t -> string
end

(* Вспомогательный модуль для создания keyed типа *)
module Make (T : sig type t val key : t -> string end) = struct
  include T
end

(* of_fun: создать first-class Keyed.S из функции инлайн — для случаев,
   когда не хочется объявлять модуль заранее. Работает с любым
   оператором, принимающим (module Keyed.S). *)
let of_fun (type a) (by : a -> string) : (module S with type t = a) =
  (module struct type t = a let key = by end)
