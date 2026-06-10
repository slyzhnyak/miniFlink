(** Type class «имеет ключ».

    Любой тип, реализующий {!S}, работает с [window], [enrich], [dedup]
    без явного [~key] в каждом операторе — ключ описывается один раз.
    Это убирает повторение [(fun t -> t.device_id)] во всех вызовах. *)

(** Тип с ключом группировки. *)
module type S = sig
  type t
  (** Извлечь строковый ключ из значения. *)
  val key : t -> string
end

(** Создать keyed-модуль из типа и функции ключа:
    {[
      module Sensor = Keyed.Make (struct
        type t = reading
        let key r = r.sensor_id
      end)
    ]} *)
module Make (T : sig type t val key : t -> string end) : S with type t = T.t

val of_fun : ('a -> string) -> (module S with type t = 'a)
(** Создать {!S} из функции ключа инлайн, без объявления модуля заранее:
    {[ Pipe.window (Keyed.of_fun (fun e -> e.id)) spec stream ]}
    Работает с любым оператором, принимающим [(module Keyed.S)]. *)
