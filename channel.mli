(* ============================================================
   Channel.mli — интерфейс канала между операторами

   Реализации:
     channel_v4.ml — Mutex + Condition (OCaml 4 Thread)
     channel_v5.ml — Atomic lock-free SPSC (OCaml 5 Domain)

   Makefile выбирает нужную и копирует в channel.ml
   ============================================================ *)

type 'a t

val make_unbounded : unit -> 'a t
val make_bounded   : int  -> 'a t

val push     : 'a t -> 'a -> unit   (* блокирует если bounded и полный *)
val pop      : 'a t -> 'a option    (* блокирует если bounded и пустой; None = закрыт *)
val try_pop  : 'a t -> 'a option    (* не блокирует *)
val close    : 'a t -> unit
val length   : 'a t -> int
val to_stream: 'a t -> 'a Stream.t
