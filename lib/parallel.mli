(* ============================================================
   Parallel.mli — интерфейс data-parallel выполнения

   Реализации:
     parallel_v4.ml — Thread.create / Thread.join
     parallel_v5.ml — Domain.spawn / Domain.join
   ============================================================ *)

type worker_stats = {
  mutable processed : int;
  mutable emitted   : int;
  mutable wm_seen   : int;
}

val make_stats : unit -> worker_stats
val hash_key : string -> int -> int

(** Data-parallel pipeline.
    Шардирует source по hash(key_of) на N воркеров.
    Каждый воркер запускает свою копию pipeline.
    Sink защищён мьютексом (v4) или вызывается per-domain (v5). *)
val run_parallel_simple :
  ?sink_factory : (int -> ('b -> unit)) ->
  workers  : int ->
  capacity : int ->
  key_of   : ('a -> string) ->
  pipeline : ('a Mf_event.t Stream.t -> 'b Mf_event.t Stream.t) ->
  source   : 'a Mf_event.t Stream.t ->
  sink     : ('b -> unit) ->
  unit ->
  unit
