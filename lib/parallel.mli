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
(** [hash_key key n] — детерминированный шард [0, n) для [key]
    (djb2-хеш). Бросает [Invalid_argument] если [n <= 0]. *)

(** Data-parallel pipeline.
    Шардирует source по hash(key_of) на N воркеров.
    Каждый воркер запускает свою копию pipeline.
    Sink защищён мьютексом (v4) или вызывается per-domain (v5).

    [?on_queue_depth] — опциональный хук наблюдаемости: dispatcher
    периодически вызывает его с массивом текущих глубин входных
    каналов (по воркерам). Раннее предупреждение о backpressure:
    растущая глубина = воркеры не успевают. Библиотека только
    {e сообщает} глубину — что делать (gauge, лог, алерт) решает
    вызывающий. *)
val run_parallel_simple :
  ?sink_factory : (int -> ('b -> unit)) ->
  ?on_queue_depth : (int array -> unit) ->
  workers  : int ->
  capacity : int ->
  key_of   : ('a -> string) ->
  pipeline : ('a Mf_event.t Stream.t -> 'b Mf_event.t Stream.t) ->
  source   : 'a Mf_event.t Stream.t ->
  sink     : ('b -> unit) ->
  unit ->
  unit
