(** Supervisor: запуск нескольких пайплайнов с изоляцией сбоев. Стратегия
    при падении выбирается для каждого пайплайна при создании. *)

(** Что делать при падении пайплайна. *)
type on_failure =
  | Restart of { max_retries : int; backoff_ms : int }
      (** перезапустить до [max_retries] раз. Если у пайплайна durable
          state, его [run] сам восстанавливается из checkpoint при
          повторном вызове — supervisor просто зовёт [run] снова. *)
  | Isolate
      (** пометить мёртвым, остальные пайплайны продолжают. *)
  | Crash_all
      (** пробросить сбой наружу (для критичных пайплайнов). *)

type pipeline_spec = {
  label      : string;
  run        : unit -> unit;   (** запуск пайплайна; при Restart вызывается
                                   повторно, поэтому должен сам уметь
                                   восстановиться из своего store *)
  on_failure : on_failure;
}

type status = [ `Ok | `Failed ]

(** Поднимается при сбое пайплайна с политикой [Crash_all]. *)
exception Critical_failure of string * exn

(** Запустить все пайплайны (каждый в своём потоке), вернуть статус по
    label. Не бросает на [Isolate]-сбоях; [Crash_all] пробрасывает
    {!Critical_failure}. *)
val supervise_result : pipeline_spec list -> (string * status) list

(** Как {!supervise_result}, но без возврата статусов. *)
val supervise : pipeline_spec list -> unit
