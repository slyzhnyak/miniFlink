(* ============================================================
   Barrier.mli — координация checkpoint между воркерами

   Chandy-Lamport: JobManager инжектирует barrier в потоки,
   каждый воркер сохраняет стейт когда его получает,
   commit только когда все N воркеров ответили готовы.
   ============================================================ *)

type epoch = int

(** Координатор: живёт в отдельном потоке, управляет epoch *)
type coordinator

(** Создать координатор для N воркеров *)
val create : workers:int -> checkpoint_every:int -> coordinator

(** Воркер i сигнализирует что достиг barrier для данного epoch *)
val worker_ready : coordinator -> worker:int -> epoch:epoch -> unit

(** Ждать пока все воркеры не будут готовы (блокирует) *)
val wait_all_ready : coordinator -> epoch:epoch -> unit

(** Текущий epoch *)
val current_epoch : coordinator -> epoch

(** Trigger следующего checkpoint *)
val next_epoch : coordinator -> epoch
