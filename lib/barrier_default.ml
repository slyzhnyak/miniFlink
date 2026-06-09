(* ============================================================
   Barrier_default.ml — Chandy-Lamport distributed snapshot

   ⚠️  СТАТУС: референсная реализация, НЕ используется рабочим путём.
   run_exactly_once в checkpoint_parallel.ml содержит собственный
   координатор (с учётом упавших воркеров — alive-set). Этот модуль
   оставлен как самостоятельная иллюстрация barrier-алгоритма. Если
   будешь подключать его в реальный путь — учти: (1) сброс ready в
   wait_all_ready и инъекция следующего epoch должны быть согласованы,
   иначе worker_ready нового epoch может потеряться; (2) нет обработки
   упавших воркеров (wait_all_ready ждёт всех N — при крахе залипнет,
   ср. R1 в checkpoint_parallel).

   Алгоритм:
   1. coordinator.next_epoch() → инкрементирует epoch,
      рассылает Barrier(epoch) всем воркерам через их каналы
   2. Каждый воркер при получении Barrier:
      - сохраняет свой стейт (snapshot)
      - вызывает worker_ready(coordinator, epoch)
      - продолжает обработку
   3. wait_all_ready блокирует пока все N воркеров не готовы
   4. После этого coordinator делает финальный checkpoint commit
   ============================================================ *)
type epoch = int

type coordinator = {
  workers        : int;
  checkpoint_every: int;
  mutable epoch  : int;
  ready          : bool array;   (* ready.(i) = true когда воркер i готов *)
  mu             : Mutex.t;
  all_ready      : Condition.t;
}

let create ~workers ~checkpoint_every = {
  workers;
  checkpoint_every;
  epoch  = 0;
  ready  = Array.make workers false;
  mu     = Mutex.create ();
  all_ready = Condition.create ();
}

let worker_ready c ~worker ~epoch:_ =
  Mutex.lock c.mu;
  c.ready.(worker) <- true;
  if Array.for_all Fun.id c.ready then
    Condition.broadcast c.all_ready;
  Mutex.unlock c.mu

let wait_all_ready c ~epoch:_ =
  Mutex.lock c.mu;
  while not (Array.for_all Fun.id c.ready) do
    Condition.wait c.all_ready c.mu
  done;
  (* Сбросить флаги для следующего epoch *)
  Array.fill c.ready 0 c.workers false;
  Mutex.unlock c.mu

let current_epoch c = c.epoch

let next_epoch c =
  Mutex.lock c.mu;
  c.epoch <- c.epoch + 1;
  let e = c.epoch in
  Mutex.unlock c.mu;
  e
