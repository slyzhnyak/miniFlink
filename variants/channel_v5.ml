(* ============================================================
   Channel_v5.ml — lock-free канал для OCaml 5

   Два варианта:
   Unbounded — простая Queue без синхронизации (single-thread)
   Bounded   — lock-free SPSC ring buffer через Atomic

   SPSC = Single Producer, Single Consumer.
   Это наш случай: dispatcher → worker (один пишет, один читает).

   Алгоритм Lamport SPSC queue:
   - head и tail хранятся как Atomic.t
   - producer пишет в buf[tail], потом атомарно двигает tail
   - consumer читает из buf[head], потом атомарно двигает head
   - никаких mutex, никаких condition variables
   - cache-line friendly: head и tail в разных словах

   Backpressure: producer крутится в spin loop если полный.
   Domain.cpu_relax() подсказывает процессору что мы в spin wait.

   ── Memory ordering и корректность (важно) ──────────────────

   КОНТРАКТ: bounded-канал корректен ТОЛЬКО как SPSC — ровно один
   producer (зовёт push/try_push) и ровно один consumer (зовёт pop)
   одновременно. Это не проверяется типами (см. ниже про footgun) —
   обеспечивается топологией: dispatcher → worker, один пишет, один
   читает. MPMC-использование (несколько producer'ов или consumer'ов
   на одном канале) НЕ безопасно и приведёт к гонкам/порче.

   Модель памяти: в OCaml 5 [Atomic.get]/[Atomic.set] дают
   sequentially-consistent доступ (per OCaml memory model — атомарные
   операции линеаризуемы и образуют единый total order). На это и
   опирается корректность:

   - Producer: пишет [buf.(tail) <- v] (обычная запись в ячейку, которой
     consumer ещё НЕ владеет, т.к. head != next), ЗАТЕМ [Atomic.set tail].
     SC-семантика set гарантирует, что запись в ячейку не «уедет» после
     публикации tail: consumer, увидевший новый tail, увидит и значение.
   - Consumer: читает [Atomic.get tail], сравнивает с head; если данные
     есть — читает [buf.(head)] (ячейку, которой producer уже не владеет),
     ЗАТЕМ [Atomic.set head], освобождая ячейку. Producer, увидевший новый
     head, знает что ячейка свободна.
   - Ячейкой владеет ровно одна сторона в каждый момент: producer — пока
     не опубликовал tail; consumer — между чтением tail и публикацией head.
     Пересечения нет → гонки за данными нет.

   ABA здесь не применим: head/tail — монотонно растущие индексы по модулю
   cap, не переиспользуемые указатели; сравнивается только равенство
   «пусто/полно», а не «не изменилось ли». Классической ABA-проблемы
   (характерной для CAS по указателю) тут нет, потому что мы не делаем CAS
   по освобождаемой памяти — только set одним владельцем.

   Mutex/Condition используются ТОЛЬКО для блокировки пустого consumer
   (чтобы не жечь CPU в spin при пустом канале) и пробуждения его при
   push/close — они не участвуют в публикации данных, та идёт через
   Atomic. Double-check под мьютексом в pop защищает от потерянного
   сигнала.

   ГРАНИЦА: формального доказательства линеаризуемости здесь нет — это
   инженерный аргумент, опирающийся на SC-семантику OCaml Atomic и
   single-владение ячейкой. Для учебного single-node движка этого
   достаточно; для критичного прод-использования стоило бы прогнать
   модель через verifier.
   ============================================================ *)

type 'a t =
  | Unbounded of { q : 'a Queue.t; mutable closed : bool }
  | Bounded   of {
      buf    : 'a option array;
      cap    : int;
      head   : int Atomic.t;   (* consumer двигает *)
      tail   : int Atomic.t;   (* producer двигает *)
      mutable closed : bool;
      (* Для multi-producer / blocked consumer используем Mutex как fallback *)
      mu       : Mutex.t;
      not_empty: Condition.t;
    }

let make_unbounded () =
  Unbounded { q = Queue.create (); closed = false }

let make_bounded capacity =
  let cap = max 2 capacity in   (* минимум 2 для SPSC *)
  Bounded {
    buf      = Array.make cap None;
    cap;
    head     = Atomic.make 0;
    tail     = Atomic.make 0;
    closed   = false;
    mu       = Mutex.create ();
    not_empty = Condition.create ();
  }

let make_bounded_spsc = make_bounded

let try_push ch v =
  match ch with
  | Unbounded u ->
    if u.closed then false else (Queue.push v u.q; true)
  | Bounded b ->
    let tail = Atomic.get b.tail in
    let next = (tail + 1) mod b.cap in
    (* Backpressure: spin пока не освободится место.

       NB про yield: cpu_relax даёт hint процессору, но НЕ освобождает
       runtime-lock OCaml. На OCaml 5 несколько Thread'ов (через
       threads.posix) делят один Domain и переключаются кооперативно,
       в основном на allocations/I/O. Если producer и consumer — это
       разные Thread'ы (типичный паттерн с Thread.create), busy-spin
       без yield заблокирует consumer навсегда: producer крутит CPU,
       не отпуская runtime-lock, consumer никогда не получает шанс
       сделать pop. Thread.yield освобождает lock сразу — overhead
       ~1мкс на итерацию, незаметно когда мы РЕАЛЬНО ждём.

       Для multi-domain (Domain.spawn) producer'а это не нужно — там
       настоящая параллельность, и yield был бы лишним. Но он не
       вредит: на разных Domain'ах yield ≈ no-op (runtime-lock не
       шарится между Domain'ами). *)
    while Atomic.get b.head = next && not b.closed do
      Domain.cpu_relax ();
      Thread.yield ()
    done;
    if b.closed then false
    else begin
      b.buf.(tail) <- Some v;
      Atomic.set b.tail next;
      (* Сигналим consumer если он заблокирован *)
      Mutex.lock b.mu;
      Condition.signal b.not_empty;
      Mutex.unlock b.mu;
      true
    end

let push ch v = ignore (try_push ch v)

let close ch =
  match ch with
  | Unbounded u -> u.closed <- true
  | Bounded b   ->
    b.closed <- true;
    (* Разбудить заблокированного consumer *)
    Mutex.lock b.mu;
    Condition.broadcast b.not_empty;
    Mutex.unlock b.mu

let pop ch =
  match ch with
  | Unbounded u ->
    if Queue.is_empty u.q then None
    else Some (Queue.pop u.q)
  | Bounded b ->
    let rec wait () =
      let head = Atomic.get b.head in
      let tail = Atomic.get b.tail in
      if head = tail then begin
        if b.closed then None
        else begin
          (* Канал пуст и не закрыт — ждём *)
          Mutex.lock b.mu;
          (* Double-check после захвата мьютекса *)
          if Atomic.get b.head = Atomic.get b.tail && not b.closed then
            Condition.wait b.not_empty b.mu;
          Mutex.unlock b.mu;
          wait ()
        end
      end else begin
        let v = b.buf.(head) in
        b.buf.(head) <- None;           (* освобождаем для GC *)
        Atomic.set b.head ((head + 1) mod b.cap);
        v
      end
    in
    wait ()

let try_pop ch =
  match ch with
  | Unbounded u ->
    if Queue.is_empty u.q then None else Some (Queue.pop u.q)
  | Bounded b ->
    let head = Atomic.get b.head in
    let tail = Atomic.get b.tail in
    if head = tail then None
    else begin
      let v = b.buf.(head) in
      b.buf.(head) <- None;
      Atomic.set b.head ((head + 1) mod b.cap);
      v
    end

let length ch =
  match ch with
  | Unbounded u -> Queue.length u.q
  | Bounded b   ->
    let head = Atomic.get b.head in
    let tail = Atomic.get b.tail in
    (tail - head + b.cap) mod b.cap

let to_stream ch = fun () -> pop ch
