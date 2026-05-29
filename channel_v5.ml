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

let try_push ch v =
  match ch with
  | Unbounded u ->
    if u.closed then false else (Queue.push v u.q; true)
  | Bounded b ->
    let tail = Atomic.get b.tail in
    let next = (tail + 1) mod b.cap in
    (* Backpressure: spin пока не освободится место *)
    while Atomic.get b.head = next && not b.closed do
      Domain.cpu_relax ()
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
