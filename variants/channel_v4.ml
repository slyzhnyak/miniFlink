(* ============================================================
   Channel.ml — bounded channel с backpressure

   Два варианта:
   Unbounded — простая Queue, без блокировки (single-thread)
   Bounded   — ring buffer + Mutex/Condition (multi-thread)

   Bounded channel даёт backpressure автоматически:
   producer блокируется когда канал полный.
   Consumer блокируется когда канал пустой.

   Sentinel None = конец потока (канал закрыт).
   ============================================================ *)

type 'a t =
  | Unbounded of { q : 'a Queue.t; mutable closed : bool }
  | Bounded   of {
      buf     : 'a option array;
      cap     : int;
      mutable head   : int;
      mutable tail   : int;
      mutable count  : int;
      mutable closed : bool;
      mu      : Mutex.t;
      not_full  : Condition.t;
      not_empty : Condition.t;
    }

let make_unbounded () =
  Unbounded { q = Queue.create (); closed = false }

let make_bounded capacity =
  let cap = max 1 capacity in
  Bounded {
    buf       = Array.make cap None;
    cap;
    head      = 0; tail = 0; count = 0; closed = false;
    mu        = Mutex.create ();
    not_full  = Condition.create ();
    not_empty = Condition.create ();
  }

(* try_push: записать значение, вернуть true если доставлено.
   Блокирует если bounded и полный. Возвращает false если канал закрыт
   (например, consumer-воркер упал) — это предотвращает deadlock. *)
let try_push ch v =
  match ch with
  | Unbounded u ->
    if u.closed then false else (Queue.push v u.q; true)
  | Bounded b   ->
    Mutex.lock b.mu;
    while b.count = b.cap && not b.closed do
      Condition.wait b.not_full b.mu
    done;
    let delivered =
      if b.closed then false
      else begin
        b.buf.(b.tail) <- Some v;
        b.tail  <- (b.tail + 1) mod b.cap;
        b.count <- b.count + 1;
        Condition.signal b.not_empty;
        true
      end
    in
    Mutex.unlock b.mu;
    delivered

(* push: совместимость — записать значение, игнорируя статус доставки *)
let push ch v = ignore (try_push ch v)

(* close: закрыть канал. Consumer получит None после последнего элемента. *)
let close ch =
  match ch with
  | Unbounded u ->
    u.closed <- true
  | Bounded b ->
    Mutex.lock b.mu;
    b.closed <- true;
    Condition.broadcast b.not_empty;
    Condition.broadcast b.not_full;
    Mutex.unlock b.mu

(* pop: прочитать значение. Блокирует если bounded и пустой.
   Возвращает None когда закрыт и пуст. *)
let pop ch =
  match ch with
  | Unbounded u ->
    if Queue.is_empty u.q then None
    else Some (Queue.pop u.q)
  | Bounded b ->
    Mutex.lock b.mu;
    while b.count = 0 && not b.closed do
      Condition.wait b.not_empty b.mu
    done;
    let result =
      if b.count = 0 then None      (* закрыт и пуст *)
      else begin
        let v   = b.buf.(b.head) in
        b.buf.(b.head) <- None;     (* освобождаем ссылку для GC *)
        b.head  <- (b.head + 1) mod b.cap;
        b.count <- b.count - 1;
        Condition.signal b.not_full;
        v
      end
    in
    Mutex.unlock b.mu;
    result

(* try_pop: не блокирует, возвращает None если пусто *)
let try_pop ch =
  match ch with
  | Unbounded u ->
    if Queue.is_empty u.q then None else Some (Queue.pop u.q)
  | Bounded b ->
    Mutex.lock b.mu;
    let result =
      if b.count = 0 then None
      else begin
        let v   = b.buf.(b.head) in
        b.buf.(b.head) <- None;
        b.head  <- (b.head + 1) mod b.cap;
        b.count <- b.count - 1;
        Condition.signal b.not_full;
        v
      end
    in
    Mutex.unlock b.mu;
    result

(* Преобразовать channel в Stream.t *)
let to_stream ch =
  fun () -> pop ch

let length ch =
  match ch with
  | Unbounded u -> Queue.length u.q
  | Bounded b   ->
    Mutex.lock b.mu;
    let n = b.count in
    Mutex.unlock b.mu; n
