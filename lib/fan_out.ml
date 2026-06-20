(* Fan-out: один источник → N независимых выходов.

   Каждый выход имеет СВОЮ стратегию backpressure (выбирается при
   создании). Один драйвер тянет источник и раздаёт копии события во все
   буферы; политика выхода решает что делать при переполнении ЕГО буфера.

   ПОТОКОБЕЗОПАСЕН: выходы можно читать конкурентно из разных потоков
   (паттерн supervisor — каждый выход в своём пайплайне). Внутри один
   мьютекс на структуру + condition variable: Block-выход, чей буфер
   пуст, а источник упёрся в ДРУГОЙ полный Block-буфер, ждёт на condvar,
   пока тот вычитают (broadcast при каждом pop/advance).

   Backpressure (Block): если Block-буфер полон, источник не
   продвигается дальше этого события, пока буфер не освободят — fan-out
   идёт со скоростью самого медленного Block-выхода. Drop-политики
   никогда не блокируют источник. *)

type backpressure =
  | Block          (* буфер полон → источник не продвигается (no loss) *)
  | Drop_oldest    (* буфер полон → выкинуть самое старое, принять новое *)
  | Drop_newest    (* буфер полон → выкинуть новое событие *)

type 'a outlet = {
  name        : string;
  buffer_cap  : int;
  on_pressure : backpressure;
}

type 'a buf = {
  q               : 'a Mf_event.t Queue.t;
  cap             : int;
  pol             : backpressure;
  oname           : string;
  mutable dropped : int;
}

type 'a t = {
  source              : 'a Mf_event.t Stream.t;
  bufs                : 'a buf array;
  mutable src_done    : bool;
  mutable pending     : 'a Mf_event.t option;
  pending_accepted    : bool array;
  mu                  : Mutex.t;
  cv                  : Condition.t;     (* будит ждущих при pop/advance *)
}

(* все операции ниже вызываются ПОД t.mu *)

let offer (b : 'a buf) (ev : 'a Mf_event.t) : bool =
  if Queue.length b.q < b.cap then (Queue.push ev b.q; true)
  else match b.pol with
    | Block -> false
    | Drop_newest -> b.dropped <- b.dropped + 1; true
    | Drop_oldest ->
      ignore (Queue.pop b.q); Queue.push ev b.q;
      b.dropped <- b.dropped + 1; true

(* продвинуть источник на шаг (под мьютексом). true если был прогресс
   (раздали событие/часть, или источник иссяк). false если упёрлись в
   полный Block-буфер и НИЧЕГО не раздали (надо ждать). *)
let advance (t : 'a t) : bool =
  if t.src_done && t.pending = None then false
  else begin
    let ev = match t.pending with
      | Some e -> Some e
      | None ->
        Array.fill t.pending_accepted 0 (Array.length t.pending_accepted) false;
        (match t.source () with
         | Some e -> Some e
         | None -> t.src_done <- true; None) in
    match ev with
    | None -> true   (* источник иссяк — это прогресс (дошли до конца) *)
    | Some e ->
      let any_new = ref false in
      let all_ok = ref true in
      Array.iteri (fun i b ->
        if not t.pending_accepted.(i) then begin
          if offer b e then (t.pending_accepted.(i) <- true; any_new := true)
          else all_ok := false
        end) t.bufs;
      t.pending <- (if !all_ok then None else Some e);
      !any_new || !all_ok
  end

let fan_out (source : 'a Mf_event.t Stream.t) (outlets : 'a outlet list)
    : 'a Mf_event.t Stream.t list =
  let bufs = Array.of_list (List.map (fun o ->
    { q = Queue.create (); cap = o.buffer_cap; pol = o.on_pressure;
      oname = o.name; dropped = 0 }) outlets) in
  let t = {
    source; bufs; src_done = false; pending = None;
    pending_accepted = Array.make (Array.length bufs) false;
    mu = Mutex.create (); cv = Condition.create ();
  } in
  let make_stream i =
    let b = bufs.(i) in
    fun () ->
      Mutex.lock t.mu;
      (* Весь loop держит t.mu (кроме Condition.wait, который сам
         временно отпускает его). Единственное место где может
         возникнуть исключение — advance t, внутри которого
         вызывается t.source () (пользовательский источник). Если
         источник бросит, mutex остался бы заблокированным навсегда
         (deadlock всех выходов fan_out). Оборачиваем loop так, чтобы
         любое исключение освободило mutex и было проброшено дальше.
         Все штатные выходы (Some/None) сами делают unlock до возврата;
         в момент исключения mutex заведомо удерживается нами (либо
         никогда не отпускался, либо перезахвачен Condition.wait). *)
      let rec loop () =
        if not (Queue.is_empty b.q) then begin
          let ev = Queue.pop b.q in
          (* буфер освободился — возможно, разблокировали advance *)
          Condition.broadcast t.cv;
          Mutex.unlock t.mu;
          Some ev
        end
        else if t.src_done && t.pending = None then begin
          Mutex.unlock t.mu; None
        end
        else if advance t then begin
          Condition.broadcast t.cv;  (* раздали — будим другие выходы *)
          loop ()
        end
        else begin
          (* источник упёрся в чужой полный Block-буфер, наш пуст:
             ждём пока его вычитают (broadcast при pop) *)
          Condition.wait t.cv t.mu;
          loop ()
        end
      in
      (match loop () with
       | r -> r
       | exception e -> Mutex.unlock t.mu; raise e)
  in
  List.mapi (fun i _ -> make_stream i) outlets
