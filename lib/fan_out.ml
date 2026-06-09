(* Fan-out: один источник → N независимых выходов.

   Каждый выход имеет СВОЮ стратегию backpressure (выбирается при
   создании). Один драйвер тянет источник и раздаёт копии события во все
   буферы; политика выхода решает что делать при переполнении ЕГО буфера.

   Модель pull-driven без фоновых потоков: когда выход просит следующее
   событие, а его буфер пуст, драйвер продвигает источник на шаг
   (раздаёт всем буферам по их политике). Детерминированно, без гонок.

   Backpressure (Block): если Block-буфер полон, источник не
   продвигается дальше этого события, пока буфер не освободят (его
   должны вычитывать) — fan-out идёт со скоростью самого медленного
   Block-выхода. Drop-политики никогда не блокируют источник. *)

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
}

let offer (b : 'a buf) (ev : 'a Mf_event.t) : bool =
  if Queue.length b.q < b.cap then (Queue.push ev b.q; true)
  else match b.pol with
    | Block -> false
    | Drop_newest -> b.dropped <- b.dropped + 1; true
    | Drop_oldest ->
      ignore (Queue.pop b.q); Queue.push ev b.q;
      b.dropped <- b.dropped + 1; true

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
    | None -> false
    | Some e ->
      let all_ok = ref true in
      Array.iteri (fun i b ->
        if not t.pending_accepted.(i) then begin
          if offer b e then t.pending_accepted.(i) <- true
          else all_ok := false
        end) t.bufs;
      t.pending <- (if !all_ok then None else Some e);
      true
  end

let fan_out (source : 'a Mf_event.t Stream.t) (outlets : 'a outlet list)
    : 'a Mf_event.t Stream.t list =
  let bufs = Array.of_list (List.map (fun o ->
    { q = Queue.create (); cap = o.buffer_cap; pol = o.on_pressure;
      oname = o.name; dropped = 0 }) outlets) in
  let t = {
    source; bufs; src_done = false; pending = None;
    pending_accepted = Array.make (Array.length bufs) false;
  } in
  let make_stream i =
    let b = bufs.(i) in
    fun () ->
      let rec loop () =
        if not (Queue.is_empty b.q) then Some (Queue.pop b.q)
        else if t.src_done && t.pending = None then None
        else begin
          (* запоминаем прогресс источника: если advance не двигает
             источник (застрял на полном Block-буфере ДРУГОГО выхода) и
             наш буфер не пополнился — мы в однопоточном дедлоке: этот
             выход надо читать вперемешку с тем Block-выходом. Возвращаем
             None как сигнал «сейчас пусто», а не зависаем. *)
          let before_done = t.src_done in
          let had_pending = t.pending <> None in
          if advance t then begin
            if not (Queue.is_empty b.q) then loop ()
            else if had_pending && t.pending <> None
                    && before_done = t.src_done then
              (* pending как был застрявшим, наш буфер пуст → не крутимся *)
              None
            else loop ()
          end
          else if not (Queue.is_empty b.q) then Some (Queue.pop b.q)
          else None
        end
      in loop ()
  in
  List.mapi (fun i _ -> make_stream i) outlets
