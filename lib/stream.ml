(** Pull-based ленивый поток.

    Поток — это функция [unit -> 'a option]: каждый вызов возвращает
    следующий элемент, [None] означает конец. Стек вызовов операторов
    {e и есть} граф обработки — никаких колбэков и промежуточных
    буферов (кроме {!flat_map}, где буфер необходим). *)

(** Тип потока элементов ['a]. *)
type 'a t = unit -> 'a option

let of_list xs =
  let q = Queue.of_seq (List.to_seq xs) in
  fun () -> if Queue.is_empty q then None else Some (Queue.pop q)

let empty : 'a t = fun () -> None
let map    f s   = fun () -> Option.map f (s ())
let filter p s   =
  let rec go () = match s () with
    | None             -> None
    | Some x when p x  -> Some x
    | Some _           -> go ()
  in go
let flat_map f s =
  let buf = Queue.create () in
  let rec go () =
    if not (Queue.is_empty buf) then Some (Queue.pop buf) else
    match s () with None -> None | Some x ->
      List.iter (fun y -> Queue.push y buf) (f x); go ()
  in go
let iter  f s = let rec go () = match s () with None -> () | Some x -> f x; go () in go ()
let fold  f z s = let a = ref z in iter (fun x -> a := f !a x) s; !a
let to_list s = List.rev (fold (fun a x -> x :: a) [] s)

let tee (s : 'a t) : 'a t * 'a t =
  let q_a = Queue.create () in
  let q_b = Queue.create () in
  let ended = ref false in
  (* Pull from src — кладём элемент в очередь "другой" копии, возвращаем элемент. *)
  let next_a () =
    if not (Queue.is_empty q_a) then Some (Queue.pop q_a)
    else if !ended then None
    else
      match s () with
      | None -> ended := true; None
      | Some v -> Queue.push v q_b; Some v
  in
  let next_b () =
    if not (Queue.is_empty q_b) then Some (Queue.pop q_b)
    else if !ended then None
    else
      match s () with
      | None -> ended := true; None
      | Some v -> Queue.push v q_a; Some v
  in
  (next_a, next_b)

let split n (s : 'a t) : 'a t list =
  if n <= 0 then invalid_arg "Stream.split: n must be positive";
  if n = 1 then [s]
  else
    let queues = Array.init n (fun _ -> Queue.create ()) in
    let ended = ref false in
    let make_next i () =
      if not (Queue.is_empty queues.(i)) then Some (Queue.pop queues.(i))
      else if !ended then None
      else
        match s () with
        | None -> ended := true; None
        | Some v ->
          (* В очереди других копий кладём элемент, своему возвращаем напрямую. *)
          for j = 0 to n - 1 do
            if j <> i then Queue.push v queues.(j)
          done;
          Some v
    in
    List.init n make_next
