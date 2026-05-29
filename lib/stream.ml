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
