(* Единицы времени — просто int (миллисекунды) *)
type t = int
let ms      n = n
let seconds n = n * 1_000
let minutes n = n * 60_000
let hours   n = n * 3_600_000
