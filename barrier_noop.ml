(* Заглушка: один поток, координация не нужна *)
type epoch = int
type coordinator = { mutable epoch : int }

let create ~workers:_ ~checkpoint_every:_ = { epoch = 0 }
let worker_ready _c ~worker:_ ~epoch:_    = ()
let wait_all_ready _c ~epoch:_            = ()
let current_epoch c                       = c.epoch
let next_epoch c = c.epoch <- c.epoch + 1; c.epoch
