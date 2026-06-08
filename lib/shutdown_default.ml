(* Реальный handler: SIGTERM/SIGINT → graceful drain → exit.

   ВАЖНО — async-signal-safety. Обработчик сигнала на OCaml выполняется
   в контексте прерванного потока; брать там Mutex (Log.emit, request с
   Condition) или звать произвольный on_shutdown НЕБЕЗОПАСНО: если
   прерванный поток уже держит тот же mutex (например log.mu), получаем
   дедлок (Mutex в OCaml не реентрантный). POSIX async-signal-safety
   запрещает lock/printf/malloc в обработчике.

   Поэтому обработчик делает ТОЛЬКО Atomic.set флага (async-signal-safe).
   Логирование, on_shutdown и пробуждение — в основном потоке через
   notice_if_requested (вызывается из wait и может опрашиваться из
   цикла обработки). *)

let requested   = Atomic.make false
let signal_name = Atomic.make ""        (* какой сигнал пришёл, для лога *)
let on_shutdown_ref = ref (fun () -> ())
let acted       = Atomic.make false      (* лог/on_shutdown ровно раз *)

(* Программный запрос shutdown (не из обработчика сигнала). *)
let request () = Atomic.set requested true

(* Проверка флага. Заодно выполняет отложенные действия (лог +
   on_shutdown) если сигнал пришёл — безопасно, т.к. зовётся из
   основного цикла обработки, не из обработчика сигнала. Это позволяет
   циклам, опрашивающим is_requested, корректно запустить on_shutdown
   без явного вызова wait. *)
let rec is_requested () =
  let r = Atomic.get requested in
  if r then notice_if_requested ();
  r

(* выполнить отложенные действия ровно один раз (лог + on_shutdown);
   объявлено через and чтобы is_requested мог его звать *)
and notice_if_requested () =
  if Atomic.get requested && not (Atomic.exchange acted true) then begin
    let sg = Atomic.get signal_name in
    if sg <> "" then
      Log.warn ~fields:[("signal", sg)] "shutdown signal received, draining";
    (try !on_shutdown_ref () with _ -> ())
  end

let register ~on_shutdown =
  on_shutdown_ref := on_shutdown;
  let handler name = Sys.Signal_handle (fun _ ->
    (* ТОЛЬКО async-signal-safe операции: атомарные записи *)
    Atomic.set signal_name name;
    Atomic.set requested true) in
  Sys.set_signal Sys.sigterm (handler "SIGTERM");
  Sys.set_signal Sys.sigint  (handler "SIGINT")

(* Ждать запроса shutdown. Опрашиваем атомарный флаг с коротким сном —
   обработчик сигнала не может безопасно разбудить через Condition, а
   busy-wait не нужен (это путь завершения, не hot path). При получении
   сигнала выполняем отложенные действия. *)
let wait () =
  while not (Atomic.get requested) do
    Thread.delay 0.05
  done;
  notice_if_requested ()
