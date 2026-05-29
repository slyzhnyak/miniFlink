(* Реальный handler: SIGTERM → graceful drain → exit *)
let requested = ref false
let mu        = Mutex.create ()
let cond      = Condition.create ()

let request () =
  Mutex.lock mu;
  requested := true;
  Condition.broadcast cond;
  Mutex.unlock mu

let is_requested () = !requested

let register ~on_shutdown =
  Sys.set_signal Sys.sigterm (Sys.Signal_handle (fun _ ->
    Printf.eprintf "[shutdown] SIGTERM received, draining...\n%!";
    on_shutdown ();
    request ()
  ));
  Sys.set_signal Sys.sigint (Sys.Signal_handle (fun _ ->
    Printf.eprintf "[shutdown] SIGINT received, draining...\n%!";
    on_shutdown ();
    request ()
  ))

let wait () =
  Mutex.lock mu;
  while not !requested do
    Condition.wait cond mu
  done;
  Mutex.unlock mu
