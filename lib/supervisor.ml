(* Supervisor: запускает несколько пайплайнов с изоляцией сбоев.

   Каждый пайплайн объявляет стратегию при падении (выбирается при
   создании):
   - Restart: перезапустить до max_retries с backoff. Если у пайплайна
     durable state (checkpoint), его [run] сам восстанавливается из
     store при повторном вызове — supervisor не знает внутренностей EO,
     просто зовёт run снова, а пайплайн поднимается с checkpoint.
   - Isolate: пометить мёртвым, остальные продолжают (как alive-set у
     воркеров в EO).
   - Crash_all: пробросить сбой наружу — для критичных пайплайнов, где
     деградация недопустима.

   На OCaml 4 пайплайны идут в Thread (параллелизм оркестрации — это
   ожидание I/O источников, не CPU-bound, поэтому GIL не мешает). *)

type on_failure =
  | Restart of { max_retries : int; backoff_ms : int }
  | Isolate
  | Crash_all

type pipeline_spec = {
  label      : string;
  run        : unit -> unit;
  on_failure : on_failure;
}

type status = [ `Ok | `Failed ]

exception Critical_failure of string * exn

(* запустить один пайплайн по его политике; вернуть статус.
   Crash_all → пробрасываем Critical_failure. *)
let run_one (spec : pipeline_spec) : status =
  match spec.on_failure with
  | Crash_all ->
    (try spec.run (); `Ok
     with e -> raise (Critical_failure (spec.label, e)))
  | Isolate ->
    (try spec.run (); `Ok
     with e ->
       Log.error ~fields:[("pipeline", spec.label);
                          ("error", Printexc.to_string e)]
         "pipeline isolated after failure";
       `Failed)
  | Restart { max_retries; backoff_ms } ->
    let rec attempt n =
      match spec.run () with
      | () -> `Ok
      | exception e ->
        if n >= max_retries then begin
          Log.error ~fields:[("pipeline", spec.label);
                             ("retries", string_of_int n);
                             ("error", Printexc.to_string e)]
            "pipeline gave up after retries";
          `Failed
        end else begin
          Log.warn ~fields:[("pipeline", spec.label);
                            ("attempt", string_of_int n);
                            ("error", Printexc.to_string e)]
            "pipeline failed, restarting (will resume from checkpoint if durable)";
          if backoff_ms > 0 then Thread.delay (float_of_int backoff_ms /. 1000.);
          attempt (n + 1)
        end
    in attempt 1

(* supervise_result: запускает все пайплайны (каждый в своём потоке),
   возвращает статус по label. Не бросает на Isolate-сбоях; Crash_all
   пробрасывается. *)
let supervise_result (specs : pipeline_spec list) : (string * status) list =
  let results = Array.make (List.length specs) ("", `Ok) in
  let results_mu = Mutex.create () in
  (* Копим ВСЕ критические сбои, а не только первый (H-2): при
     одновременном падении нескольких Crash_all-пайплайнов раньше
     терялась диагностика всех, кроме первого. Список копится под
     mutex'ом; в конце логируем все, а пробрасываем первый по порядку
     запуска (тип исключения не меняем — обратная совместимость). *)
  let crits : (string * exn) list ref = ref [] in
  let crit_mu = Mutex.create () in
  let threads = List.mapi (fun i spec ->
    Thread.create (fun () ->
      let st =
        try run_one spec
        with Critical_failure (lbl, e) ->
          Mutex.lock crit_mu;
          crits := (lbl, e) :: !crits;
          Mutex.unlock crit_mu;
          `Failed
      in
      (* Запись в общий массив из N потоков обязана быть под mutex'ом.
         На OCaml 4 с Thread конкурентная запись в разные ячейки
         может работать «как повезёт» из-за GIL, но это data race
         по модели памяти. На OCaml 5 с Domain — UB. *)
      Mutex.lock results_mu;
      results.(i) <- (spec.label, st);
      Mutex.unlock results_mu
    ) ()
  ) specs in
  List.iter Thread.join threads;
  (* crits накоплены в обратном порядке (prepend); развернём в порядок
     возникновения. Логируем каждый, чтобы ни один не потерялся, затем
     пробрасываем первый. *)
  let all_crits = List.rev !crits in
  (match all_crits with
   | [] -> ()
   | first :: rest ->
     (* остальные критические сбои иначе были бы потеряны — логируем *)
     List.iter (fun (lbl, e) ->
       Log.error ~fields:[("pipeline", lbl);
                          ("error", Printexc.to_string e)]
         "additional critical pipeline failure (concurrent)") rest;
     let (lbl, e) = first in
     raise (Critical_failure (lbl, e)));
  Array.to_list results

(* supervise: то же, но без возврата статусов (бросает при Crash_all). *)
let supervise (specs : pipeline_spec list) : unit =
  ignore (supervise_result specs)
