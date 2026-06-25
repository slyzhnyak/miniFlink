(** Регрессия: with_context корректно сохраняет/восстанавливает
    ambient-контекст, в т.ч. при вложенности и исключениях.

    Эти инварианты не зависят от версии OCaml — проверяют публичный
    API Runtime_context, реализованный поверх Ctx_store (ref на v4,
    Domain.DLS на v5). На v5 дополнительно гарантируется изоляция
    между доменами (это тестируется отдельно при наличии OCaml 5). *)

open Miniflink

let pass n = Printf.printf "  OK %s\n%!" n
let fail n = Printf.printf "  FAIL %s\n%!" n; exit 1
let check n c = if c then pass n else fail n

let is_durable () =
  match (Runtime_context.get ()).mode with
  | Runtime_context.Durable _ -> true
  | Runtime_context.Ephemeral -> false

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Runtime_context.with_context scope\n";
  Printf.printf "==========================================\n\n";

  (* по умолчанию — ephemeral *)
  check "default is ephemeral" (not (is_durable ()));

  let backend = Persistence_backend.of_memory (Hashtbl.create 16) in
  let durable = Runtime_context.durable backend in

  (* внутри with_context — durable; после — снова ephemeral *)
  Runtime_context.with_context durable (fun () ->
    check "inside with_context: durable" (is_durable ()));
  check "after with_context: back to ephemeral" (not (is_durable ()));

  (* вложенность: ephemeral внутри durable, восстановление обоих уровней *)
  Runtime_context.with_context durable (fun () ->
    check "outer durable" (is_durable ());
    Runtime_context.with_context Runtime_context.ephemeral (fun () ->
      check "inner ephemeral overrides" (not (is_durable ())));
    check "restored to durable after inner" (is_durable ()));
  check "restored to ephemeral after outer" (not (is_durable ()));

  (* восстановление при исключении *)
  (try
     Runtime_context.with_context durable (fun () ->
       check "durable before raise" (is_durable ());
       raise Exit)
   with Exit -> ());
  check "context restored after exception" (not (is_durable ()));

  Printf.printf "\nRuntime_context scope regression passed.\n"
