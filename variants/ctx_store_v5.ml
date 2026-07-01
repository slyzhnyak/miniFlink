(* ============================================================
   Ctx_store (v5) — domain-local storage, функтор по типу ячейки.

   На OCaml 5 домены исполняются параллельно. [Domain.DLS] даёт каждому
   домену собственную ячейку — изоляция без блокировок. Ячейка
   инициализируется [None] (контекст не установлен → дефолт) и НЕ
   наследуется при [Domain.spawn] (см. ctx_store.mli).

   Тип ячейки — параметр функтора [T.t], а не [Obj.t]: приведения
   типов не нужны (A-2). [Make] создаёт свой DLS-ключ при аппликации;
   Runtime_context инстанцирует функтор один раз. *)

module type CELL = sig type t end

module Make (T : CELL) = struct
  let key : T.t option Domain.DLS.key =
    Domain.DLS.new_key (fun () -> None)
  let get () = Domain.DLS.get key
  let set v = Domain.DLS.set key v
end
