(* ============================================================
   Ctx_store (v4) — простой [ref], функтор по типу ячейки.

   На OCaml 4 поток исполнения один (Thread кооперируются под GIL),
   поэтому глобальная ячейка-[ref] безопасна. Тип ячейки — параметр
   функтора [T.t], а не [Obj.t]: приведения не нужны (A-2). *)

module type CELL = sig type t end

module Make (T : CELL) = struct
  let cell : T.t option ref = ref None
  let get () = !cell
  let set v = cell := v
end
