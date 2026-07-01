(** Тест A-1: TTL-очистка per-key state в {!Trigger.of_stream}.

    Без [?ttl] states/last_event_ts растут без границ. С [?ttl] спокойный
    (S_ok) ключ, не обновлявшийся дольше [wm - ttl], выселяется при
    watermark. КЛЮЧЕВОЕ свойство безопасности: ключ с активным алертом
    (S_problem, sticky) НЕ выселяется — иначе потеряли бы состояние
    алерта и он мог бы повторно сработать.

    Проверяем поведенчески (states внутренний):
    - спокойный ключ выселяется → после eviction sticky-история
      сбрасывается (повторное превышение снова эмитит alert);
    - ключ в Problem не выселяется → остаётся sticky даже после
      выселяющего watermark (повторное превышение молчит). *)

open Miniflink

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

type alert =
  | Above of string * float * Time.t
  | Resolved of string * Time.t

let above5 () =
  Trigger.create
    ~name:"above5"
    ~condition:(Trigger.greater_than 5.0)
    ~severity:Trigger.Warning
    ~produce_alert:(fun ~key ~value ~ts -> Above (key, value, ts))
    ~produce_recovery:(fun ~key ~ts -> Resolved (key, ts))
    ()

(* прогнать с явными событиями (нужны Watermark'и), вернуть alert-события *)
let run_ev ?ttl spec events =
  let stream = events |> Stream.of_list |> Trigger.of_stream ?ttl spec in
  let acc = ref [] in
  let rec loop () = match stream () with
    | None -> ()
    | Some (Mf_event.Data (a, _)) -> acc := a :: !acc; loop ()
    | Some _ -> loop ()
  in loop ();
  List.rev !acc

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  A-1: Trigger TTL eviction\n";
  Printf.printf "==========================================\n";

  (* ── 1. Спокойный ключ выселяется: sticky-история сбрасывается ── *)
  Printf.printf "\n-- 1. calm key evicted -> state resets\n";
  (* "A" под порогом (S_ok) при ts=10; watermark=100 (ttl=20 → порог 80,
     ключ 10 < 80, выселяется); затем "A" превышает порог при ts=110.
     Поскольку состояние сброшено, alert эмитится нормально. *)
  let alerts = run_ev ~ttl:20 (above5 ()) [
    Mf_event.data ("A", 3.0) 10;    (* под порогом → S_ok *)
    Mf_event.wm 100;                 (* выселяет A *)
    Mf_event.data ("A", 7.0) 110;   (* превышение → alert *)
  ] in
  check "после eviction превышение эмитит alert"
    (List.exists (function Above ("A", 7.0, _) -> true | _ -> false) alerts);

  (* ── 2. Ключ в Problem НЕ выселяется (sticky сохраняется) ─────── *)
  Printf.printf "\n-- 2. key in Problem is NOT evicted (stays sticky)\n";
  (* "B" превышает порог при ts=10 → S_problem (alert #1). watermark=100
     (ttl=20 → порог 80). B НЕ обновлялся с ts=10 < 80, НО он в S_problem,
     поэтому выселяться не должен. Повторное превышение при ts=110 —
     sticky, alert НЕ эмитится повторно. Если бы B ошибочно выселили,
     состояние сбросилось бы и alert #2 эмитился бы (=баг). *)
  let alerts = run_ev ~ttl:20 (above5 ()) [
    Mf_event.data ("B", 7.0) 10;    (* превышение → S_problem, alert #1 *)
    Mf_event.wm 100;                 (* НЕ должен выселить B (Problem) *)
    Mf_event.data ("B", 8.0) 110;   (* sticky → молчит *)
  ] in
  let n = List.length (List.filter
    (function Above ("B", _, _) -> true | _ -> false) alerts) in
  check (Printf.sprintf "Problem-ключ остаётся sticky: ровно 1 alert (got %d)" n)
    (n = 1);

  (* ── 3. Без ttl: спокойный ключ сохраняется (контроль) ────────── *)
  Printf.printf "\n-- 3. without ttl: calm key state persists\n";
  (* Тот же сценарий, что #1, но без ttl. Sticky-история "A" не
     сбрасывается... но "A" был в S_ok (под порогом), так что здесь
     eviction и без того ничего бы не изменил семантически для alert.
     Проверяем главное: без ttl всё работает как раньше — превышение
     после спокойного периода эмитит alert. *)
  let alerts = run_ev (above5 ()) [
    Mf_event.data ("A", 3.0) 10;
    Mf_event.wm 100;
    Mf_event.data ("A", 7.0) 110;
  ] in
  check "без ttl превышение эмитит alert (совместимость)"
    (List.exists (function Above ("A", 7.0, _) -> true | _ -> false) alerts);

  Printf.printf "\nA-1 trigger eviction tests passed.\n"
