(** TSan-нагрузка на параллельный exactly-once путь (аудит 2026-07,
    категория «data races систематически не проверены»).

    Прогон на машине с OCaml 5.x + tsan:
      opam switch create 5.2.0+tsan   # или add tsan вариант
      dune build test/tsan_parallel.exe \
        --profile tsan                 # см. dune-workspace ниже / README
      _build/default/test/tsan_parallel.exe

    Либо руками:
      OCAMLRUNPARAM=... TSAN_OPTIONS="halt_on_error=1" \
        _build/.../tsan_parallel.exe

    Идея: обычные функциональные тесты детерминированно НЕ вскрывают
    гонки — планировщик их прячет. TSan инструментирует каждый доступ к
    памяти и ловит race даже без «неправильного» исхода. Мы чинили класс
    C-1 (Atomic failed[]), M-1 (dlq count под mutex), M-3 (crash_epoch),
    H-2 (supervisor под mutex) — этот тест ГОНЯЕТ ровно те пути под
    максимальной contention, чтобы TSan подтвердил, что гонок не осталось
    (или нашёл оставшиеся).

    Условия для contention:
      - много воркеров (8) на несколько ядер;
      - частые чекпоинты (barrier alignment — общая точка синхронизации);
      - общий счётчик выходов под mutex (путь dlq/sink);
      - несколько прогонов подряд (crash/recovery не трогаем — отдельный
        тест; здесь чистый happy-path race-surface).

    Ожидание: 0 предупреждений TSan. Любое предупреждение = найденная
    гонка, вход и стек TSan напечатает. *)

open Miniflink

let count_process backend ev =
  match ev with
  | Mf_event.Data (key, _) ->
    let cur = match State_backend_memory.get backend key with
      | Some b -> int_of_string (Bytes.to_string b) | None -> 0 in
    State_backend_memory.set backend key
      (Bytes.of_string (string_of_int (cur + 1)));
    [key]
  | _ -> []

let mk_events n =
  List.init n (fun i -> Mf_event.data (Printf.sprintf "k%d" (i mod 16)) (i * 10))

(* один прогон под максимальной contention *)
let one_run ~workers ~events ~checkpoint_every =
  let store = Checkpoint_parallel.make_store () in
  let evs = mk_events events in
  let emitted = ref 0 in
  let mu = Mutex.create () in
  Checkpoint_parallel.run_exactly_once
    ~workers ~capacity:64 ~checkpoint_every
    ~key_of:(fun k -> k)
    ~make_state:State_backend_memory.create
    ~process:count_process
    ~source:(Checkpoint_parallel.seekable_of_list evs)
    ~sink:(Checkpoint_parallel.idempotent_sink
             (fun _ -> Mutex.lock mu; incr emitted; Mutex.unlock mu))
    ~store
    ();
  !emitted

let () =
  Printf.printf "=== tsan_parallel: contention stress ===\n%!";
  (* Несколько прогонов с разными параметрами — расширяем race-surface.
     Под TSan каждый прогон инструментируется; крэш/ворнинг остановит. *)
  let configs = [
    (* workers, events, checkpoint_every *)
    (8,  5000, 100);   (* частые барьеры *)
    (8,  5000, 37);    (* нечётный шаг — барьеры не совпадают с mod 16 *)
    (16, 8000, 250);   (* больше воркеров, чем ключей-групп *)
    (4,  3000, 500);   (* редкие барьеры, длинные фазы *)
    (2,  2000, 1);     (* барьер на каждом событии — экстремум *)
  ] in
  List.iteri (fun i (w, e, c) ->
    let out = one_run ~workers:w ~events:e ~checkpoint_every:c in
    (* корректность заодно: exactly-once → ровно e выходов *)
    if out <> e then begin
      Printf.printf "  FAIL config %d (w=%d e=%d c=%d): got %d outputs, expected %d\n%!"
        i w e c out e;
      exit 1
    end else
      Printf.printf "  OK config %d (w=%d e=%d c=%d): %d outputs\n%!" i w e c out
  ) configs;
  (* Честный вердикт: определяем, инструментирован ли бинарь TSan-ом.
     Sys.runtime_variant () на tsan-сборке содержит "tsan"; плюс при
     tsan-прогоне обычно задан TSAN_OPTIONS. Без инструментации этот
     прогон проверил только КОРРЕКТНОСТЬ (exactly-once), но НЕ гонки. *)
  let variant = Sys.runtime_variant () in
  let under_tsan =
    (let re_tsan s =
       let s = String.lowercase_ascii s in
       let n = String.length s and sub = "tsan" in
       let m = String.length sub in
       let rec go i = i + m <= n && (String.sub s i m = sub || go (i+1)) in
       go 0 in
     re_tsan variant)
    || (try Sys.getenv "TSAN_OPTIONS" <> "" with Not_found -> false)
  in
  if under_tsan then
    Printf.printf
      "\n[TSan active: runtime_variant=%S] Прогон завершён без \
       предупреждений TSan → гонок не обнаружено.\n%!" variant
  else begin
    Printf.printf
      "\n[TSan НЕ активен] Проверена только КОРРЕКТНОСТЬ (exactly-once: \
       ровно N выходов).\n\
       Гонки этим прогоном НЕ проверены. Для проверки гонок соберите под \
       ThreadSanitizer:\n\
      \  opam switch create 5.2.0+tsan   # если ещё нет\n\
      \  dune build --profile tsan test/tsan_parallel.exe\n\
      \  TSAN_OPTIONS=\"halt_on_error=1\" ./_build/tsan/test/tsan_parallel.exe\n%!"
  end
