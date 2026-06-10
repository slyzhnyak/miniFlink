open Miniflink
(* Тесты health и config — оба отдают/принимают структуры (значения),
   а не поднимают сервер / парсят файлы. Проверяем что библиотека
   корректно вычисляет статус и валидирует конфиг. *)

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let contains hay needle =
  let nl = String.length needle and hl = String.length hay in
  let rec go i = i + nl <= hl && (String.sub hay i nl = needle || go (i+1)) in
  nl = 0 || go 0

(* ── Health: check собирает статус из замыканий приложения ── *)
let test_health_check () =
  Printf.printf "\n-- Health.check assembles status from app closures\n";
  let s = Health.check
    ~readiness:(fun () -> Health.Ready)
    ~state_size:(fun () -> 1234)
    ~watermark_lag_ms:(fun () -> 50)
    ~max_queue_depth:(fun () -> 7)
    () in
  check "readiness Ready" (s.Health.ready = Health.Ready);
  check "state_size captured" (s.Health.state_size = 1234);
  check "lag captured" (s.Health.watermark_lag_ms = 50);
  check "queue depth captured" (s.Health.max_queue_depth = 7)

let test_health_defaults () =
  Printf.printf "\n-- Health.check uses neutral defaults for unset sources\n";
  let s = Health.check () in
  check "default ready" (s.Health.ready = Health.Ready);
  check "default state_size 0" (s.Health.state_size = 0);
  check "default lag 0" (s.Health.watermark_lag_ms = 0)

let test_health_json_and_live () =
  Printf.printf "\n-- Health.to_json + is_live\n";
  let s = Health.check ~readiness:(fun () -> Health.Draining)
    ~state_size:(fun () -> 10) () in
  let j = Health.to_json s in
  check "json has ready field" (contains j "\"ready\":\"draining\"");
  check "json has state_size" (contains j "\"state_size\":10");
  check "draining is still live" (Health.is_live s);
  let unhealthy = Health.check ~readiness:(fun () -> Health.Unhealthy) () in
  check "unhealthy is not live" (not (Health.is_live unhealthy))

(* ── Config: запись с дефолтами + валидация ────────────────── *)
let test_config_default () =
  Printf.printf "\n-- Config.default is valid\n";
  check "default validates" (match Config.validate Config.default with Ok _ -> true | _ -> false);
  check "default workers > 0" (Config.default.Config.workers > 0)

let test_config_override () =
  Printf.printf "\n-- Config override via record update\n";
  let cfg = { Config.default with workers = 8; checkpoint_every = 5000 } in
  check "workers overridden" (cfg.Config.workers = 8);
  check "checkpoint overridden" (cfg.Config.checkpoint_every = 5000);
  check "untouched field keeps default" (cfg.Config.capacity = Config.default.Config.capacity);
  check "override still valid" (match Config.validate cfg with Ok _ -> true | _ -> false)

let test_config_validation () =
  Printf.printf "\n-- Config.validate rejects bad values\n";
  let bad_workers = { Config.default with workers = 0 } in
  check "workers=0 → Error" (match Config.validate bad_workers with Error _ -> true | _ -> false);
  let bad_cap = { Config.default with capacity = -1 } in
  check "capacity<0 → Error" (match Config.validate bad_cap with Error _ -> true | _ -> false);
  let bad_dir = { Config.default with state_dir = "" } in
  check "empty state_dir → Error" (match Config.validate bad_dir with Error _ -> true | _ -> false);
  (* checkpoint_every = 0 разрешено (выключение чекпойнтов) *)
  let cp_off = { Config.default with checkpoint_every = 0 } in
  check "checkpoint_every=0 ok (disabled)" (match Config.validate cp_off with Ok _ -> true | _ -> false)

let test_config_json () =
  Printf.printf "\n-- Config.to_json for startup logging\n";
  let j = Config.to_json Config.default in
  check "has workers" (contains j "\"workers\":4");
  check "has state_dir" (contains j "\"state_dir\"")

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Health + Config (structures, not servers)\n";
  Printf.printf "==========================================\n";
  test_health_check ();
  test_health_defaults ();
  test_health_json_and_live ();
  test_config_default ();
  test_config_override ();
  test_config_validation ();
  test_config_json ();
  Printf.printf "\nAll health+config tests passed.\n"

(* мост Config.t -> Runtime.config *)
let test_config_bridge () =
  Printf.printf "\n-- Runtime.of_config bridges app Config to runtime\n";
  let c = { Config.default with workers = 8; capacity = 2048 } in
  let rc = Runtime.of_config c in
  check "workers -> parallelism" (rc.Runtime.parallelism = 8);
  check "capacity carried" (rc.Runtime.capacity = 2048);
  check "default mode is Prod" (rc.Runtime.mode = Runtime.Prod)

let () = test_config_bridge ()
