open Miniflink
(* Тест структурированного логирования. Библиотека порождает события
   со структурой; sink (куда писать) задаёт приложение. Проверяем
   JSON-формат, экранирование, фильтр по уровню, перенаправление sink. *)

let pass name = Printf.printf "  OK %s\n%!" name
let fail name = Printf.printf "  FAIL %s\n%!" name; exit 1
let check name c = if c then pass name else fail name

let contains hay needle =
  let nl = String.length needle and hl = String.length hay in
  let rec go i = i + nl <= hl && (String.sub hay i nl = needle || go (i+1)) in
  nl = 0 || go 0

(* ── JSON-формат события ───────────────────────────────────── *)
let test_json_format () =
  Printf.printf "\n-- to_json produces valid structured JSON\n";
  let e = { Log.level = Log.Info; message = "hello";
            fields = [("worker", "3"); ("offset", "1900")]; ts_ms = 1700000000000 } in
  let j = Log.to_json e in
  check "has level" (contains j "\"level\":\"info\"");
  check "has msg" (contains j "\"msg\":\"hello\"");
  check "has ts" (contains j "\"ts\":1700000000000");
  check "has field worker" (contains j "\"worker\":\"3\"");
  check "has field offset" (contains j "\"offset\":\"1900\"");
  check "starts with {" (j.[0] = '{');
  check "ends with }" (j.[String.length j - 1] = '}')

(* ── Экранирование спецсимволов ────────────────────────────── *)
let test_escaping () =
  Printf.printf "\n-- JSON escaping of special chars\n";
  let e = { Log.level = Log.Error; message = "quote\" back\\ new\nline";
            fields = [("k", "tab\tval")]; ts_ms = 0 } in
  let j = Log.to_json e in
  check "escapes quote" (contains j "\\\"");
  check "escapes backslash" (contains j "\\\\");
  check "escapes newline" (contains j "\\n");
  check "escapes tab" (contains j "\\t");
  (* результат должен оставаться однострочным (нет сырых переводов строк) *)
  check "no raw newline in output" (not (String.contains j '\n'))

(* ── Кастомный sink: приложение перехватывает события ──────── *)
let test_custom_sink () =
  Printf.printf "\n-- set_sink redirects events to the application\n";
  let captured = ref [] in
  Log.set_sink (fun e -> captured := e :: !captured);
  Log.set_level Log.Debug;
  Log.info "first";
  Log.warn ~fields:[("x", "1")] "second";
  Log.error "third";
  let evs = List.rev !captured in
  check "captured 3 events" (List.length evs = 3);
  check "first is info" ((List.nth evs 0).Log.level = Log.Info);
  check "second has field" ((List.nth evs 1).Log.fields = [("x", "1")]);
  check "third is error" ((List.nth evs 2).Log.level = Log.Error)

(* ── Фильтр по уровню ──────────────────────────────────────── *)
let test_level_filter () =
  Printf.printf "\n-- set_level filters below threshold\n";
  let captured = ref [] in
  Log.set_sink (fun e -> captured := e :: !captured);
  Log.set_level Log.Warning;       (* debug/info отбрасываются *)
  Log.debug "drop me";
  Log.info "drop me too";
  Log.warn "keep";
  Log.error "keep";
  let evs = !captured in
  check "only warn+error kept (2)" (List.length evs = 2);
  check "no info/debug leaked"
    (List.for_all (fun e -> e.Log.level = Log.Warning || e.Log.level = Log.Error) evs)

(* ── sink приложения не должен ронять пайплайн ─────────────── *)
let test_sink_isolation () =
  Printf.printf "\n-- a throwing sink does not crash the caller\n";
  Log.set_level Log.Info;
  Log.set_sink (fun _ -> failwith "sink boom");
  (try Log.info "should not propagate"; pass "throwing sink swallowed"
   with _ -> fail "exception from sink propagated to caller")

let () =
  Printf.printf "==========================================\n";
  Printf.printf "  Structured logging\n";
  Printf.printf "==========================================\n";
  test_json_format ();
  test_escaping ();
  test_custom_sink ();
  test_level_filter ();
  test_sink_isolation ();
  Printf.printf "\nAll logging tests passed.\n"
