(* ============================================================
   Metrics_log.ml — Prometheus-совместимые метрики

   Экспорт:
   1. stderr: периодический дамп (start_reporter)
   2. TCP /metrics endpoint для Prometheus scrape (start_server)

   Формат: Prometheus exposition text format
   # TYPE miniflink_events_in counter
   miniflink_events_in{mode="prod"} 1234567
   ============================================================ *)

type counter = {
  c_name   : string;
  c_labels : (string * string) list;
  mutable c_val : int;
  c_mu : Mutex.t;
}

type gauge = {
  g_name   : string;
  g_labels : (string * string) list;
  mutable g_val : float;
  g_mu : Mutex.t;
}

type histogram = {
  h_name    : string;
  h_labels  : (string * string) list;
  mutable h_count : int;
  mutable h_sum   : float;
  (* бакеты: 1µs 10µs 100µs 1ms 10ms 100ms 1s *)
  mutable h_buckets : int array;
  h_mu : Mutex.t;
}

let buckets_us = [| 1; 10; 100; 1_000; 10_000; 100_000; 1_000_000 |]

(* Глобальный реестр *)
let all_counters   : counter   list ref = ref []
let all_gauges     : gauge     list ref = ref []
let all_histograms : histogram list ref = ref []
let reg_mu = Mutex.create ()

let pp_labels = function
  | [] -> ""
  | ls -> "{" ^ String.concat ","
    (List.map (fun (k,v) -> Printf.sprintf "%s=\"%s\"" k v) ls) ^ "}"

let counter ~name ~labels =
  let c = { c_name=name; c_labels=labels; c_val=0; c_mu=Mutex.create () } in
  Mutex.lock reg_mu; all_counters := c :: !all_counters; Mutex.unlock reg_mu; c

let gauge ~name ~labels =
  let g = { g_name=name; g_labels=labels; g_val=0.; g_mu=Mutex.create () } in
  Mutex.lock reg_mu; all_gauges := g :: !all_gauges; Mutex.unlock reg_mu; g

let histogram ~name ~labels =
  let h = { h_name=name; h_labels=labels; h_count=0; h_sum=0.;
            h_buckets=Array.make (Array.length buckets_us + 1) 0;
            h_mu=Mutex.create () } in
  Mutex.lock reg_mu; all_histograms := h :: !all_histograms; Mutex.unlock reg_mu; h

let incr (c:counter) =
  Mutex.lock c.c_mu; c.c_val <- c.c_val + 1; Mutex.unlock c.c_mu

let add (c:counter) n =
  Mutex.lock c.c_mu; c.c_val <- c.c_val + n; Mutex.unlock c.c_mu

let set_gauge (g:gauge) v =
  Mutex.lock g.g_mu; g.g_val <- v; Mutex.unlock g.g_mu

let observe (h:histogram) v_us =
  Mutex.lock h.h_mu;
  h.h_count <- h.h_count + 1;
  h.h_sum   <- h.h_sum +. v_us;
  let vi = int_of_float v_us in
  let found = ref false in
  Array.iteri (fun i b ->
    if not !found && vi <= b then begin
      for j = i to Array.length h.h_buckets - 1 do
        h.h_buckets.(j) <- h.h_buckets.(j) + 1
      done;
      found := true
    end
  ) buckets_us;
  if not !found then
    h.h_buckets.(Array.length h.h_buckets - 1) <-
      h.h_buckets.(Array.length h.h_buckets - 1) + 1;
  Mutex.unlock h.h_mu

let dump () =
  let buf = Buffer.create 1024 in
  List.iter (fun (c:counter) ->
    Mutex.lock c.c_mu;
    Buffer.add_string buf (Printf.sprintf
      "# TYPE %s counter\n%s%s %d\n"
      c.c_name c.c_name (pp_labels c.c_labels) c.c_val);
    Mutex.unlock c.c_mu
  ) (List.rev !all_counters);
  List.iter (fun (g:gauge) ->
    Mutex.lock g.g_mu;
    Buffer.add_string buf (Printf.sprintf
      "# TYPE %s gauge\n%s%s %.3f\n"
      g.g_name g.g_name (pp_labels g.g_labels) g.g_val);
    Mutex.unlock g.g_mu
  ) (List.rev !all_gauges);
  List.iter (fun h ->
    Mutex.lock h.h_mu;
    let ls = pp_labels h.h_labels in
    let extra = if ls = "" then "" else "," ^ String.sub ls 1 (String.length ls - 1) in
    Buffer.add_string buf (Printf.sprintf "# TYPE %s histogram\n" h.h_name);
    Array.iteri (fun i _ ->
      let le = if i < Array.length buckets_us
               then string_of_int buckets_us.(i) else "+Inf" in
      Buffer.add_string buf (Printf.sprintf
        "%s_bucket{le=\"%s\"%s} %d\n"
        h.h_name le extra h.h_buckets.(i))
    ) h.h_buckets;
    Buffer.add_string buf (Printf.sprintf
      "%s_sum%s %.3f\n%s_count%s %d\n"
      h.h_name ls h.h_sum h.h_name ls h.h_count);
    Mutex.unlock h.h_mu
  ) (List.rev !all_histograms);
  Buffer.contents buf

(* ── stderr reporter ─────────────────────────────────────── *)

let start_reporter ~interval_s =
  let _t = Thread.create (fun () ->
    while true do
      Unix.sleep interval_s;
      let d = dump () in
      if d <> "" then
        Printf.eprintf "--- metrics [%s] ---\n%s\n%!"
          (let t = Unix.gettimeofday () in
           let tm = Unix.gmtime t in
           Printf.sprintf "%02d:%02d:%02d" tm.Unix.tm_hour
             tm.Unix.tm_min tm.Unix.tm_sec)
          d
    done
  ) () in
  ()

(* ── Prometheus HTTP endpoint ────────────────────────────── *)
(* Простой TCP server: GET /metrics → Prometheus exposition format *)

let start_server ?(port=9090) () =
  let _t = Thread.create (fun () ->
    let sock = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
    Unix.setsockopt sock Unix.SO_REUSEADDR true;
    Unix.bind sock (Unix.ADDR_INET (Unix.inet_addr_any, port));
    Unix.listen sock 5;
    Printf.eprintf "[metrics] Prometheus endpoint: http://0.0.0.0:%d/metrics\n%!" port;
    while true do
      (try
        let (client, _) = Unix.accept sock in
        let ic = Unix.in_channel_of_descr client in
        let _req = input_line ic in   (* читаем первую строку запроса *)
        let body = dump () in
        let response = Printf.sprintf
          "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: %d\r\n\r\n%s"
          (String.length body) body in
        let oc = Unix.out_channel_of_descr client in
        output_string oc response;
        flush oc;
        Unix.close client
      with _ -> ())
    done
  ) () in
  ()
