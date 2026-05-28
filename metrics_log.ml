(* ============================================================
   Metrics_log.ml — периодический вывод в stderr

   Prometheus exposition format:
   # HELP miniflink_events_total Total events processed
   # TYPE miniflink_events_total counter
   miniflink_events_total{operator="window"} 1234567
   ============================================================ *)

type counter = {
  c_name   : string;
  c_labels : (string * string) list;
  mutable c_val : int;
  mu : Mutex.t;
}

type gauge = {
  g_name   : string;
  g_labels : (string * string) list;
  mutable g_val : float;
  mu : Mutex.t;
}

type histogram = {
  h_name   : string;
  h_labels : (string * string) list;
  mutable h_count : int;
  mutable h_sum   : float;
  (* простые бакеты: 1µs 10µs 100µs 1ms 10ms 100ms 1s *)
  mutable h_buckets : int array;
  mu : Mutex.t;
}

let buckets_us = [| 1; 10; 100; 1_000; 10_000; 100_000; 1_000_000 |]

(* Глобальный реестр *)
let all_counters   : counter   list ref = ref []
let all_gauges     : gauge     list ref = ref []
let all_histograms : histogram list ref = ref []
let reg_mu = Mutex.create ()

let pp_labels = function
  | []  -> ""
  | ls  -> "{" ^ String.concat "," (List.map (fun (k,v) ->
              Printf.sprintf "%s=\"%s\"" k v) ls) ^ "}"

let counter ~name ~labels =
  let c = { c_name=name; c_labels=labels; c_val=0; mu=Mutex.create () } in
  Mutex.lock reg_mu; all_counters := c :: !all_counters; Mutex.unlock reg_mu; c

let gauge ~name ~labels =
  let g = { g_name=name; g_labels=labels; g_val=0.; mu=Mutex.create () } in
  Mutex.lock reg_mu; all_gauges := g :: !all_gauges; Mutex.unlock reg_mu; g

let histogram ~name ~labels =
  let h = { h_name=name; h_labels=labels; h_count=0; h_sum=0.;
            h_buckets=Array.make (Array.length buckets_us + 1) 0;
            mu=Mutex.create () } in
  Mutex.lock reg_mu; all_histograms := h :: !all_histograms; Mutex.unlock reg_mu; h

let incr (c:counter) =
  Mutex.lock c.mu; c.c_val <- c.c_val + 1; Mutex.unlock c.mu

let add (c:counter) n =
  Mutex.lock c.mu; c.c_val <- c.c_val + n; Mutex.unlock c.mu

let set_gauge (g:gauge) v =
  Mutex.lock g.mu; g.g_val <- v; Mutex.unlock g.mu

let observe h v_us =
  Mutex.lock h.mu;
  h.h_count <- h.h_count + 1;
  h.h_sum   <- h.h_sum +. v_us;
  let vi = int_of_float v_us in
  let idx = ref (Array.length buckets_us) in
  Array.iteri (fun i b -> if vi <= b && !idx = Array.length buckets_us then idx := i) buckets_us;
  for i = !idx to Array.length h.h_buckets - 1 do
    h.h_buckets.(i) <- h.h_buckets.(i) + 1
  done;
  Mutex.unlock h.mu

let dump () =
  let buf = Buffer.create 512 in
  List.iter (fun (c:counter) ->
    Mutex.lock c.mu;
    Buffer.add_string buf
      (Printf.sprintf "# TYPE %s counter\n%s%s %d\n"
        c.c_name c.c_name (pp_labels c.c_labels) c.c_val);
    Mutex.unlock c.mu
  ) !all_counters;
  List.iter (fun (g:gauge) ->
    Mutex.lock g.mu;
    Buffer.add_string buf
      (Printf.sprintf "# TYPE %s gauge\n%s%s %.3f\n"
        g.g_name g.g_name (pp_labels g.g_labels) g.g_val);
    Mutex.unlock g.mu
  ) !all_gauges;
  List.iter (fun h ->
    Mutex.lock h.mu;
    let ls = pp_labels h.h_labels in
    Buffer.add_string buf (Printf.sprintf "# TYPE %s histogram\n" h.h_name);
    Array.iteri (fun i b ->
      let le = if i < Array.length buckets_us
               then string_of_int buckets_us.(i)
               else "+Inf" in
      Buffer.add_string buf
        (Printf.sprintf "%s_bucket{le=\"%s\"%s} %d\n"
           h.h_name le
           (if ls = "" then "" else "," ^ String.sub ls 1 (String.length ls - 1))
           h.h_buckets.(i))
    ) h.h_buckets;
    Buffer.add_string buf
      (Printf.sprintf "%s_sum%s %.3f\n%s_count%s %d\n"
        h.h_name ls h.h_sum h.h_name ls h.h_count);
    Mutex.unlock h.mu
  ) !all_histograms;
  Buffer.contents buf

let start_reporter ~interval_s =
  let _t = Thread.create (fun () ->
    while true do
      Unix.sleep interval_s;
      Printf.eprintf "--- metrics ---\n%s\n%!" (dump ())
    done
  ) () in
  ()
