type t = {
  workers          : int;
  capacity         : int;
  checkpoint_every : int;
  watermark_latency_ms : int;
  watermark_interval_ms : int;
  state_dir        : string;
  metrics_interval_s : int;
}

let default = {
  workers          = 4;
  capacity         = 256;
  checkpoint_every = 1000;
  watermark_latency_ms = 3000;
  watermark_interval_ms = 1000;
  state_dir        = "./mf_state";
  metrics_interval_s = 15;
}

let validate c =
  if c.workers <= 0 then Error "workers должно быть > 0"
  else if c.capacity <= 0 then Error "capacity должно быть > 0"
  else if c.checkpoint_every < 0 then Error "checkpoint_every должно быть >= 0"
  else if c.watermark_latency_ms < 0 then Error "watermark_latency_ms должно быть >= 0"
  else if c.watermark_interval_ms < 0 then Error "watermark_interval_ms должно быть >= 0"
  else if c.metrics_interval_s < 0 then Error "metrics_interval_s должно быть >= 0"
  else if c.state_dir = "" then Error "state_dir не должен быть пустым"
  else Ok c

let to_json c =
  Printf.sprintf
    "{\"workers\":%d,\"capacity\":%d,\"checkpoint_every\":%d,\"watermark_latency_ms\":%d,\"watermark_interval_ms\":%d,\"state_dir\":\"%s\",\"metrics_interval_s\":%d}"
    c.workers c.capacity c.checkpoint_every c.watermark_latency_ms
    c.watermark_interval_ms c.state_dir c.metrics_interval_s
