type readiness =
  | Starting
  | Ready
  | Draining
  | Unhealthy

type status = {
  ready        : readiness;
  state_size   : int;
  watermark_lag_ms : int;
  max_queue_depth  : int;
  detail       : string option;
}

let readiness_str = function
  | Starting -> "starting"
  | Ready -> "ready"
  | Draining -> "draining"
  | Unhealthy -> "unhealthy"

let check ?readiness ?state_size ?watermark_lag_ms ?max_queue_depth () =
  let opt f default = match f with Some g -> g () | None -> default in
  {
    ready      = opt readiness Ready;
    state_size = opt state_size 0;
    watermark_lag_ms = opt watermark_lag_ms 0;
    max_queue_depth  = opt max_queue_depth 0;
    detail = None;
  }

let to_json s =
  let base = Printf.sprintf
    "{\"ready\":\"%s\",\"state_size\":%d,\"watermark_lag_ms\":%d,\"max_queue_depth\":%d"
    (readiness_str s.ready) s.state_size s.watermark_lag_ms s.max_queue_depth in
  match s.detail with
  | Some d ->
    (* экранируем кавычки в detail *)
    let esc = Log.json_escape d in
    Printf.sprintf "%s,\"detail\":\"%s\"}" base esc
  | None -> base ^ "}"

let is_live s = s.ready <> Unhealthy
