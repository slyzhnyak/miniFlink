type policy = {
  max_attempts : int;
  base_ms      : int;
  factor       : float;
  max_ms       : int;
  jitter       : bool;
}

let default = {
  max_attempts = 5;
  base_ms      = 100;
  factor       = 2.0;
  max_ms       = 5000;
  jitter       = false;
}

let delay_for p attempt =
  if attempt <= 1 then 0
  else begin
    (* base * factor^(attempt-2) для 2-й попытки = base *)
    let raw = float_of_int p.base_ms *. (p.factor ** float_of_int (attempt - 2)) in
    let capped = int_of_float (Float.min raw (float_of_int p.max_ms)) in
    if p.jitter && capped > 0 then Random.int (capped + 1) else capped
  end

let with_retry p ~sleep ~on_give_up f x =
  let rec attempt n =
    match f x with
    | result -> Some result
    | exception e ->
      if n >= p.max_attempts then begin
        on_give_up e n;
        None
      end else begin
        (* пауза перед следующей попыткой (delay для попытки n+1) *)
        let d = delay_for p (n + 1) in
        if d > 0 then sleep d;
        attempt (n + 1)
      end
  in
  attempt 1
