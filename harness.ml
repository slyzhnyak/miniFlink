(* ============================================================
   Harness.ml — тестовый фреймворк

   Пример использования:
     let ctx = Harness.create pipeline in
     Harness.push_event ctx telemetry ~ts:1000;
     Harness.push_wm    ctx ~ts:35000;
     let alerts = Harness.collect ctx in
     Harness.assert_count ctx 2;
     assert (List.for_all (fun a -> a.rule = "speed") alerts)
   ============================================================ *)

type 'a ctx = {
  in_ch  : 'a Mf_event.t Channel.t;
  out    : 'a list ref;   (* полиморфный: 'a на самом деле 'b *)
  mutable closed : bool;
}

let create pipeline =
  let in_ch = Channel.make_unbounded () in
  let out   = ref [] in
  let src   = Channel.to_stream in_ch in
  (* Запускаем pipeline лениво — collect вычитает *)
  let stream = pipeline src in
  let ctx = { in_ch; out = Obj.magic out; closed = false } in
  (* pipeline stream доступен через closure *)
  ignore stream;
  ctx

let push_event ctx v ~ts =
  Channel.push ctx.in_ch (Mf_event.data v ts)

let push_wm ctx ~ts =
  Channel.push ctx.in_ch (Mf_event.wm ts)

let close ctx =
  if not ctx.closed then begin
    Channel.close ctx.in_ch;
    ctx.closed <- true
  end

let collect ctx =
  close ctx;
  List.rev !(Obj.magic ctx.out)

let assert_count ctx expected =
  let got = List.length (collect ctx) in
  if got <> expected then
    failwith (Printf.sprintf "Harness.assert_count: expected %d, got %d"
                expected got)

let assert_all ctx pred msg =
  let results = collect ctx in
  if not (List.for_all pred results) then
    failwith (Printf.sprintf "Harness.assert_all failed: %s" msg)

let run = collect
