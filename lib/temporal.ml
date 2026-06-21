(* Temporal join (versioned / as-of join).

   Обычный enrich из Table даёт «текущее» значение справочника. Temporal
   join даёт значение, актуальное НА EVENT-TIME события — и делает это
   корректно даже если апдейты справочника пришли с ОПОЗДАНИЕМ (поздно по
   порядку прихода, но с ранним valid_from).

   Как это возможно при опоздании: основной поток и поток апдейтов несут
   watermarks. Событие основного потока с временем T НЕ обогащается, пока
   watermark потока апдейтов не достигнет T — только тогда гарантировано,
   что ВСЕ апдейты с valid_from <= T уже прибыли. До этого момента событие
   буферизуется. Это превращает «гонку прихода» в детерминированный
   результат по event-time.

   Аналог temporal table join во Flink (FOR SYSTEM_TIME AS OF). *)

(* ── Versioned table: история значений по ключу ───────────── *)
(* Для каждого ключа храним версии (valid_from, value), отсортированные
   по valid_from УБЫВАНИЕМ (свежее впереди). as_of находит первую версию
   с valid_from <= запрошенного времени. *)
type ('k, 'v) versioned = ('k, (Time.t * 'v) list) Hashtbl.t

let create_versioned () : ('k, 'v) versioned = Hashtbl.create 64

(* Добавить версию: значение [v] для ключа [k], действующее с [valid_from].
   Вставка с сохранением сортировки по убыванию valid_from (опоздавший
   апдельт со старым valid_from встанет на своё место в истории).

   ВНИМАНИЕ про память: история версий per ключ растёт с каждым апдейтом
   и НЕ обрезается автоматически — as_of должен уметь ответить на запрос
   для любого прошлого ts. У long-running temporal join это
   неограниченный рост. Если main-поток имеет известный максимальный
   lateness, версии старше (min_main_wm - max_lateness) уже не нужны:
   используйте prune_versions_before для обрезки. *)
let put_version (tbl : ('k,'v) versioned) ~key ~valid_from v =
  let cur = match Hashtbl.find_opt tbl key with Some l -> l | None -> [] in
  let rec insert = function
    | [] -> [(valid_from, v)]
    | (t, _) :: _ as rest when valid_from >= t -> (valid_from, v) :: rest
    | x :: xs -> x :: insert xs in
  Hashtbl.replace tbl key (insert cur)

(* Обрезать историю: для каждого ключа оставить версии с valid_from
   >= [before], ПЛЮС одну непосредственно предшествующую (она ещё может
   быть актуальна для as_of на момент [before]). Версии старше неё уже
   недостижимы никаким as_of(ts) для ts >= before, поэтому удаляются.

   Вызывать когда известно что main-поток больше не запросит as_of для
   ts < before (например before = min_main_watermark - max_lateness).
   Ограничивает рост памяти versioned-таблицы. *)
let prune_versions_before (tbl : ('k,'v) versioned) ~before =
  Hashtbl.filter_map_inplace (fun _key versions ->
    (* versions отсортированы по убыванию valid_from. Идём сверху
       (свежее), оставляем всё с vf >= before; как только встретили
       vf < before — это первая «предшествующая», оставляем её и
       обрываем (всё после неё ещё старше и не нужно). *)
    let rec take = function
      | [] -> []
      | (vf, v) :: _ when vf < before -> [(vf, v)]  (* первая старая — keep + stop *)
      | x :: xs -> x :: take xs
    in
    Some (take versions)) tbl

(* as_of: значение ключа [k], актуальное на момент [ts] — первая версия
   с valid_from <= ts (история отсортирована по убыванию). None если на
   момент ts ключ ещё не имел значения. *)
let as_of (tbl : ('k,'v) versioned) k ts : 'v option =
  match Hashtbl.find_opt tbl k with
  | None -> None
  | Some versions ->
    let rec find = function
      | [] -> None
      | (vf, v) :: _ when vf <= ts -> Some v
      | _ :: rest -> find rest
    in find versions

(* ── Temporal join оператор ───────────────────────────────── *)

(* [temporal_join ~key_main ~key_upd ~valid_from ~merge ~updates main]

   main     — основной поток событий типа 'a
   updates  — поток апдейтов справочника типа 'u (с watermarks!)
   key_main — ключ join из события основного потока
   key_upd  — ключ join из апдейта
   valid_from — с какого event-time апдейт вступает в силу
   merge    — как обогатить событие 'a найденным значением 'u option

   Основной поток ТОЖЕ должен нести watermarks. Событие main с временем T
   удерживается в буфере, пока watermark апдейтов не достигнет T; затем
   обогащается as_of-значением и эмитится. Гарантирует корректность при
   опоздавших апдейтах. Буфер упорядочивается по event-time. *)
let temporal_join
    (type a) (type u) (type k)
    ~(key_main : a -> k)
    ~(key_upd  : u -> k)
    ~(valid_from : u -> Time.t)
    ~(merge : a -> u option -> a)
    ~(updates : u Mf_event.t Stream.t)
    (main : a Mf_event.t Stream.t)
    : a Mf_event.t Stream.t =
  let tbl : (k, u) versioned = create_versioned () in
  let wm_upd  = ref min_int in     (* watermark потока апдейтов *)
  let upd_done = ref false in
  (* буфер показаний, ждущих пока wm_upd догонит их ts. Список,
     упорядоченный по возрастанию ts (FIFO по времени). *)
  let buffer : (a * Time.t) Queue.t = Queue.create () in
  let out : a Mf_event.t Queue.t = Queue.create () in
  let main_done = ref false in

  (* продвинуть апдейты: вычитываем из updates пока не наткнёмся на
     ситуацию когда дальше читать не нужно. Здесь — осушаем доступное:
     апдейты применяем в tbl, watermark поднимаем. *)
  let pull_update () =
    if !upd_done then () else
    match updates () with
    | None -> upd_done := true; wm_upd := max_int  (* апдейтов больше нет → всё «прошлое» закрыто *)
    | Some (Mf_event.Watermark w) -> if w > !wm_upd then wm_upd := w
    | Some (Mf_event.Data (u, _)) ->
      put_version tbl ~key:(key_upd u) ~valid_from:(valid_from u) u
    | Some (Mf_event.Update { new_value = u; _ }) ->
      (* Update — заменяет old → new; для versioned tbl пишем
         new_value как новую версию. *)
      put_version tbl ~key:(key_upd u) ~valid_from:(valid_from u) u
    | Some (Mf_event.Retract _) -> ()
  in

  (* событие готово к эмиссии, если wm апдейтов догнал его ts (все
     апдейты с valid_from <= ts прибыли) *)
  let flush_ready () =
    let rec go () =
      if not (Queue.is_empty buffer) then begin
        let (ev, t) = Queue.peek buffer in
        if t <= !wm_upd then begin
          ignore (Queue.pop buffer);
          let v = as_of tbl (key_main ev) t in
          Queue.push (Mf_event.data (merge ev v) t) out;
          go ()
        end
      end
    in go ()
  in

  fun () ->
    let rec step () =
      if not (Queue.is_empty out) then Some (Queue.pop out) else
      (* подтягиваем апдейты чтобы поднять wm_upd, потом пробуем флашить *)
      if not !upd_done then (pull_update (); flush_ready ();
                             if not (Queue.is_empty out) then step () else step_main ())
      else (flush_ready (); step_main ())
    and step_main () =
      if not (Queue.is_empty out) then Some (Queue.pop out) else
      if !main_done then
        (* основной поток кончился: дочитываем апдейты пока буфер не
           разрешится; когда upd_done → wm=max_int, флашим всё *)
        (if Queue.is_empty buffer then None
         else if !upd_done then (flush_ready ();
           if Queue.is_empty out then None else Some (Queue.pop out))
         else (pull_update (); flush_ready (); step_main ()))
      else
      match main () with
      | None -> main_done := true; step_main ()
      | Some (Mf_event.Watermark w) -> Some (Mf_event.wm w)  (* пробрасываем wm основного *)
      | Some (Mf_event.Retract (v, t)) -> Some (Mf_event.retract v t)
      | Some (Mf_event.Update { old; new_value; ts }) ->
        (* Update — атомарная коррекция. Пробрасываем как есть;
           downstream enrich применит обогащение к both old и new. *)
        Some (Mf_event.update old new_value ts)
      | Some (Mf_event.Data (ev, t)) ->
        Queue.push (ev, t) buffer;
        flush_ready ();
        if not (Queue.is_empty out) then step () else step_main ()
    in step ()
