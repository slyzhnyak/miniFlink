(* ============================================================
   Window.ml — оконные операторы (вынесено из pipe.ml).

   Содержит: спецификации окон (win_spec), назначение событий окнам
   (assign), и сами операторы — временные окна (window), инкрементальная
   агрегация (window_fold), count-окна, global-окно с триггерами,
   session-окна со слиянием.

   Pipe переэкспортирует это как Pipe.window / Pipe.tumbling / ... —
   публичный API не меняется.
   ============================================================ *)

(* ── Window ───────────────────────────────────────────────── *)

(* ── win_key + Hashtbl-based state ─────────────────────────
   Раньше состояние окон жило в Map.Make (immutable), который на
   каждый add создавал новое дерево с аллокациями. Sliding-окно 60s/5s
   делает 12 add'ов на КАЖДОЕ событие — это была главная цена window.ml
   (64% времени всего пайплайна median_rssi по bench_ops, перепроверено
   A/B-бенчмарком: 2.3x speedup при переходе на Hashtbl).

   Hashtbl мутабельный → нужна осторожность с iter: нельзя мутировать
   таблицу во время обхода. Паттерн: сначала собрать ключи в список,
   потом изменить таблицу. *)
type win_key = string * int * int   (* (key, start, stop) *)

type win_spec =
  | Tumbling of Time.t
  | Sliding  of Time.t * Time.t   (* size, step *)

(* Стабильный строковый тег спецификации окна — для namespace
   managed-state, чтобы разные window_fold не коллидировали в
   backend. *)
let win_spec_tag = function
  | Tumbling sz -> Printf.sprintf "tumbling:%d" sz
  | Sliding (sz, step) -> Printf.sprintf "sliding:%d:%d" sz step

(** Неперекрывающиеся окна фиксированного размера [size] (> 0).
    @raise Invalid_argument если [size <= 0]. *)
let tumbling size =
  if size <= 0 then
    invalid_arg "Pipe.tumbling: размер окна должен быть > 0";
  Tumbling size

(** Перекрывающиеся окна: [sliding size step] — окна размера [size]
    с шагом [step] (при [step < size] окна перекрываются).
    @raise Invalid_argument если [size <= 0] или [step <= 0]. *)
let sliding size step =
  if size <= 0 then
    invalid_arg "Pipe.sliding: размер окна должен быть > 0";
  if step <= 0 then
    invalid_arg "Pipe.sliding: шаг окна должен быть > 0";
  Sliding (size, step)

(* floor-деление: для отрицательных ts обычное (/) в OCaml округляет
   к нулю, и событие на ts=-5 попало бы в [0,size) вместо [-size,0).
   floor_div даёт корректное окно для любого знака event-time. *)
let floor_div a b = if a >= 0 then a / b else (a - b + 1) / b

let assign spec ts =
  match spec with
  | Tumbling size ->
    let s = (floor_div ts size) * size in [(s, s + size)]
  | Sliding (size, step) ->
    (* Наибольший старт-кратный step, не превышающий ts; идём вниз пока
       окно ещё накрывает ts. Для ts >= 0 окна выравниваются от 0 (старт
       не уходит в отрицательные — соглашение, как в Flink от epoch).
       Для ts < 0 floor_div и отрицательные старты корректны. *)
    let last = (floor_div ts step) * step in
    let min_start = if ts >= 0 then 0 else min_int in
    let rec go s acc =
      if s + size <= ts || s < min_start then acc
      else go (s - step) ((s, s + size) :: acc)
    in go last []

(* window groups by KEYED.key, fires when watermark closes the window.
   Output: (key * value list) per window.

   Late data handling (retractions):
   - Окно после закрытия по watermark переходит в Fired, но данные
     сохраняются ещё на `allowed_lateness` времени.
   - Late Data попадающее в Fired окно: переоткрывает его, эмитит
     Retract(старый результат) затем Data(новый результат).
   - Окно окончательно удаляется когда wm > stop + latency + allowed_lateness. *)

type 'a win_state =
  | Open  of 'a list
  | Fired of 'a list   (* данные сохранены для late data *)

(** [window (module K) ?latency ?allowed_lateness spec upstream]
    группирует события по ключу [K.key] и временным окнам [spec]
    ({!tumbling} или {!sliding}). Окно закрывается когда watermark
    проходит его правую границу (плюс [latency]), выдавая
    [(ключ, событие list)].

    Поздние данные в пределах [~allowed_lateness] переоткрывают
    закрытое окно: эмитится [Retract] старого результата, затем
    [Data] нового. За пределами [allowed_lateness] окно окончательно
    удаляется (ограничивает рост состояния). *)

(* Убрать первое вхождение [x] из списка (для retract/update в
   materializing-окнах, которые хранят полный список событий). *)
let remove_first x lst =
  let rec go = function
    | [] -> []
    | h :: tl when h = x -> tl
    | h :: tl -> h :: go tl
  in go lst

let window
    (type a)
    (module K : Keyed.S with type t = a)
    ?(latency = 0)
    ?(allowed_lateness = 0)
    ?(on_late = fun _ -> ())
    (spec     : win_spec)
    (upstream : a Mf_event.t Stream.t)
    : (string * a list) Mf_event.t Stream.t =
  let tbl : (win_key, a win_state) Hashtbl.t = Hashtbl.create 4096 in
  let cur_wm = ref min_int in   (* последний виденный watermark *)
  (* самодиагностика сборки пайпа *)
  let saw_data = ref false and saw_wm = ref false in
  let n_late = ref 0 and n_emitted = ref 0 in
  let on_late x = incr n_late; on_late x in
  let out : (string * a list) Mf_event.t Queue.t = Queue.create () in
  let emit_data k stop vs =
    incr n_emitted;
    Queue.push (Mf_event.data (k, List.rev vs) stop) out in
  let emit_update k stop old_vs new_vs =
    (* Подавляем noop: если список окна не изменился (например retract
       значения которого там не было — remove_first вернул тот же
       список), Update{xs→xs} downstream был бы шумом. *)
    if old_vs <> new_vs then
      Queue.push (Mf_event.update
                    (k, List.rev old_vs) (k, List.rev new_vs) stop) out in
  fun () ->
    let rec pull () =
      if not (Queue.is_empty out) then Some (Queue.pop out) else
      match upstream () with
      | None ->
        if !saw_data && not !saw_wm then
          Log.warn ~fields:[("windows_emitted_incrementally", "0")]
            "window: события были, но ни одного watermark — окна эмитированы \
             только при завершении потока (на бесконечном потоке — никогда), \
             late-семантика не работала. Добавьте Pipe.event_time перед окном";
        if !saw_data && !saw_wm && !n_emitted = 0 && !n_late > 0 then
          Log.warn ~fields:[("late", string_of_int !n_late)]
            "window: ВСЕ события ушли в late (0 окон эмитировано) — \
             watermark обгоняет данные; увеличьте lateness у Pipe.event_time \
             или allowed_lateness у окна";
        (* Закрываем все Open окна; Fired уже эмитили *)
        Hashtbl.iter (fun (k,_,stop) st ->
          match st with Open vs when vs <> [] -> emit_data k stop vs | _ -> ()
        ) tbl;
        Hashtbl.clear tbl;
        if Queue.is_empty out then None else Some (Queue.pop out)
      | Some (Mf_event.Watermark wm) ->
        saw_wm := true;
        cur_wm := wm;
        (* Open окна со stop+latency <= wm → Fire. Hashtbl нельзя
           мутировать в iter — собираем ключи отдельно. *)
        let to_fire = ref [] in
        Hashtbl.iter (fun key st ->
          let (_,_,stop) = key in
          match st with
          | Open vs when stop + latency <= wm -> to_fire := (key, vs) :: !to_fire
          | _ -> ()
        ) tbl;
        List.iter (fun ((k,_,stop) as key, vs) ->
          if vs <> [] then emit_data k stop vs;
          Hashtbl.replace tbl key (Fired vs)
        ) !to_fire;
        (* Fired окна старше allowed_lateness → удаляем окончательно *)
        let to_remove = ref [] in
        Hashtbl.iter (fun key st ->
          let (_,_,stop) = key in
          match st with
          | Fired _ when stop + latency + allowed_lateness <= wm ->
            to_remove := key :: !to_remove
          | _ -> ()
        ) tbl;
        List.iter (Hashtbl.remove tbl) !to_remove;
        Queue.push (Mf_event.wm wm) out;
        pull ()
      | Some (Mf_event.Retract (v, t)) ->
        (* Materializing window хранит список — убираем первое
           вхождение v из затронутых окон. *)
        List.iter (fun (s, stop) ->
          let mk = (K.key v, s, stop) in
          match Hashtbl.find_opt tbl mk with
          | None -> ()
          | Some (Open vs) ->
            Hashtbl.replace tbl mk (Open (remove_first v vs))
          | Some (Fired vs) ->
            let vs' = remove_first v vs in
            emit_update (K.key v) stop vs vs';
            Hashtbl.replace tbl mk (Fired vs')
        ) (assign spec t);
        pull ()
      | Some (Mf_event.Update { old; new_value; ts = t }) ->
        (* Атомарная коррекция: убрать old, добавить new. *)
        saw_data := true;
        List.iter (fun (s, stop) ->
          let mk = (K.key new_value, s, stop) in
          match Hashtbl.find_opt tbl mk with
          | None ->
            if stop + latency + allowed_lateness <= !cur_wm then
              on_late new_value
            else
              Hashtbl.add tbl mk (Open [new_value])
          | Some (Open vs) ->
            Hashtbl.replace tbl mk (Open (new_value :: remove_first old vs))
          | Some (Fired vs) ->
            let vs' = new_value :: remove_first old vs in
            emit_update (K.key new_value) stop vs vs';
            Hashtbl.replace tbl mk (Fired vs')
        ) (assign spec t);
        pull ()
      | Some (Mf_event.Data (v,t)) ->
        saw_data := true;
        List.iter (fun (s, stop) ->
          let mk = (K.key v, s, stop) in
          match Hashtbl.find_opt tbl mk with
          | None ->
            (* окна нет. Если его граница уже прошла порог окончательного
               удаления (stop+latency+allowed_lateness <= wm), значит
               событие опоздало СВЕРХ allowed_lateness — в side output,
               не создаём «призрачное» окно которое закроется в одиночку *)
            if stop + latency + allowed_lateness <= !cur_wm then
              on_late v
            else
              Hashtbl.add tbl mk (Open [v])
          | Some (Open vs) ->
            Hashtbl.replace tbl mk (Open (v :: vs))
          | Some (Fired vs) ->
            (* Phase 3 atomic: late data в пределах allowed_lateness —
               эмитим ОДИН Update event вместо пары Retract+Data,
               чтобы downstream snapshot-операторы (keyed_join) не
               видели промежуточный None flicker. *)
            let vs' = v :: vs in
            emit_update (K.key v) stop vs vs';
            Hashtbl.replace tbl mk (Fired vs')
        ) (assign spec t);
        pull ()
    in pull ()

(* ── Incremental window aggregation ───────────────────────── *)

(* Инкрементальная агрегация: вместо хранения всех событий окна списком
   и свёртки post-factum, окно держит АККУМУЛЯТОР и обновляет его на
   каждом событии через [~add]. Это убирает материализацию списка
   (меньше памяти, нет O(n) reverse, лучше locality) — главный perf-win
   для окон с большим числом событий.

   Состояние окна — (acc, есть ли хоть одно событие). Пустые окна не
   эмитятся. Late data в закрытое окно сворачивается в сохранённый acc
   и пере-эмитится (retract старого результата + новый), как и в [window]. *)
type 'acc fold_state =
  | FOpen  of 'acc * bool        (* аккумулятор, был ли хоть один add *)
  | FFired of 'acc * bool

(** [window_fold (module K) ?latency ?allowed_lateness spec ~init ~add
    upstream] — окно с {e инкрементальной} агрегацией. Вместо накопления
    списка событий сворачивает каждое событие в аккумулятор [~add acc v]
    сразу при поступлении, начиная с [~init ()]. Эмитит [(key, acc)] при
    закрытии окна.

    Отличие от [window |> aggregate]: тот хранит весь список окна и
    сворачивает в конце (O(n) память на окно); этот хранит только
    аккумулятор (O(1) на окно по числу событий). Используйте когда
    агрегат инкрементален (сумма, счёт, max, min, среднее как (сумма,
    счёт)) — это даёт меньше памяти и GC-давления на больших окнах.

    [~init] — функция (а не значение), чтобы у каждого окна был свой
    свежий аккумулятор (важно для mutable-аккумуляторов). *)
let window_fold
    (type a) (type acc)
    (module K : Keyed.S with type t = a)
    ?(latency = 0)
    ?(allowed_lateness = 0)
    ?(remove : (acc -> a -> acc) option)
    (spec : win_spec)
    ~(init : unit -> acc)
    ~(add  : acc -> a -> acc)
    (upstream : a Mf_event.t Stream.t)
    : (string * acc) Mf_event.t Stream.t =

  (* Состояние окон — managed-state с прозрачной persistence.
     Persistence решается ambient Runtime_context, НЕ параметром
     оператора: тот же код работает в ephemeral и durable. Ключ окна
     (user_key, start, stop) сериализуем в строку для backend.

     namespace включает spec, чтобы разные window_fold в одном
     пайплайне не коллидировали по ключам в backend. *)
  let win_key_str (uk, start, stop) = Printf.sprintf "%s:%d:%d" uk start stop in
  let win_key_unstr s =
    (* обратный разбор: два int с конца — start, stop; остальное — uk *)
    match List.rev (String.split_on_char ':' s) with
    | stop_s :: start_s :: rest_rev ->
      let uk = String.concat ":" (List.rev rest_rev) in
      (uk, int_of_string start_s, int_of_string stop_s)
    | _ -> invalid_arg ("window_fold: bad win_key in backend: " ^ s)
  in
  let state = Managed_state.create
      ~name:("window_fold:" ^ win_spec_tag spec)
      ~key_str:win_key_str ~key_unstr:win_key_unstr () in
  let out : (string * acc) Mf_event.t Queue.t = Queue.create () in
  let emit_data k stop acc = Queue.push (Mf_event.data (k, acc) stop) out in
  (* Phase 3: эмитим atomic Update вместо пары Retract+Data при
     late-correction. Downstream видит ОДНО событие, нет flicker'а
     через None в snapshot-based операторах (keyed_join). *)
  let emit_update k stop old_acc new_acc =
    Queue.push (Mf_event.update (k, old_acc) (k, new_acc) stop) out
  in


  fun () ->
    let rec pull () =
      if not (Queue.is_empty out) then Some (Queue.pop out) else
      match upstream () with
      | None ->
        (* На конце потока поведение зависит от наличия backend:
           - Без backend: end-of-stream fire — финальная очистка,
             выдаём что есть, чтобы данные не потерялись.
           - С backend: считаем что upstream может вернуться (recovery
             сценарий) и оставляем open окна в backend как есть, чтобы
             новый instance продолжил их при рестарте. *)
        if not (Managed_state.is_durable state) then begin
          Managed_state.iter state (fun (k,_,stop) st ->
            match st with FOpen (acc, true) -> emit_data k stop acc | _ -> ()
          );
          Managed_state.iter state (fun k _ -> Managed_state.remove state k)
        end;
        if Queue.is_empty out then None else Some (Queue.pop out)
      | Some (Mf_event.Watermark wm) ->
        (* Open окна со stop+latency <= wm → Fire (с двухфазным проходом) *)
        let to_fire = ref [] in
        Managed_state.iter state (fun key st ->
          let (_,_,stop) = key in
          match st with
          | FOpen (acc, nonempty) when stop + latency <= wm ->
            to_fire := (key, acc, nonempty) :: !to_fire
          | _ -> ()
        );
        List.iter (fun ((k,_,stop) as key, acc, nonempty) ->
          if nonempty then emit_data k stop acc;
          Managed_state.set state key (FFired (acc, nonempty))
        ) !to_fire;
        (* Удаление старых FFired *)
        let to_remove = ref [] in
        Managed_state.iter state (fun key st ->
          let (_,_,stop) = key in
          match st with
          | FFired _ when stop + latency + allowed_lateness <= wm ->
            to_remove := key :: !to_remove
          | _ -> ()
        );
        List.iter (Managed_state.evict state) !to_remove;
        (* Snapshot после всех изменений по watermark'у — noop если
           ephemeral, поэтому зовём безусловно. *)
        Managed_state.checkpoint state;
        Queue.push (Mf_event.wm wm) out;
        pull ()
      | Some (Mf_event.Retract (v, t)) ->
        (* Phase 2: если remove передан, применяем его к окнам куда
           попадает (K.key v, t). Без remove — drop (backwards compat). *)
        (match remove with
         | None -> pull ()
         | Some rem ->
           List.iter (fun (s, stop) ->
             let mk = (K.key v, s, stop) in
             match Managed_state.get state mk with
             | None -> ()  (* окна нет — retract на пустой стейт игнорируется *)
             | Some (FOpen (acc, nonempty)) ->
               Managed_state.set state mk (FOpen (rem acc v, nonempty))
             | Some (FFired (acc, _nonempty)) ->
               (* Phase 3 atomic: late retract применяет remove и
                  эмитит ОДИН Update event (был Retract+Data пара). *)
               let new_acc = rem acc v in
               emit_update (K.key v) stop acc new_acc;
               Managed_state.set state mk (FFired (new_acc, true))
           ) (assign spec t);
           pull ())
      | Some (Mf_event.Update { old; new_value; ts = t }) ->
        (* Native Update на input: атомарная коррекция old → new.
           Семантика: убрать old из аккумулятора (требует remove),
           добавить new. old и new назначаются в окна по своим
           ключам через assign(t).

           Если remove нет — агрегат не retractable, и мы НЕ можем
           корректно убрать old. В этом случае применяем только new
           как Data (best-effort, документированное ограничение). *)
        (match remove with
         | None ->
           (* Не retractable: применяем только new_value как Data.
              old остаётся "учтённым" — это известное ограничение
              для non-retractable агрегатов. *)
           List.iter (fun (s, stop) ->
             let mk = (K.key new_value, s, stop) in
             match Managed_state.get state mk with
             | None ->
               Managed_state.set state mk (FOpen (add (init ()) new_value, true))
             | Some (FOpen (acc, _)) ->
               Managed_state.set state mk (FOpen (add acc new_value, true))
             | Some (FFired (acc, _)) ->
               let acc' = add acc new_value in
               emit_update (K.key new_value) stop acc acc';
               Managed_state.set state mk (FFired (acc', true))
           ) (assign spec t);
           pull ()
         | Some rem ->
           (* Retractable: атомарно remove(old) + add(new) в каждом
              затронутом окне. Предполагаем что old и new попадают в
              одни и те же окна (одинаковый ts t) — это так для
              window correction, где меняется значение, а не время. *)
           List.iter (fun (s, stop) ->
             let mk = (K.key new_value, s, stop) in
             match Managed_state.get state mk with
             | None ->
               (* окна нет: old там не было, просто добавляем new *)
               Managed_state.set state mk (FOpen (add (init ()) new_value, true))
             | Some (FOpen (acc, nonempty)) ->
               let acc' = add (rem acc old) new_value in
               Managed_state.set state mk (FOpen (acc', nonempty))
             | Some (FFired (acc, _)) ->
               (* late update на закрытое окно: атомарно пересчитываем
                  и эмитим ОДИН Update event downstream *)
               let acc' = add (rem acc old) new_value in
               emit_update (K.key new_value) stop acc acc';
               Managed_state.set state mk (FFired (acc', true))
           ) (assign spec t);
           pull ())
      | Some (Mf_event.Data (v, t)) ->
        List.iter (fun (s, stop) ->
          let mk = (K.key v, s, stop) in
          match Managed_state.get state mk with
          | None ->
            Managed_state.set state mk (FOpen (add (init ()) v, true))
          | Some (FOpen (acc, _)) ->
            Managed_state.set state mk (FOpen (add acc v, true))
          | Some (FFired (acc, _)) ->
            (* Phase 3 atomic: late data применяет add и эмитит
               ОДИН Update event (был Retract+Data пара). *)
            let acc' = add acc v in
            emit_update (K.key v) stop acc acc';
            Managed_state.set state mk (FFired (acc', true))
        ) (assign spec t);
        pull ()
    in pull ()


(* ── Count windows ────────────────────────────────────────── *)

(** Спецификация count-окна: по числу событий, не по времени. *)
type count_spec =
  | CountTumbling of int          (** каждые N событий → окно *)
  | CountSliding  of int * int    (** окно размера N, шаг step (step<=N) *)

(** [count_window (module K) spec upstream] группирует события по ключу
    [K.key] и {e количеству}, а не времени. Окно эмитится когда у ключа
    накопилось нужное число событий — watermarks не нужны (это и есть
    их преимущество: результат без ожидания по event-time).

    - [count_tumbling n]: каждые [n] событий ключа → одно окно [(key, vs)],
      буфер сбрасывается.
    - [count_sliding n step]: окно из последних [n] событий, новое окно
      каждые [step] событий (при [step < n] окна перекрываются).

    Watermark и Retract проходят прозрачно. На конце потока неполные
    буферы {e не} эмитятся (count-окно по определению требует ровно [n]
    событий; неполный остаток — не окно).
    @raise Invalid_argument при [n <= 0] или [step <= 0]. *)
let count_window
    (type a)
    (module K : Keyed.S with type t = a)
    (spec : count_spec)
    (upstream : a Mf_event.t Stream.t)
    : (string * a list) Mf_event.t Stream.t =
  (match spec with
   | CountTumbling n -> if n <= 0 then invalid_arg "count_tumbling: n должно быть > 0"
   | CountSliding (n, step) ->
     if n <= 0 then invalid_arg "count_sliding: размер должен быть > 0";
     if step <= 0 then invalid_arg "count_sliding: шаг должен быть > 0");
  (* буфер на ключ: список накопленных значений (в обратном порядке) +
     счётчик с последней эмиссии (для sliding) + длина буфера (чтобы не
     звать List.length на каждое событие — O(n) → O(1)) *)
  let buffers : (string, a list * int * int) Hashtbl.t = Hashtbl.create 16 in
  let out : (string * a list) Mf_event.t Queue.t = Queue.create () in
  let emit k vs = Queue.push (Mf_event.data (k, vs) 0) out in
  let push_value k v =
    let (buf, since, len) = match Hashtbl.find_opt buffers k with
      | Some x -> x | None -> ([], 0, 0) in
    let buf = v :: buf in
    let since = since + 1 in
    let len = len + 1 in
    match spec with
    | CountTumbling n ->
      if len >= n then begin
        emit k (List.rev buf);
        Hashtbl.replace buffers k ([], 0, 0)
      end else
        Hashtbl.replace buffers k (buf, since, len)
    | CountSliding (n, step) ->
      (* держим максимум n последних; эмитим каждые step событий когда
         накоплено >= n *)
      let (buf, len) = if len > n
        then (match List.rev buf with _ :: rest -> (List.rev rest, len - 1)
                                     | [] -> ([], 0))
        else (buf, len) in
      if len >= n && since >= step then begin
        emit k (List.rev buf);
        Hashtbl.replace buffers k (buf, 0, len)   (* сдвиг: считаем step заново *)
      end else
        Hashtbl.replace buffers k (buf, since, len)
  in
  fun () ->
    let rec pull () =
      if not (Queue.is_empty out) then Some (Queue.pop out) else
      match upstream () with
      | None -> None   (* неполные буферы не эмитим *)
      | Some (Mf_event.Watermark wm) ->
        (* watermark несёт только время — пересоздаём в выходном типе *)
        Some (Mf_event.wm wm)
      | Some (Mf_event.Retract _) ->
        (* Count-окна группируют по ЧИСЛУ прибывших событий, а не по
           значениям. Retract («убрать событие») для такого окна не
           имеет смысла — окно определяется порядком прибытия, а не
           содержимым. Документированное ограничение, не TODO. *)
        pull ()
      | Some (Mf_event.Update _) ->
        (* Update тоже не транслируется в count-окно по той же
           причине (см. Retract выше) — count не зависит от значений. *)
        pull ()
      | Some (Mf_event.Data (v, _)) ->
        push_value (K.key v) v;
        pull ()
    in pull ()

(** Count-tumbling: окно каждые [n] событий. *)
let count_tumbling n = CountTumbling n

(** Count-sliding: окно из [n] событий с шагом [step]. *)
let count_sliding n step = CountSliding (n, step)

(* ── Global window + custom triggers ──────────────────────── *)

(** Решение триггера при поступлении события. *)
type trigger_action =
  | Continue        (** копить дальше *)
  | Fire            (** эмитить окно, оставить накопленное (накопительно) *)
  | FireAndPurge    (** эмитить окно и сбросить буфер *)

(** Триггер: чистая функция от (число накопленных, последнее значение) к
    решению. Отделяет политику «когда фаерить» от группировки. *)
type 'a trigger = count:int -> last:'a -> trigger_action

(** Триггер «каждые n событий». *)
let trigger_count n : 'a trigger =
  fun ~count ~last:_ -> if count >= n then FireAndPurge else Continue

(** Триггер по предикату: фаерит когда событие удовлетворяет условию
    (например тревожное значение → ранняя эмиссия). *)
let trigger_on_value (pred : 'a -> bool) : 'a trigger =
  fun ~count:_ ~last -> if pred last then Fire else Continue

(** [global_window (module K) ~trigger upstream] держит {e одно} окно на
    ключ (без временных/количественных границ) и фаерит его когда скажет
    [trigger]. Основа, на которой через триггеры строятся другие политики:
    assigner (одно окно) отделён от trigger (когда эмитить).

    [Fire] эмитит накопленное не очищая (накопительный результат);
    [FireAndPurge] эмитит и сбрасывает. На конце потока непустой остаток
    эмитится. *)
let global_window
    (type a)
    (module K : Keyed.S with type t = a)
    ~(trigger : a trigger)
    (upstream : a Mf_event.t Stream.t)
    : (string * a list) Mf_event.t Stream.t =
  let buffers : (string, a list) Hashtbl.t = Hashtbl.create 16 in
  (* множество ключей, чей буфер уже эмитирован и НЕ менялся с тех пор
     (нет новых данных). На конце потока такие не эмитим повторно. *)
  let clean : (string, unit) Hashtbl.t = Hashtbl.create 16 in
  let out : (string * a list) Mf_event.t Queue.t = Queue.create () in
  let get k = match Hashtbl.find_opt buffers k with Some b -> b | None -> [] in
  fun () ->
    let rec pull () =
      if not (Queue.is_empty out) then Some (Queue.pop out) else
      match upstream () with
      | None ->
        (* конец потока: эмитим только непустые буферы, в которых есть
           НЕ-эмитированные данные (не помечены clean). Это убирает дубль
           когда последним действием был Fire без новых событий после него. *)
        Hashtbl.iter (fun k buf ->
          if buf <> [] && not (Hashtbl.mem clean k) then
            Queue.push (Mf_event.data (k, List.rev buf) 0) out
        ) buffers;
        Hashtbl.reset buffers;
        if Queue.is_empty out then None else Some (Queue.pop out)
      | Some (Mf_event.Watermark wm) -> Some (Mf_event.wm wm)
      | Some (Mf_event.Retract (v, _)) ->
        (* global_window хранит буфер-список per ключ — убираем
           первое вхождение v. Не триггерим (retract уменьшает буфер,
           не повод для fire). *)
        let k = K.key v in
        (match Hashtbl.find_opt buffers k with
         | None -> ()
         | Some buf -> Hashtbl.replace buffers k (remove_first v buf));
        pull ()
      | Some (Mf_event.Update { old; new_value; _ }) ->
        (* Атомарная коррекция: убрать old, добавить new в буфер.
           Проверяем триггер на новом размере буфера (new может
           триггернуть fire так же как Data). *)
        let k = K.key new_value in
        Hashtbl.remove clean k;
        let buf0 = match Hashtbl.find_opt buffers k with
          | Some b -> remove_first old b | None -> [] in
        let buf = new_value :: buf0 in
        (match trigger ~count:(List.length buf) ~last:new_value with
         | Continue -> Hashtbl.replace buffers k buf
         | Fire ->
           Queue.push (Mf_event.data (k, List.rev buf) 0) out;
           Hashtbl.replace buffers k buf;
           Hashtbl.replace clean k ()
         | FireAndPurge ->
           Queue.push (Mf_event.data (k, List.rev buf) 0) out;
           Hashtbl.replace buffers k []);
        pull ()
      | Some (Mf_event.Data (v, _)) ->
        let k = K.key v in
        Hashtbl.remove clean k;             (* новые данные → буфер «грязный» *)
        let buf = v :: get k in
        (match trigger ~count:(List.length buf) ~last:v with
         | Continue -> Hashtbl.replace buffers k buf
         | Fire ->
           Queue.push (Mf_event.data (k, List.rev buf) 0) out;
           Hashtbl.replace buffers k buf;     (* накопительно *)
           Hashtbl.replace clean k ()         (* эмитировано, новых нет *)
         | FireAndPurge ->
           Queue.push (Mf_event.data (k, List.rev buf) 0) out;
           Hashtbl.replace buffers k []);     (* сброс — буфер пуст, не эмитится *)
        pull ()
    in pull ()

(* ── Session windows (с слиянием) ─────────────────────────── *)

(* Сессия одного ключа: интервал [start, last] + накопленные значения.
   gap определяет когда сессии сливаются и когда закрываются. *)
type 'a session = {
  s_start : int;          (* начало (мин. ts) *)
  s_last  : int;          (* конец (макс. ts) *)
  s_vals  : (int * 'a) list;  (* (ts, value), для слияния по времени *)
}

(** [session_window (module K) ~gap upstream] группирует события по ключу
    [K.key] в {e сессии} — периоды активности, разделённые паузами больше
    [gap]. В отличие от tumbling/sliding границы {e динамические}: сессия
    растёт пока приходят события в пределах [gap], и {b сливается} с
    соседней если новое событие перекрывает разрыв между ними.

    Это ломает допущение «окно = чистая функция от timestamp» (которое
    держит tumbling/sliding): сессия зависит от {e последовательности}
    событий, а не только от их времени. Поэтому это отдельный оператор
    со своим состоянием и логикой слияния.

    Сессия закрывается (эмитится [(key, vs)]) когда watermark проходит
    [last + gap] — позже события уже не могут её продлить. На конце потока
    все открытые сессии закрываются.
    @raise Invalid_argument при [gap <= 0]. *)
let session_window
    (type a)
    (module K : Keyed.S with type t = a)
    ~(gap : int)
    (upstream : a Mf_event.t Stream.t)
    : (string * a list) Mf_event.t Stream.t =
  if gap <= 0 then invalid_arg "session_window: gap должен быть > 0";
  (* активные сессии на ключ (список, обычно 1-2 штуки) *)
  let sessions : (string, a session list) Hashtbl.t = Hashtbl.create 16 in
  let out : (string * a list) Mf_event.t Queue.t = Queue.create () in

  (* добавить событие в сессии ключа, выполнив слияние при необходимости *)
  let add_event k ts v =
    let cur = match Hashtbl.find_opt sessions k with Some s -> s | None -> [] in
    (* новое «точечное» окно события *)
    let ev_session = { s_start = ts; s_last = ts; s_vals = [(ts, v)] } in
    (* сессия пересекается с событием если событие в пределах gap от неё:
       [s_start - gap, s_last + gap] *)
    let overlaps s =
      ts >= s.s_start - gap && ts <= s.s_last + gap in
    let (touched, untouched) = List.partition overlaps cur in
    (* сливаем все затронутые + новое событие в одну сессию *)
    let merged = List.fold_left (fun acc s ->
      { s_start = min acc.s_start s.s_start;
        s_last  = max acc.s_last s.s_last;
        s_vals  = s.s_vals @ acc.s_vals })
      ev_session touched in
    Hashtbl.replace sessions k (merged :: untouched)
  in

  (* закрыть сессии ключа, у которых last + gap <= wm *)
  let close_ready wm =
    (* Собираем изменения, потом применяем — нельзя мутировать sessions
       во время Hashtbl.iter по нему. Раньше тут стоял Hashtbl.copy
       (копия всей таблицы на каждый watermark, O(ключей)); вместо неё
       копим список отложенных правок (только затронутые ключи). *)
    let to_remove = ref [] in
    let to_replace = ref [] in
    Hashtbl.iter (fun k sess ->
      let (ready, still) = List.partition (fun s -> s.s_last + gap <= wm) sess in
      List.iter (fun s ->
        (* значения в порядке времени *)
        let vs = List.sort (fun (a,_) (b,_) -> compare a b) s.s_vals
                 |> List.map snd in
        Queue.push (Mf_event.data (k, vs) s.s_last) out) ready;
      if ready <> [] then
        (if still = [] then to_remove := k :: !to_remove
         else to_replace := (k, still) :: !to_replace)
    ) sessions;
    List.iter (Hashtbl.remove sessions) !to_remove;
    List.iter (fun (k, still) -> Hashtbl.replace sessions k still) !to_replace
  in

  fun () ->
    let rec pull () =
      if not (Queue.is_empty out) then Some (Queue.pop out) else
      match upstream () with
      | None ->
        (* конец потока: закрыть все открытые сессии *)
        Hashtbl.iter (fun k sess ->
          List.iter (fun s ->
            let vs = List.sort (fun (a,_) (b,_) -> compare a b) s.s_vals
                     |> List.map snd in
            Queue.push (Mf_event.data (k, vs) s.s_last) out) sess
        ) sessions;
        Hashtbl.reset sessions;
        if Queue.is_empty out then None else Some (Queue.pop out)
      | Some (Mf_event.Watermark wm) ->
        close_ready wm;
        Queue.push (Mf_event.wm wm) out;
        pull ()
      | Some (Mf_event.Retract _) ->
        (* Session-окна определяются временной близостью значений
           (gap-based merging). Убрать значение может изменить
           границы сессии [s_start, s_last] или даже РАСЩЕПИТЬ
           сессию на две. Это не простая list-операция — пришлось
           бы пересчитывать merging. Документированное ограничение:
           session_window не поддерживает retract на input. *)
        pull ()
      | Some (Mf_event.Update { new_value; ts; _ }) ->
        (* Update: conservative best-effort — добавляем new_value
           как Data. old НЕ убираем (см. ограничение Retract выше). *)
        add_event (K.key new_value) ts new_value;
        pull ()
      | Some (Mf_event.Data (v, t)) ->
        add_event (K.key v) t v;
        pull ()
    in pull ()


(* ── Instrumented window (метрики latency закрытия) ──────── *)
(** window с histogram для latency закрытия окна *)
let window_instrumented
    (type a)
    (module K : Keyed.S with type t = a)
    ?(latency = 0)
    ~observe_window_ms
    (spec : win_spec)
    (upstream : a Mf_event.t Stream.t)
    : (string * a list) Mf_event.t Stream.t =
  let tbl : (win_key, a list) Hashtbl.t = Hashtbl.create 4096 in
  let out : (string * a list) Mf_event.t Queue.t = Queue.create () in
  let close k stop vs =
    if vs <> [] then begin
      let t0 = int_of_float (Unix.gettimeofday () *. 1_000_000.) in
      Queue.push (Mf_event.data (k, List.rev vs) stop) out;
      let t1 = int_of_float (Unix.gettimeofday () *. 1_000_000.) in
      observe_window_ms (float_of_int (t1 - t0))
    end
  in
  fun () ->
    let rec pull () =
      if not (Queue.is_empty out) then Some (Queue.pop out) else
      match upstream () with
      | None ->
        Hashtbl.iter (fun (k,_,stop) vs -> close k stop vs) tbl;
        Hashtbl.clear tbl;
        if Queue.is_empty out then None else Some (Queue.pop out)
      | Some (Mf_event.Watermark wm) ->
        (* собираем ключи к закрытию, потом удаляем и эмитим *)
        let to_close = ref [] in
        Hashtbl.iter (fun key vs ->
          let (_,_,stop) = key in
          if stop + latency <= wm then to_close := (key, vs) :: !to_close
        ) tbl;
        List.iter (fun ((k,_,stop) as key, vs) ->
          Hashtbl.remove tbl key;
          close k stop vs
        ) !to_close;
        Queue.push (Mf_event.wm wm) out;
        pull ()
      | Some (Mf_event.Retract (v, t)) ->
        (* instrumented window хранит списки — убираем v из окон. *)
        List.iter (fun (s, stop) ->
          let mk = (K.key v, s, stop) in
          match Hashtbl.find_opt tbl mk with
          | None -> ()
          | Some vs -> Hashtbl.replace tbl mk (remove_first v vs)
        ) (assign spec t);
        pull ()
      | Some (Mf_event.Update { old; new_value; ts = t }) ->
        (* Атомарная коррекция: убрать old, добавить new. *)
        List.iter (fun (s, stop) ->
          let mk = (K.key new_value, s, stop) in
          let existing = Option.value ~default:[] (Hashtbl.find_opt tbl mk) in
          Hashtbl.replace tbl mk (new_value :: remove_first old existing)
        ) (assign spec t);
        pull ()
      | Some (Mf_event.Data (v,t)) ->
        List.iter (fun (s, stop) ->
          let mk = (K.key v, s, stop) in
          let existing = Option.value ~default:[] (Hashtbl.find_opt tbl mk) in
          Hashtbl.replace tbl mk (v :: existing)
        ) (assign spec t);
        pull ()
    in pull ()
