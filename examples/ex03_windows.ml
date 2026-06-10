(* ============================================================
   Пример 3 — типы окон.

   Показывает четыре способа группировки одного потока: по времени
   (tumbling), по количеству (count), по паузам активности (session),
   и по триггеру (global). Один набор данных, разные окна — видно
   как меняется группировка.

   Запуск: dune exec examples/03_windows.exe
   ============================================================ *)

open Miniflink

open Time

type click = { user : string; page : string; ts : int }

module User = Keyed.Make (struct
  type t = click
  let key c = c.user
end)

(* Клики пользователя A: всплеск, пауза, ещё всплеск *)
let clicks = [
  { user = "A"; page = "/home";    ts = seconds 1 };
  { user = "A"; page = "/catalog"; ts = seconds 2 };
  { user = "A"; page = "/item";    ts = seconds 3 };
  (* пауза 40 секунд *)
  { user = "A"; page = "/home";    ts = seconds 43 };
  { user = "A"; page = "/cart";    ts = seconds 44 };
]

let stream () =
  Stream.of_list (List.map (fun c -> Mf_event.data c c.ts) clicks)

let count_windows label s =
  Printf.printf "%s\n" label;
  s |> Stream.to_list |> List.iter (function
    | Mf_event.Data ((user, pages), _) ->
      Printf.printf "  %s: %d кликов [%s]\n"
        user (List.length pages) (String.concat ", " pages)
    | _ -> ());
  Printf.printf "\n"

let () =
  Printf.printf "=== Пример 3: один поток, разные типы окон ===\n\n";

  (* по времени: окна по 30 секунд *)
  stream ()
  |> Pipe.event_time ~lateness:0
  |> Pipe.window (module User) (Pipe.tumbling (seconds 30))
  |> Pipe.aggregate (fun u cs -> (u, List.map (fun c -> c.page) cs))
  |> count_windows "Tumbling 30с (по времени) — всплеск и второй всплеск в разных окнах:";

  (* по количеству: каждые 2 клика *)
  stream ()
  |> Pipe.count_window (module User) (Pipe.count_tumbling 2)
  |> Pipe.aggregate (fun u cs -> (u, List.map (fun c -> c.page) cs))
  |> count_windows "Count 2 (по количеству) — окно каждые 2 клика, watermark не нужен:";

  (* по сессиям: пауза > 20с разрывает сессию *)
  stream ()
  |> Pipe.session_window (module User) ~gap:(seconds 20)
  |> Pipe.aggregate (fun u cs -> (u, List.map (fun c -> c.page) cs))
  |> count_windows "Session gap 20с (по активности) — две сессии: до паузы и после:";

  (* global + триггер: фаерить когда зашли в корзину.
     Fire накопительный, но на конце потока без новых данных дубля нет —
     одна эмиссия при заходе в /cart. *)
  stream ()
  |> Pipe.global_window (module User)
       ~trigger:(Pipe.trigger_on_value (fun c -> c.page = "/cart"))
  |> Pipe.aggregate (fun u cs -> (u, List.map (fun c -> c.page) cs))
  |> count_windows "Global + trigger (по событию) — эмиссия при заходе в /cart:";

  (* aggregate выше копит ВЕСЬ список кликов в окне (List.map по cs) —
     удобно когда нужны сами события, но память растёт с числом событий.
     Если нужна лишь МЕТРИКА (счётчик, среднее), window_agg считает её
     инкрементально, O(1) памяти на окно — список не копится. *)
  Printf.printf "Count кликов в окне инкрементально (window_agg, без списка):\n";
  stream ()
  |> Pipe.event_time ~lateness:0
  |> Pipe.window_agg (module User) (Pipe.tumbling (seconds 30)) Agg.count
  |> Stream.to_list
  |> List.iter (function
     | Mf_event.Data ((user, n), wend) ->
       Printf.printf "  [%2ds] %s: %d кликов\n" (wend/1000) user n
     | _ -> ());
  Printf.printf "\n"
