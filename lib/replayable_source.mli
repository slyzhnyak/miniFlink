(** Kafka-like source с replay/seek-возможностью.

    Простая реализация поверх in-memory list. Главное свойство —
    можно начать чтение с заданного {b offset'а} (индекса в логе),
    не с начала. Это позволяет реализовать паттерн "crash + restart"
    из настоящих message broker'ов: consumer хранит свой
    последний committed offset в persistent storage; после рестарта
    читает offset, открывает source с этой позиции, продолжает с
    того места.

    {1 Чем не похоже на настоящую Kafka}

    - Single partition (нет распределённости)
    - Single consumer (нет consumer groups)
    - Только in-memory (нет диска)
    - Offset = индекс элемента в списке
    - Нет retention policy, log хранится всё время жизни source'а

    Этого достаточно для тестов и демонстрации persistence-паттерна.
    Для production обвязка с реальной Kafka — другой слой.

    {1 Использование}

    {[
      (* Создать source *)
      let src = Replayable_source.of_list events in

      (* Phase 1: читаем с начала, считаем offset *)
      let stream, get_offset = Replayable_source.read_from src in
      (* ... обработка через trigger с backend, periodically коммитим
         offset в тот же backend ... *)
      let committed_offset = get_offset () in

      (* "Crash" — потеряли in-memory state *)

      (* Phase 2: восстанавливаем offset из backend, продолжаем с него *)
      let restored_offset = restore_from_backend () in
      let stream', _ = Replayable_source.read_from ~offset:restored_offset src in
      (* ... обработка продолжается без пропусков и дублей ...*)
    ]}
*)

type 'a t
(** Replayable source с фиксированным log'ом. *)

val of_list : 'a list -> 'a t
(** Создать source из списка events. Порядок сохраняется. *)

val length : 'a t -> int
(** Сколько всего events в log'е. *)

val read_from :
  ?offset:int ->
  'a t ->
  'a Stream.t * (unit -> int)
(** [read_from ?offset src] возвращает [(stream, get_offset)]:

    - [stream] эмитит events начиная с [offset] (default 0). Когда
      log закончится, stream возвращает [None].
    - [get_offset ()] — текущая позиция в log'е (сколько events уже
      прочитано). Это значение пользователь может сохранить в
      backend как "committed offset" для recovery.

    [offset] должен быть [0 ≤ offset ≤ length src]. Иначе
    [Invalid_argument].

    Несколько [read_from] на одном source создают {b независимые}
    streams и offset-счётчики. Это полезно для тестов "что если
    мы перечитаем с того же места".

    {b ВНИМАНИЕ — НЕ thread-safe:} возвращённый [stream] хранит
    внутреннюю позицию через [ref int] без mutex'а. Использование
    одного [stream] из нескольких threads {b одновременно} даёт
    data race: возможен пропуск или дублирование events, и в
    худшем случае [Array_bounds_error] если pos инкрементируется
    параллельно за пределы log'а.

    Безопасно: один thread на один [stream]. Если нужны N parallel
    workers — создайте N независимых [read_from] (каждый со своим
    [ref]), либо обернитe [stream] в собственный mutex.

    В типичных pipeline'ах (один dispatcher thread) это
    ограничение не проявляется. *)
