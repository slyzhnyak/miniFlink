(** Параллельная обработка с exactly-once: чекпойнты в стиле
    Chandy-Lamport (barrier alignment) поверх воркеров, шардированных по
    ключу, плюс восстановление через перемотку источника.

    {b Гарантия.} При сбое система перематывает источник на offset
    последнего закоммиченного чекпойнта и переобрабатывает хвост. В паре
    с {!transactional_sink} (2PC на выходе) это даёт сквозной
    exactly-once: каждое входное Data-событие отражается в выходе ровно
    один раз, даже при падении воркера и переигрывании.

    {b Как это работает.} Координатор инжектит барьеры в поток через
    каждые [checkpoint_every] событий. Когда все живые воркеры
    отчитались снапшотом для epoch, чекпойнт фиксируется: его
    [cp_offset] — позиция источника, СОГЛАСОВАННАЯ со снапшотом
    (см. поле [cp_offset]). На фиксации вызывается [ts_commit] синка,
    делая выход epoch видимым атомарно.

    Этот модуль скрывает внутреннюю механику координации (сбор
    снапшотов, выравнивание барьеров, сериализация). Наружу — типы
    чекпойнта/источника/синка, готовые синки и две точки входа:
    {!run_exactly_once} и {!recover}. *)

(** {1 Базовые типы} *)

type epoch = int
(** Номер чекпойнт-эпохи (монотонно растёт). *)

type offset = int
(** Позиция чтения в источнике. Для Kafka — offset партиции, для
    in-memory — индекс в потоке. *)

(** {1 Адресуемый источник} *)

(** Источник, который умеет не только читать события, но и сообщать
    позицию и перематываться на неё. Перемотка — то, что делает
    возможным exactly-once на входе: после сбоя источник
    устанавливается на offset последнего чекпойнта. *)
type 'a seekable_source = {
  pull     : unit -> 'a Mf_event.t option;  (** следующее событие или [None] в конце *)
  position : unit -> offset;                (** текущая позиция чтения *)
  seek     : offset -> unit;                (** перемотать на позицию *)
}

val seekable_of_list : 'a Mf_event.t list -> 'a seekable_source
(** Обернуть список в адресуемый источник (для тестов и in-memory). *)

(** {1 Чекпойнт} *)

(** Снапшот одного воркера в данной эпохе. [processed] — сколько
    Data-событий воркер обработал к моменту барьера; используется для
    проверки согласованности offset со снапшотом. *)
type worker_snapshot = {
  worker    : int;
  epoch     : epoch;
  state     : bytes;
  processed : int;
}

(** Полная запись чекпойнта. [cp_offset] — позиция источника,
    соответствующая ровно тому набору обработанных Data, что зафиксирован
    в снапшотах; благодаря этому восстановление перематывает источник в
    точку, согласованную с состоянием воркеров, независимо от того,
    сколько событий было in-flight в момент инжекта барьера. *)
type checkpoint = {
  cp_epoch     : epoch;
  cp_offset    : offset;
  cp_snapshots : worker_snapshot array;
}

(** {1 Хранилище чекпойнтов} *)

type checkpoint_store
(** Хранилище закоммиченных чекпойнтов. Потокобезопасно; опционально
    пишет на диск (см. {!durable_store}). *)

val make_store : ?persist:(checkpoint -> unit) -> unit -> checkpoint_store
(** In-memory хранилище. [?persist] — необязательный хук durable-записи,
    вызывается под мьютексом на каждый commit. *)

val commit : checkpoint_store -> checkpoint -> unit
(** Зафиксировать чекпойнт (и вызвать persist-хук, если задан). *)

val latest_checkpoint : checkpoint_store -> checkpoint option
(** Последний закоммиченный чекпойнт, если есть. *)

val checkpoint_count : checkpoint_store -> int
(** Сколько чекпойнтов закоммичено. *)

(** {2 Durable-хранилище} *)

val durable_store : dir:string -> checkpoint_store
(** Хранилище, пишущее каждый чекпойнт в [dir] (Marshal) с атомарным
    обновлением указателя LATEST через rename. *)

val load_durable : dir:string -> checkpoint_store
(** Загрузить ранее сохранённое durable-хранилище из [dir]. *)

(** {1 Транзакционный sink (2PC на выходе)} *)

(** Sink с протоколом two-phase commit: выход эпохи буферизуется
    ([ts_write]) и становится видимым только на [ts_commit], либо
    отбрасывается на [ts_abort]. Координатор зовёт commit/abort в
    привязке к границам чекпойнтов — так выход атомарен относительно
    состояния. *)
type 'b transactional_sink = {
  ts_write  : epoch -> 'b -> unit;  (** записать в pre-commit для эпохи *)
  ts_commit : epoch -> unit;        (** сделать видимым (чекпойнт прошёл) *)
  ts_abort  : epoch -> unit;        (** откатить (сбой до commit) *)
  ts_flush  : unit -> unit;         (** опубликовать хвост при штатном завершении *)
}

val idempotent_sink : ('b -> unit) -> 'b transactional_sink
(** Обернуть upsert-функцию. Commit/abort — no-op: повторная запись по
    тому же детерминированному ключу безвредна. Самый дешёвый корректный
    выход для sink с ключевой записью. *)

val buffered_sink : ('b list -> unit) -> 'b transactional_sink
(** Эмуляция 2PC для sink без нативных транзакций: накапливает выход
    эпохи в in-memory буфере, публикует пачкой на commit, отбрасывает на
    abort. *)

(** {1 Точки входа} *)

val run_exactly_once :
  workers:int ->
  capacity:int ->
  checkpoint_every:int ->
  key_of:('a -> string) ->
  make_state:(unit -> State_backend_memory.t) ->
  process:(State_backend_memory.t -> 'a Mf_event.t -> 'b list) ->
  source:'a seekable_source ->
  sink:'b transactional_sink ->
  store:checkpoint_store ->
  unit -> unit
(** Запустить exactly-once конвейер до исчерпания источника.

    События шардируются по [key_of] на [workers] воркеров (каждый —
    отдельный поток с каналом ёмкости [capacity]). [process] применяет
    пользовательскую логику к событию, обновляя per-key состояние
    ([make_state] на воркер) и возвращая выходные значения, которые идут
    в [sink] в pre-commit текущей эпохи. Барьер инжектится каждые
    [checkpoint_every] событий; на фиксации эпохи [sink.ts_commit]
    делает её выход видимым, а чекпойнт сохраняется в [store].

    Падение воркера не приводит к дедлоку: координатор не ждёт упавших,
    выжившие доводят обработку. Для восстановления после сбоя процесса
    используйте {!recover} с тем же [store]. *)

val recover :
  workers:int ->
  make_state:(unit -> State_backend_memory.t) ->
  source:'a seekable_source ->
  checkpoint_store ->
  State_backend_memory.t array
(** Восстановить состояние воркеров из последнего чекпойнта в [store] и
    перемотать [source] на его [cp_offset]. Возвращает массив backend'ов
    (по одному на воркер), готовых к продолжению обработки. Если
    чекпойнтов нет — холодный старт с позиции 0. *)

(** {1 Шардирование} *)

val hash_key : string -> int -> int
(** [hash_key key n] — номер воркера [0..n) для ключа. Детерминированно:
    одинаковый ключ всегда идёт к одному воркеру (key-affinity). *)
