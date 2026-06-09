(** Temporal join (versioned / as-of join).

    Обогащение потока справочными данными, актуальными {b на event-time
    события}, а не «текущими». Корректно даже когда апдейты справочника
    приходят с ОПОЗДАНИЕМ (поздно по порядку прихода, но с ранним
    valid_from): событие удерживается, пока watermark потока апдейтов не
    достигнет его event-time, и только тогда обогащается версией as-of.
    Аналог temporal table join во Flink ([FOR SYSTEM_TIME AS OF]). *)

(** Версионированная таблица: для каждого ключа — история [(valid_from,
    value)]. *)
type ('k, 'v) versioned

(** Пустая версионированная таблица. *)
val create_versioned : unit -> ('k, 'v) versioned

(** [put_version tbl ~key ~valid_from v] — значение [v] для [key]
    действует с момента [valid_from]. Порядок вставки не важен:
    опоздавший апдельт со старым [valid_from] встаёт на своё место в
    истории. *)
val put_version :
  ('k, 'v) versioned -> key:'k -> valid_from:Time.t -> 'v -> unit

(** [as_of tbl k ts] — значение [k], актуальное на момент [ts] (последняя
    версия с [valid_from <= ts]). [None], если на [ts] ключ ещё не имел
    значения. *)
val as_of : ('k, 'v) versioned -> 'k -> Time.t -> 'v option

(** [temporal_join ~key_main ~key_upd ~valid_from ~merge ~updates main]
    обогащает каждое событие [main] значением справочника, актуальным на
    event-time события.

    - [main] и [updates] {b оба несут watermarks};
    - [key_main]/[key_upd] — ключ join с каждой стороны;
    - [valid_from u] — с какого event-time апдейт [u] вступает в силу;
    - [merge событие (значение option)] — как обогатить (left join:
      [None] если на момент события значения ещё не было).

    Событие [main] с временем T удерживается в буфере, пока watermark
    апдейтов не достигнет T (тогда все апдейты с [valid_from <= T]
    гарантированно прибыли), затем обогащается и эмитится. Результат
    детерминирован по event-time независимо от порядка прихода —
    опоздавшие апдейты учитываются корректно. *)
val temporal_join :
  key_main:('a -> 'k) ->
  key_upd:('u -> 'k) ->
  valid_from:('u -> Time.t) ->
  merge:('a -> 'u option -> 'a) ->
  updates:'u Mf_event.t Stream.t ->
  'a Mf_event.t Stream.t -> 'a Mf_event.t Stream.t
