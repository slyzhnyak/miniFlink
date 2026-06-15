(** Replayable обёртка для packet-источников.

    Превращает любой список packet'ов и gas-пакетов в источник с
    тем же интерфейсом что у [Mock_source.S], но с возможностью
    seek to offset (как Replayable_source). Это нужно для
    crash+restart сценариев: после рестарта потребитель открывает
    источник с committed offset'а, не с начала.

    Пример использования:
    {[
      (* Phase 1: создаём источник, читаем, коммитим offset *)
      let module Src = Replayable_mock.Make (struct
        let packets = packets_list
        let gas     = gas_list
      end) in
      let packets = Src.read () in
      (* ... обработка, периодически commit offset через
         backend.set "consumer:offset:packets"
                     (string_of_int (Src.packets_offset ())) ... *)

      (* Phase 2: после "рестарта", читаем offset из backend и
         открываем источник с этой позиции *)
      let module Src' = Replayable_mock.Make_at_offset (struct
        let packets = packets_list  (* тот же лог *)
        let gas     = gas_list
        let packets_start = restored_packets_offset
        let gas_start     = restored_gas_offset
      end) in
      let packets' = Src'.read () in
      (* ... продолжение с committed позиции ... *)
    ]}

    Прозрачность с {!Mock_source}: модуль удовлетворяет тому же
    [module type S], поэтому существующие пайплайны работают без
    изменений — нужно только заменить
    [module Src = Mock_source.Default] на
    [module Src = Replayable_mock.Make (...)]. *)

open Miniflink

(** Параметры для Make: списки event'ов в их естественном порядке. *)
module type Params = sig
  val packets : Domain.packet list
  val gas     : Domain.gas_packet list
end

(** Параметры для Make_at_offset: те же списки + начальные позиции. *)
module type Params_at_offset = sig
  include Params
  val packets_start : int
  val gas_start     : int
end

(** Расширенный интерфейс: Mock_source.S плюс accessor'ы для offset'ов. *)
module type S = sig
  include Mock_source.S
  val packets_offset : unit -> int
  val gas_offset     : unit -> int
end

(** Конструктор начинающий с offset=0 (полный replay). *)
module Make (P : Params) : S = struct
  let packets_src = Replayable_source.of_list P.packets
  let gas_src     = Replayable_source.of_list P.gas

  let p_stream_ref, p_off_ref =
    let s, o = Replayable_source.read_from packets_src in
    ref s, ref o
  let g_stream_ref, g_off_ref =
    let s, o = Replayable_source.read_from gas_src in
    ref s, ref o

  let read () =
    (* Конвертируем packet stream → Mf_event.t Stream.t.
       Watermark в конце для дренажа downstream'а. *)
    let s = !p_stream_ref in
    let done_ = ref false in
    let last_ts = ref 0 in
    let next () =
      if !done_ then None
      else
        match s () with
        | Some (p : Domain.packet) ->
          last_ts := p.ts;
          Some (Mf_event.data p p.ts)
        | None ->
          done_ := true;
          Some (Mf_event.wm (!last_ts + 1000))
    in
    next

  let read_gas () =
    let s = !g_stream_ref in
    let done_ = ref false in
    let last_ts = ref 0 in
    let next () =
      if !done_ then None
      else
        match s () with
        | Some (g : Domain.gas_packet) ->
          last_ts := g.g_ts;
          Some (Mf_event.data g g.g_ts)
        | None ->
          done_ := true;
          Some (Mf_event.wm (!last_ts + 1000))
    in
    next

  let packets_offset () = !p_off_ref ()
  let gas_offset     () = !g_off_ref ()

  let stats () = [
    Printf.sprintf "Replayable_mock.Make: %d packets, %d gas packets total"
      (List.length P.packets) (List.length P.gas);
    Printf.sprintf "  current offsets: packets=%d, gas=%d"
      (!p_off_ref ()) (!g_off_ref ());
  ]
end

(** Конструктор начинающий с заданных offset'ов (recovery после
    рестарта). *)
module Make_at_offset (P : Params_at_offset) : S = struct
  let packets_src = Replayable_source.of_list P.packets
  let gas_src     = Replayable_source.of_list P.gas

  let p_stream_ref, p_off_ref =
    let s, o = Replayable_source.read_from ~offset:P.packets_start packets_src in
    ref s, ref o
  let g_stream_ref, g_off_ref =
    let s, o = Replayable_source.read_from ~offset:P.gas_start gas_src in
    ref s, ref o

  let read () =
    let s = !p_stream_ref in
    let done_ = ref false in
    let last_ts = ref 0 in
    let next () =
      if !done_ then None
      else
        match s () with
        | Some (p : Domain.packet) ->
          last_ts := p.ts;
          Some (Mf_event.data p p.ts)
        | None ->
          done_ := true;
          Some (Mf_event.wm (!last_ts + 1000))
    in
    next

  let read_gas () =
    let s = !g_stream_ref in
    let done_ = ref false in
    let last_ts = ref 0 in
    let next () =
      if !done_ then None
      else
        match s () with
        | Some (g : Domain.gas_packet) ->
          last_ts := g.g_ts;
          Some (Mf_event.data g g.g_ts)
        | None ->
          done_ := true;
          Some (Mf_event.wm (!last_ts + 1000))
    in
    next

  let packets_offset () = !p_off_ref ()
  let gas_offset     () = !g_off_ref ()

  let stats () = [
    Printf.sprintf "Replayable_mock.Make_at_offset: %d packets, %d gas packets total"
      (List.length P.packets) (List.length P.gas);
    Printf.sprintf "  start offsets: packets=%d, gas=%d"
      P.packets_start P.gas_start;
    Printf.sprintf "  current offsets: packets=%d, gas=%d"
      (!p_off_ref ()) (!g_off_ref ());
  ]
end
