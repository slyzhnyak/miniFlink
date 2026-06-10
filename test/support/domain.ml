open Miniflink
(* ============================================================
   Domain.ml — доменные типы + KEYED инстансы

   Каждый тип реализует Keyed.S — это убирает ~key: из pipeline.
   [@@deriving yojson] генерирует сериализацию.
   ============================================================ *)

(* ── Базовые типы ─────────────────────────────────────────── *)

type position = {
  lat : float;
  lon : float;
} [@@deriving yojson, show]

type device_info = {
  owner     : string;
  max_speed : float;
  zone      : string;
} [@@deriving yojson, show]

(* ── Телеметрия ───────────────────────────────────────────── *)

type telemetry = {
  device_id : string;
  speed_kmh : float;
  fuel_pct  : float;
  position  : position;
  ts        : int;
  device    : device_info option;   (* заполняется через enrich *)
} [@@deriving yojson, show]

module Telemetry = Keyed.Make(struct
  type t = telemetry
  let key t = t.device_id
end)

(* ── Статистика за окно ───────────────────────────────────── *)

type stats = {
  device_id : string;
  max_speed : float;
  avg_speed : float;
  min_fuel  : float;
  count     : int;
  device    : device_info option;
} [@@deriving yojson, show]

module Stats = Keyed.Make(struct
  type t = stats
  let key s = s.device_id
end)

(* ── Алерт ────────────────────────────────────────────────── *)

type severity = Critical | Warning | Info
  [@@deriving yojson, show]

type alert = {
  id        : string;
  device_id : string;
  rule      : string;
  severity  : severity;
  message   : string;
  ts        : int;
} [@@deriving yojson, show]

module Alert = Keyed.Make(struct
  type t = alert
  let key a = a.device_id
end)

(* ── Codecs ───────────────────────────────────────────────── *)

let telemetry_json = Codec.json
  ~encode:telemetry_to_yojson
  ~decode:telemetry_of_yojson

let alert_json = Codec.json
  ~encode:alert_to_yojson
  ~decode:alert_of_yojson
