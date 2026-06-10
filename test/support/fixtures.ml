open Miniflink
let ev id t_s speed fuel =
  Mf_event.data
    { Domain.device_id = id; speed_kmh = speed; fuel_pct = fuel;
      position = { Domain.lat = 55.75; lon = 37.61 };
      ts = t_s * 1000; device = None }
    (t_s * 1000)

let scenario_alerts = [
  ev "truck_A"  0   60.  80.;
  ev "truck_B"  2  100.  60.;
  ev "truck_C"  1   50.  15.;
  ev "truck_A"  5   65.  79.;
  ev "truck_B"  7  145.  58.;
  ev "truck_A"  3   62.  79.;  (* late data *)
  ev "truck_C"  8   55.  12.;
  ev "truck_B"  12 148.  55.;
  ev "truck_A"  60  68.  75.;
  ev "truck_B"  61  92.  50.;
  ev "truck_C"  62  52.   9.;
  ev "truck_A" 120  66.  72.;
  ev "truck_B" 121  88.  48.;
  ev "truck_C" 122  54.   7.;
]
