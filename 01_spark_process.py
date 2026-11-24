"""
Phase 3 - Step 1: APACHE SPARK PROCESSING (PySpark + Spark SQL)
==============================================================
Turns the raw GTFS-Realtime archive (nested JSON, one snapshot per line) into
analysis tables that compare OBSERVED bus movement with the SCHEDULED timetable.

  1  Flatten   from_json + explode: nested entity[] -> one row per vehicle fix
  2  Clean     Spark SQL: bounding box, missing trip, de-duplicate (vehicle, timestamp)
  3  Pairs     window LAG per vehicle+trip -> distance, time gap, inferred speed (km/h)
  4  O1 speed  percentile_approx P10/P50/P90 per route x hour  (+ scheduled speed)
  5  O3 cells  250 m grid: median speed, stationary share away from stops (congestion)
  6  Arrivals  GPS segment x scheduled stops join -> interpolated arrival time + delay
  7  O2 gaps   observed vs scheduled headway for consecutive trips, bunching, EWT
  8  O4 cover  scheduled trips actually seen in the feed; GPS footprint vs route shapes
Each step is a Spark SQL query on temporary views, so the logic reads as SQL.

Run from the project root:   python phase3/01_spark_process.py
AI-assisted (Claude Code), 2026-11-24
"""
import json
import time

import pandas as pd
from pyspark.sql import functions as F

from spark_utils import ARCHIVE, LAT_MAX, LAT_MIN, LON_MAX, LON_MIN, OUT, get_spark, read_gtfs, read_jsonl, to_csv

T0 = time.time()
spark = get_spark("G12-01-process")
sql = spark.sql
print(f"Spark {spark.version}, {spark.sparkContext.defaultParallelism} cores")

# Haversine distance in km as a reusable SQL snippet
def hav(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = (f"({x})" for x in (lat1, lon1, lat2, lon2))
    return (f"(2*6371*ASIN(SQRT(POWER(SIN(RADIANS({lat2}-{lat1})/2),2) + "
            f"COS(RADIANS({lat1}))*COS(RADIANS({lat2}))*POWER(SIN(RADIANS({lon2}-{lon1})/2),2))))")

# Local planar coordinates (km) around Johor Bahru, used for projections and the 250 m grid
KX, KY, CELL_KM = 111.32 * 0.99966, 110.574, 0.25

# ---------------------------------------------------------------------------
# 1. FLATTEN the semi-structured feed
# ---------------------------------------------------------------------------
RT_SCHEMA = ("header STRUCT<gtfsRealtimeVersion:STRING, incrementality:STRING, timestamp:STRING>, "
             "entity ARRAY<STRUCT<id:STRING, vehicle:STRUCT<"
             "trip:STRUCT<tripId:STRING, startDate:STRING, routeId:STRING, directionId:INT>, "
             "position:STRUCT<latitude:DOUBLE, longitude:DOUBLE, bearing:DOUBLE, speed:DOUBLE>, "
             "timestamp:STRING, vehicle:STRUCT<id:STRING, label:STRING, licensePlate:STRING>>>>, "
             "_ingested_at STRING")

raw = read_jsonl(spark, ARCHIVE / "date=*" / "positions.jsonl")
raw.selectExpr(f"from_json(value, '{RT_SCHEMA}') AS doc").createOrReplaceTempView("rt_doc")

sql("""
  SELECT CAST(doc.header.timestamp AS BIGINT)        AS feed_ts,
         e.id                                        AS entity_id,
         CAST(e.vehicle.timestamp AS BIGINT)         AS ts,
         e.vehicle.vehicle.id                        AS vehicle_id,
         e.vehicle.trip.tripId                       AS trip_id,
         e.vehicle.trip.startDate                    AS start_date,
         e.vehicle.trip.routeId                      AS route_id,
         e.vehicle.trip.directionId                  AS direction_id,
         e.vehicle.position.latitude                 AS lat,
         e.vehicle.position.longitude                AS lon
  FROM rt_doc LATERAL VIEW explode(doc.entity) x AS e
""").cache().createOrReplaceTempView("rt_flat")

# ---------------------------------------------------------------------------
# 2. CLEAN (Spark SQL) + data-quality log
# ---------------------------------------------------------------------------
trips = read_gtfs(spark, "trips"); trips.createOrReplaceTempView("g_trips")
routes = read_gtfs(spark, "routes"); routes.createOrReplaceTempView("g_routes")
stops = read_gtfs(spark, "stops"); stops.createOrReplaceTempView("g_stops_raw")
read_gtfs(spark, "stop_times").createOrReplaceTempView("g_stop_times_raw")
read_gtfs(spark, "shapes").createOrReplaceTempView("g_shapes_raw")

sql(f"""
  SELECT f.*, r.route_short_name AS route,
         ts - unix_timestamp(start_date, 'yyyyMMdd')               AS svc_sec,   -- seconds since service-day midnight (MYT)
         to_date(from_unixtime(ts))                                AS obs_date,
         hour(from_unixtime(ts))                                   AS hour,
         CASE WHEN dayofweek(from_unixtime(ts)) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END AS day_type
  FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY vehicle_id, ts ORDER BY feed_ts) AS rn FROM rt_flat) f
  JOIN g_trips t  ON t.trip_id = f.trip_id                         -- drops fixes without a valid timetable trip
  JOIN g_routes r ON r.route_id = f.route_id
  WHERE rn = 1
    AND lat BETWEEN {LAT_MIN} AND {LAT_MAX} AND lon BETWEEN {LON_MIN} AND {LON_MAX}
    AND vehicle_id IS NOT NULL AND ts IS NOT NULL
""").drop("rn").cache().createOrReplaceTempView("rt")

quality = sql(f"""
  SELECT 'snapshots (feed versions)'            AS step, COUNT(DISTINCT feed_ts) AS n FROM rt_flat
  UNION ALL SELECT 'vehicle records (raw)',          COUNT(*) FROM rt_flat
  UNION ALL SELECT 'records with no trip_id',        SUM(CASE WHEN trip_id IS NULL THEN 1 ELSE 0 END) FROM rt_flat
  UNION ALL SELECT 'records with trip_id not in GTFS', SUM(CASE WHEN f.trip_id IS NOT NULL AND t.trip_id IS NULL THEN 1 ELSE 0 END)
            FROM rt_flat f LEFT JOIN g_trips t ON t.trip_id = f.trip_id
  UNION ALL SELECT 'records outside Johor bbox',     SUM(CASE WHEN lat NOT BETWEEN {LAT_MIN} AND {LAT_MAX} OR lon NOT BETWEEN {LON_MIN} AND {LON_MAX} THEN 1 ELSE 0 END) FROM rt_flat
  UNION ALL SELECT 'duplicate (vehicle, timestamp)', COUNT(*) - COUNT(DISTINCT vehicle_id, ts) FROM rt_flat
  UNION ALL SELECT 'clean vehicle records',          COUNT(*) FROM rt
  UNION ALL SELECT 'distinct vehicles',              COUNT(DISTINCT vehicle_id) FROM rt
  UNION ALL SELECT 'distinct trips observed',        COUNT(DISTINCT trip_id, start_date) FROM rt
  UNION ALL SELECT 'distinct routes observed',       COUNT(DISTINCT route_id) FROM rt
""")
q = to_csv(quality, "p3_quality_log")
print(q.to_string(index=False))

# ---------------------------------------------------------------------------
# Scheduled side (structured GTFS) in Spark SQL
# ---------------------------------------------------------------------------
gtfs_sec = lambda c: (f"(CAST(split({c}, ':')[0] AS INT)*3600 + CAST(split({c}, ':')[1] AS INT)*60 "
                      f"+ CAST(split({c}, ':')[2] AS INT))")
sql("SELECT stop_id, stop_code, stop_name, CAST(stop_lat AS DOUBLE) AS slat, CAST(stop_lon AS DOUBLE) AS slon "
    "FROM g_stops_raw").cache().createOrReplaceTempView("g_stops")
sql(f"""
  SELECT st.trip_id, st.stop_id, CAST(st.stop_sequence AS INT) AS stop_sequence,
         {gtfs_sec('st.arrival_time')} AS arr_sec, {gtfs_sec('st.departure_time')} AS dep_sec,
         t.route_id, CAST(t.direction_id AS INT) AS direction_id, s.slat, s.slon
  FROM g_stop_times_raw st JOIN g_trips t ON t.trip_id = st.trip_id JOIN g_stops s ON s.stop_id = st.stop_id
""").cache().createOrReplaceTempView("g_st")

# scheduled segment speed by route x hour (same definition as Phase 2, now in Spark)
sql(f"""
  WITH nxt AS (
    SELECT route_id, dep_sec, slat, slon, LEAD(slat) OVER w AS nlat, LEAD(slon) OVER w AS nlon,
           LEAD(arr_sec) OVER w AS narr
    FROM g_st WINDOW w AS (PARTITION BY trip_id ORDER BY stop_sequence)),
  seg AS (
    SELECT route_id, CAST(FLOOR(dep_sec / 3600) % 24 AS INT) AS hour,
           {hav('slat', 'slon', 'nlat', 'nlon')} AS km, (narr - dep_sec) / 3600.0 AS h
    FROM nxt WHERE narr IS NOT NULL)
  SELECT r.route_short_name AS route, hour, COUNT(*) AS n_seg,
         percentile_approx(km / h, 0.5) AS sched_speed_p50, SUM(km) / SUM(h) AS sched_speed_mean
  FROM seg JOIN g_routes r USING (route_id)
  WHERE h > 0 AND km / h <= 90
  GROUP BY r.route_short_name, hour
""").createOrReplaceTempView("sched_speed")

# ---------------------------------------------------------------------------
# 3. CONSECUTIVE GPS PAIRS -> inferred speed
# ---------------------------------------------------------------------------
sql(f"""
  WITH lagged AS (
    SELECT *, LAG(lat) OVER w AS plat, LAG(lon) OVER w AS plon, LAG(ts) OVER w AS pts, LAG(svc_sec) OVER w AS psvc
    FROM rt WINDOW w AS (PARTITION BY vehicle_id, trip_id, start_date ORDER BY ts))
  SELECT vehicle_id, trip_id, start_date, route_id, route, direction_id, obs_date, hour, day_type,
         plat, plon, lat, lon, pts, ts, psvc, svc_sec,
         ts - pts AS dt_s,
         {hav('plat', 'plon', 'lat', 'lon')} AS km,
         {hav('plat', 'plon', 'lat', 'lon')} / ((ts - pts) / 3600.0) AS speed_kmh,
         (plat + lat) / 2 AS mlat, (plon + lon) / 2 AS mlon
  FROM lagged
  WHERE pts IS NOT NULL AND ts - pts BETWEEN 10 AND 180          -- ignore long tracking gaps
""").createOrReplaceTempView("pairs_all")
sql("SELECT * FROM pairs_all WHERE speed_kmh <= 90").cache().createOrReplaceTempView("pairs")   # drop GPS jumps

n_pairs_all, n_pairs = sql("SELECT COUNT(*) FROM pairs_all").first()[0], sql("SELECT COUNT(*) FROM pairs").first()[0]
print(f"GPS pairs: {n_pairs_all:,} -> {n_pairs:,} after removing implausible jumps (> 90 km/h)")

# ---------------------------------------------------------------------------
# 5. OBJECTIVE 3: 250 m congestion grid (dwell at stops separated from traffic delay)
# ---------------------------------------------------------------------------
cell = lambda lat, lon: (f"CAST(FLOOR({lon} * {KX} / {CELL_KM}) AS BIGINT)", f"CAST(FLOOR({lat} * {KY} / {CELL_KM}) AS BIGINT)")
cx, cy = cell("mlat", "mlon")
sx, sy = cell("slat", "slon")
sql(f"SELECT *, {cx} AS cx, {cy} AS cy FROM pairs").createOrReplaceTempView("pairs_c")
# route termini = first and last stop of every scheduled trip
sql("""SELECT DISTINCT st.stop_id FROM g_st st
       JOIN (SELECT trip_id, MIN(stop_sequence) AS mn, MAX(stop_sequence) AS mx FROM g_st GROUP BY trip_id) m
         ON m.trip_id = st.trip_id AND st.stop_sequence IN (m.mn, m.mx)
    """).cache().createOrReplaceTempView("terminals")
# every stop is copied into its own and the 8 neighbouring cells, so a pair only meets nearby stops
sql(f"""SELECT g.stop_id, slat, slon, {sx} + dx AS cx, {sy} + dy AS cy, t.stop_id IS NOT NULL AS is_term
        FROM g_stops g LEFT JOIN terminals t ON t.stop_id = g.stop_id
        LATERAL VIEW explode(array(-1,0,1)) a AS dx LATERAL VIEW explode(array(-1,0,1)) b AS dy
     """).createOrReplaceTempView("stops_nb")
sql(f"""
  SELECT p.*, COALESCE(MIN(d) OVER (PARTITION BY p.vehicle_id, p.ts), 9) AS stop_km,
         COALESCE(MIN(CASE WHEN is_term THEN d END) OVER (PARTITION BY p.vehicle_id, p.ts), 9) AS term_km
  FROM (SELECT p.*, s.is_term, {hav('p.mlat', 'p.mlon', 's.slat', 's.slon')} AS d
        FROM pairs_c p LEFT JOIN stops_nb s ON s.cx = p.cx AND s.cy = p.cy) p
""").drop("d", "is_term").dropDuplicates(["vehicle_id", "ts"]).cache().createOrReplaceTempView("pairs_s")
# terminal layovers (bus parked within 150 m of a route terminus) are not "travel" -> excluded from speed stats
sql("SELECT * FROM pairs_s WHERE term_km > 0.15").cache().createOrReplaceTempView("pairs_m")
n_m = sql("SELECT COUNT(*) FROM pairs_m").first()[0]
print(f"moving pairs (terminal layovers removed): {n_m:,}")

# ---------------------------------------------------------------------------
# 4. OBJECTIVE 1: observed speed by route x hour  (vs scheduled)
# ---------------------------------------------------------------------------
to_csv(sql("""
  SELECT p.route, p.hour, COUNT(*) AS n_pairs, COUNT(DISTINCT p.vehicle_id) AS n_vehicles,
         percentile_approx(speed_kmh, 0.1) AS p10, percentile_approx(speed_kmh, 0.5) AS p50,
         percentile_approx(speed_kmh, 0.9) AS p90,
         SUM(km) / (SUM(dt_s) / 3600.0) AS speed_mean,                  -- distance-weighted mean
         AVG(CASE WHEN speed_kmh < 5 THEN 1.0 ELSE 0 END) AS share_stationary,
         s.sched_speed_p50, s.sched_speed_mean
  FROM pairs_m p LEFT JOIN sched_speed s ON s.route = p.route AND s.hour = p.hour
  GROUP BY p.route, p.hour, s.sched_speed_p50, s.sched_speed_mean
  ORDER BY p.route, p.hour"""), "p3_speed_route_hour")

to_csv(sql("""
  SELECT day_type, hour, COUNT(*) AS n_pairs,
         percentile_approx(speed_kmh, 0.1) AS p10, percentile_approx(speed_kmh, 0.25) AS p25,
         percentile_approx(speed_kmh, 0.5) AS p50, percentile_approx(speed_kmh, 0.75) AS p75,
         percentile_approx(speed_kmh, 0.9) AS p90, SUM(km) / (SUM(dt_s) / 3600.0) AS speed_mean
  FROM pairs_m GROUP BY day_type, hour ORDER BY day_type, hour"""), "p3_speed_hour")

to_csv(sql(f"""
  SELECT cx, cy, (cx + 0.5) * {CELL_KM} / {KX} AS lon, (cy + 0.5) * {CELL_KM} / {KY} AS lat,
         COUNT(*) AS n_pairs, COUNT(DISTINCT vehicle_id) AS n_vehicles, COUNT(DISTINCT route) AS n_routes,
         percentile_approx(speed_kmh, 0.5) AS p50_speed, percentile_approx(speed_kmh, 0.1) AS p10_speed,
         STDDEV(speed_kmh) AS sd_speed,
         AVG(CASE WHEN speed_kmh < 5 AND stop_km > 0.05 THEN 1.0 ELSE 0 END) AS share_stopped_offstop,
         AVG(CASE WHEN speed_kmh < 5 AND stop_km <= 0.05 THEN 1.0 ELSE 0 END) AS share_dwell_at_stop,
         AVG(CASE WHEN hour IN (7, 8, 17, 18) THEN 1.0 ELSE 0 END) AS share_peak_obs,
         percentile_approx(CASE WHEN hour IN (7, 8, 17, 18) THEN speed_kmh END, 0.5) AS p50_speed_peak,
         percentile_approx(CASE WHEN hour NOT IN (7, 8, 17, 18) THEN speed_kmh END, 0.5) AS p50_speed_offpeak,
         MIN(stop_km) AS min_stop_km
  FROM pairs_s GROUP BY cx, cy"""), "p3_grid_cells")

# ---------------------------------------------------------------------------
# 6. ARRIVALS: project each scheduled stop of the trip onto the GPS segment
#    between two fixes; if it passes within 60 m, interpolate the arrival time.
# ---------------------------------------------------------------------------
sql(f"""
  WITH cand AS (
    SELECT p.trip_id, p.start_date, p.vehicle_id, p.route, p.route_id, p.direction_id, p.day_type,
           s.stop_id, s.stop_sequence, s.arr_sec,
           (s.slon-p.plon)*{KX} AS sx, (s.slat-p.plat)*{KY} AS sy,
           (p.lon-p.plon)*{KX} AS vx, (p.lat-p.plat)*{KY} AS vy, p.psvc, p.svc_sec
    FROM pairs p JOIN g_st s ON s.trip_id = p.trip_id
    WHERE s.slat BETWEEN LEAST(p.plat, p.lat) - 0.001 AND GREATEST(p.plat, p.lat) + 0.001
      AND s.slon BETWEEN LEAST(p.plon, p.lon) - 0.001 AND GREATEST(p.plon, p.lon) + 0.001),
  proj AS (
    SELECT *, CASE WHEN vx*vx + vy*vy < 1e-6 THEN 0
                   ELSE GREATEST(0, LEAST(1, (sx*vx + sy*vy) / (vx*vx + vy*vy))) END AS t
    FROM cand),
  hit AS (
    SELECT *, psvc + t * (svc_sec - psvc) AS obs_sec,
           SQRT(POWER(sx - t*vx, 2) + POWER(sy - t*vy, 2)) AS perp_km
    FROM proj),
  ok AS (
    SELECT *, obs_sec - arr_sec AS delay_s FROM hit WHERE perp_km <= 0.06 AND ABS(obs_sec - arr_sec) <= 5400),
  best AS (   -- the pass closest to the timetable (handles loop routes that visit a stop twice)
    SELECT *, FIRST_VALUE(obs_sec) OVER (PARTITION BY trip_id, start_date, stop_sequence ORDER BY ABS(delay_s)) AS best_sec
    FROM ok)
  SELECT trip_id, start_date, stop_sequence, FIRST(vehicle_id) AS vehicle_id, FIRST(route) AS route,
         FIRST(route_id) AS route_id, FIRST(direction_id) AS direction_id, FIRST(day_type) AS day_type,
         FIRST(stop_id) AS stop_id, FIRST(arr_sec) AS sched_sec,
         MIN(obs_sec) AS obs_sec                                     -- first moment at the stop in that pass
  FROM best WHERE obs_sec BETWEEN best_sec - 600 AND best_sec + 600
  GROUP BY trip_id, start_date, stop_sequence
""").withColumn("delay_min", F.expr("(obs_sec - sched_sec) / 60.0")).cache().createOrReplaceTempView("arrivals")

sql("""
  SELECT a.*, CAST(FLOOR(sched_sec / 3600) % 24 AS INT) AS hour,
         a.stop_sequence / m.n_stops AS trip_progress, m.n_stops,
         date_format(to_date(start_date, 'yyyyMMdd'), 'E') AS dow
  FROM arrivals a JOIN (SELECT trip_id, MAX(stop_sequence) AS n_stops FROM g_st GROUP BY trip_id) m USING (trip_id)
""").createOrReplaceTempView("arrivals_x")
arr = to_csv(sql("SELECT * FROM arrivals_x ORDER BY route, start_date, trip_id, stop_sequence"), "p3_arrivals")

to_csv(sql("""
  SELECT route, hour, day_type, COUNT(*) AS n_arrivals, COUNT(DISTINCT trip_id, start_date) AS n_trips,
         percentile_approx(delay_min, 0.5) AS median_delay_min, AVG(delay_min) AS mean_delay_min,
         percentile_approx(delay_min, 0.9) AS p90_delay_min,
         AVG(CASE WHEN delay_min BETWEEN -1 AND 5 THEN 1.0 ELSE 0 END) AS on_time_share,
         AVG(CASE WHEN delay_min < -1 THEN 1.0 ELSE 0 END) AS early_share,
         AVG(CASE WHEN delay_min > 5 THEN 1.0 ELSE 0 END) AS late_share
  FROM arrivals_x WHERE stop_sequence > 1          -- first stop = terminal departure, not an arrival
  GROUP BY route, hour, day_type ORDER BY route, hour"""), "p3_adherence_route_hour")

# ---------------------------------------------------------------------------
# 7. OBJECTIVE 2: observed vs scheduled headway for trips that are consecutive
#    in the timetable at the same stop, both observed.
# ---------------------------------------------------------------------------
sql("""
  SELECT route_id, direction_id, stop_id, trip_id, arr_sec,
         LAG(trip_id) OVER w AS prev_trip, arr_sec - LAG(arr_sec) OVER w AS sched_h_s
  FROM g_st WINDOW w AS (PARTITION BY route_id, direction_id, stop_id ORDER BY arr_sec)
""").createOrReplaceTempView("sched_seq")
sql("""
  SELECT c.route, c.route_id, c.direction_id, c.stop_id, c.start_date, c.day_type, c.trip_id, q.prev_trip,
         CAST(FLOOR(c.sched_sec / 3600) % 24 AS INT) AS hour,
         q.sched_h_s / 60.0 AS sched_h_min, (c.obs_sec - p.obs_sec) / 60.0 AS obs_h_min
  FROM arrivals c
  JOIN sched_seq q ON q.trip_id = c.trip_id AND q.stop_id = c.stop_id AND q.arr_sec = c.sched_sec
  JOIN arrivals p  ON p.trip_id = q.prev_trip AND p.stop_id = c.stop_id AND p.start_date = c.start_date
  WHERE q.prev_trip <> c.trip_id AND q.sched_h_s BETWEEN 60 AND 7200
""").withColumn("ratio", F.expr("obs_h_min / sched_h_min")) \
    .cache().createOrReplaceTempView("hw")
to_csv(sql("SELECT * FROM hw"), "p3_headway_pairs")

hw_route = to_csv(sql("""
  SELECT route, COUNT(*) AS n_pairs, COUNT(DISTINCT stop_id) AS n_stops,
         percentile_approx(sched_h_min, 0.5) AS sched_headway_p50,
         percentile_approx(obs_h_min, 0.5) AS obs_headway_p50,
         STDDEV(obs_h_min) / AVG(obs_h_min) AS cv_obs_headway,
         AVG(CASE WHEN ratio < 0.5 THEN 1.0 ELSE 0 END) AS bunching_share,     -- bus arrives < half the planned gap
         AVG(CASE WHEN ratio > 1.5 THEN 1.0 ELSE 0 END) AS long_gap_share,     -- gap > 1.5x planned
         SUM(POWER(GREATEST(obs_h_min, 0), 2)) / (2 * SUM(GREATEST(obs_h_min, 0))) AS awt_min,   -- actual wait
         SUM(POWER(sched_h_min, 2)) / (2 * SUM(sched_h_min)) AS swt_min                          -- scheduled wait
  FROM hw GROUP BY route
""").withColumn("ewt_min", F.expr("awt_min - swt_min"))
          .orderBy("route"), "p3_headway_route")
print(hw_route.to_string(index=False))

to_csv(sql("""
  SELECT hour, day_type, COUNT(*) AS n_pairs,
         AVG(CASE WHEN ratio < 0.5 THEN 1.0 ELSE 0 END) AS bunching_share,
         AVG(CASE WHEN ratio > 1.5 THEN 1.0 ELSE 0 END) AS long_gap_share,
         SUM(POWER(GREATEST(obs_h_min, 0), 2)) / (2 * SUM(GREATEST(obs_h_min, 0)))
           - SUM(POWER(sched_h_min, 2)) / (2 * SUM(sched_h_min)) AS ewt_min
  FROM hw GROUP BY hour, day_type ORDER BY day_type, hour"""), "p3_headway_hour")

# ---------------------------------------------------------------------------
# 8. OBJECTIVE 4: coverage - were scheduled trips seen, and does GPS cover the routes?
# ---------------------------------------------------------------------------
# hours with a healthy feed (>= 60 of ~120 possible snapshots) define the observation window
sql("""
  SELECT obs_date, hour, COUNT(DISTINCT feed_ts) AS snaps
  FROM (SELECT feed_ts, to_date(from_unixtime(feed_ts)) AS obs_date, hour(from_unixtime(feed_ts)) AS hour FROM rt_flat)
  GROUP BY obs_date, hour HAVING COUNT(DISTINCT feed_ts) >= 60
""").cache().createOrReplaceTempView("covered_hours")
to_csv(sql("SELECT * FROM covered_hours ORDER BY obs_date, hour"), "p3_feed_hours")

sql("""
  SELECT trip_id, route_id, MIN(dep_sec) AS start_sec, MAX(arr_sec) AS end_sec FROM g_st GROUP BY trip_id, route_id
""").createOrReplaceTempView("trip_span")
# a scheduled trip is "expected" on a date if both its start and end hour were fully covered by the feed
sql("""
  SELECT d.obs_date, ts.trip_id, ts.route_id,
         CASE WHEN dayofweek(d.obs_date) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END AS day_type
  FROM (SELECT DISTINCT obs_date FROM covered_hours) d CROSS JOIN trip_span ts
  JOIN covered_hours h1 ON h1.obs_date = d.obs_date AND h1.hour = CAST(FLOOR(ts.start_sec / 3600) AS INT)
  JOIN covered_hours h2 ON h2.obs_date = d.obs_date AND h2.hour = CAST(FLOOR(ts.end_sec / 3600) AS INT)
  WHERE ts.end_sec < 86400
""").createOrReplaceTempView("expected")
to_csv(sql("""
  SELECT r.route_short_name AS route, e.day_type, COUNT(*) AS trips_expected,
         SUM(CASE WHEN o.trip_id IS NOT NULL THEN 1 ELSE 0 END) AS trips_seen,
         AVG(CASE WHEN o.trip_id IS NOT NULL THEN 1.0 ELSE 0 END) AS share_seen,
         AVG(o.stops_matched / o.n_stops) AS avg_share_stops_matched
  FROM expected e JOIN g_routes r ON r.route_id = e.route_id
  LEFT JOIN (SELECT trip_id, to_date(start_date, 'yyyyMMdd') AS obs_date, COUNT(*) AS stops_matched, MAX(n_stops) AS n_stops
             FROM arrivals_x GROUP BY trip_id, start_date) o
         ON o.trip_id = e.trip_id AND o.obs_date = e.obs_date
  GROUP BY r.route_short_name, e.day_type ORDER BY route"""), "p3_trip_coverage")

# GPS footprint: share of each route's 250 m shape cells that contain at least one GPS pair
sx2, sy2 = cell("CAST(shape_pt_lat AS DOUBLE)", "CAST(shape_pt_lon AS DOUBLE)")
sql(f"""SELECT DISTINCT regexp_extract(shape_id, 'shape_(.*)_[0-9]+$', 1) AS route_id, {sx2} AS cx, {sy2} AS cy
        FROM g_shapes_raw""").createOrReplaceTempView("shape_cells")
to_csv(sql("""
  SELECT r.route_short_name AS route, COUNT(*) AS shape_cells,
         SUM(CASE WHEN o.cx IS NOT NULL THEN 1 ELSE 0 END) AS cells_with_gps,
         AVG(CASE WHEN o.cx IS NOT NULL THEN 1.0 ELSE 0 END) AS footprint_share
  FROM shape_cells s JOIN g_routes r ON r.route_id = s.route_id
  LEFT JOIN (SELECT DISTINCT route_id, cx, cy FROM pairs_c) o ON o.route_id = s.route_id AND o.cx = s.cx AND o.cy = s.cy
  GROUP BY r.route_short_name ORDER BY route"""), "p3_footprint")

# ---------------------------------------------------------------------------
# Volume summary + compact archive of clean positions (Parquet, columnar) for R
# ---------------------------------------------------------------------------
vol = sql("""SELECT COUNT(DISTINCT feed_ts) AS snapshots, COUNT(*) AS records,
                    MIN(from_unixtime(feed_ts)) AS first_snapshot, MAX(from_unixtime(feed_ts)) AS last_snapshot,
                    COUNT(DISTINCT to_date(from_unixtime(feed_ts))) AS days FROM rt_flat""").toPandas().iloc[0].to_dict()
raw_bytes = sum(p.stat().st_size for p in ARCHIVE.glob("date=*/positions.jsonl"))
vol.update(raw_json_mb=round(raw_bytes / 1e6, 1), clean_records=int(q.loc[q.step == "clean vehicle records", "n"].iloc[0]),
           gps_pairs=int(n_pairs), arrivals=int(len(arr)), runtime_s=round(time.time() - T0, 1))
pd.DataFrame([vol]).to_csv(OUT / "p3_volume.csv", index=False)
sql("SELECT vehicle_id, trip_id, start_date, route, direction_id, ts, lat, lon, hour, day_type, speed_kmh, stop_km "
    "FROM pairs_s").toPandas().to_parquet(OUT / "p3_pairs.parquet", index=False)
sql("SELECT route, hour, day_type, speed_kmh, km, dt_s FROM pairs").toPandas().to_csv(OUT / "p3_pairs_slim.csv.gz", index=False)
print(json.dumps(vol, indent=1, default=str))
spark.stop()
print(f"Step 1 (Spark) done in {time.time() - T0:.1f}s -> phase3/output/")
