-- =============================================================================
-- Phase 3 - analytical SQL views (run by 04_store_sql.R)
-- AI-assisted (Claude Code), 2026-11-24
-- =============================================================================

-- Scheduled headway at every stop: gap to the previous bus of the same route+direction
DROP VIEW IF EXISTS v_sched_headway;
CREATE VIEW v_sched_headway AS
SELECT t.route_id, t.direction_id, st.stop_id, st.trip_id, st.arr_sec,
       (st.arr_sec / 3600) % 24 AS hour,
       (st.arr_sec - LAG(st.arr_sec) OVER (PARTITION BY t.route_id, t.direction_id, st.stop_id
                                            ORDER BY st.arr_sec)) / 60.0 AS headway_min
FROM stop_times st JOIN trips t ON t.trip_id = st.trip_id;

-- Scheduled service per route: span of operation, trips/day, typical headway at the first stop
DROP VIEW IF EXISTS v_route_service;
CREATE VIEW v_route_service AS
SELECT r.route, r.route_long_name,
       COUNT(DISTINCT t.trip_id)                             AS trips_per_day,
       printf('%02d:%02d', MIN(st.dep_sec) / 3600, (MIN(st.dep_sec) % 3600) / 60) AS first_departure,
       printf('%02d:%02d', MAX(st.arr_sec) / 3600, (MAX(st.arr_sec) % 3600) / 60) AS last_arrival,
       COUNT(DISTINCT st.stop_id)                            AS stops_served
FROM routes r JOIN trips t ON t.route_id = r.route_id
JOIN stop_times st ON st.trip_id = t.trip_id
GROUP BY r.route, r.route_long_name;

-- Schedule adherence per route (arrivals after the first stop), industry on-time window -1 / +5 min
DROP VIEW IF EXISTS v_route_adherence;
CREATE VIEW v_route_adherence AS
SELECT route,
       COUNT(*)                                                   AS arrivals,
       COUNT(DISTINCT trip_id || start_date)                      AS trips,
       ROUND(AVG(delay_min), 2)                                   AS mean_delay_min,
       ROUND(AVG(CASE WHEN delay_min BETWEEN -1 AND 5 THEN 1.0 ELSE 0 END), 3) AS on_time_share,
       ROUND(AVG(CASE WHEN delay_min < -1 THEN 1.0 ELSE 0 END), 3)              AS early_share,
       ROUND(AVG(CASE WHEN delay_min > 5  THEN 1.0 ELSE 0 END), 3)              AS late_share
FROM obs_arrivals WHERE stop_sequence > 1
GROUP BY route;

-- Delay growth along the trip: does lateness/earliness accumulate? (deciles of trip progress)
DROP VIEW IF EXISTS v_delay_by_progress;
CREATE VIEW v_delay_by_progress AS
SELECT route, CAST(MIN(trip_progress * 10, 9) AS INTEGER) * 10 AS progress_pct,
       COUNT(*) AS arrivals, AVG(delay_min) AS mean_delay_min
FROM obs_arrivals WHERE stop_sequence > 1
GROUP BY route, progress_pct;

-- Stops where buses are most often late, with names (structured join to the stop table)
DROP VIEW IF EXISTS v_stop_lateness;
CREATE VIEW v_stop_lateness AS
SELECT s.stop_id, s.stop_name, s.stop_lat, s.stop_lon,
       COUNT(*) AS arrivals, COUNT(DISTINCT a.route) AS routes,
       AVG(a.delay_min) AS mean_delay_min,
       AVG(CASE WHEN a.delay_min > 5 THEN 1.0 ELSE 0 END) AS late_share
FROM obs_arrivals a JOIN stops s ON s.stop_id = a.stop_id
WHERE a.stop_sequence > 1
GROUP BY s.stop_id, s.stop_name, s.stop_lat, s.stop_lon;
