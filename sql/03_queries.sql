-- =============================================================================
-- Phase 3 - analysis queries; 04_store_sql.R runs each block (split on ';')
-- and saves the result as output/sql_<name>.csv (block name = the "name" tag line).
-- AI-assisted (Claude Code), 2026-11-24
-- =============================================================================

-- name: route_scorecard
-- One row per route: scheduled service + observed reliability (SQL join of timetable and Spark output)
SELECT s.route, s.trips_per_day, s.first_departure, s.last_arrival,
       a.trips AS trips_observed, a.arrivals AS arrivals_observed,
       a.mean_delay_min, a.on_time_share, a.early_share, a.late_share,
       h.obs_headway_p50, h.sched_headway_p50, h.bunching_share, h.long_gap_share, ROUND(h.ewt_min, 2) AS ewt_min,
       sp.speed_obs_kmh, sp.speed_sched_kmh
FROM v_route_service s
LEFT JOIN v_route_adherence a ON a.route = s.route
LEFT JOIN obs_headway_route h ON h.route = s.route
LEFT JOIN (SELECT route, SUM(speed_mean * n_pairs) / SUM(n_pairs) AS speed_obs_kmh,
                  SUM(sched_speed_mean * n_pairs) / SUM(n_pairs) AS speed_sched_kmh
           FROM obs_speed_route_hour GROUP BY route) sp ON sp.route = s.route
ORDER BY s.trips_per_day DESC;

-- name: late_stops_top15
-- Stops with the highest share of late arrivals (min. 20 observed arrivals)
SELECT stop_name, routes, arrivals, ROUND(mean_delay_min, 1) AS mean_delay_min, ROUND(late_share, 3) AS late_share
FROM v_stop_lateness WHERE arrivals >= 20
ORDER BY late_share DESC, mean_delay_min DESC LIMIT 15;

-- name: hotspots_named
-- Congested grid cells (K-means cluster) named after the nearest stop
SELECT label, lat, lon, n_pairs, p50_speed, share_stopped_offstop, nearest_stop,
       ROUND(dist_m) AS nearest_stop_m
FROM (SELECT c.label, c.lat, c.lon, c.n_pairs, ROUND(c.p50_speed, 1) AS p50_speed,
             ROUND(c.share_stopped_offstop, 3) AS share_stopped_offstop, s.stop_name AS nearest_stop,
             111320 * SQRT((s.stop_lat - c.lat) * (s.stop_lat - c.lat) + (s.stop_lon - c.lon) * (s.stop_lon - c.lon)) AS dist_m,
             ROW_NUMBER() OVER (PARTITION BY c.cx, c.cy
                                ORDER BY (s.stop_lat - c.lat) * (s.stop_lat - c.lat) + (s.stop_lon - c.lon) * (s.stop_lon - c.lon)) AS rk
      FROM obs_cell_clusters c CROSS JOIN stops s
      WHERE c.label LIKE 'Congested%')
WHERE rk = 1
ORDER BY share_stopped_offstop DESC, n_pairs DESC LIMIT 20;

-- name: peak_vs_offpeak
-- Objective 1/2: peak (07-09, 17-19) vs off-peak, speed and punctuality
SELECT CASE WHEN hour IN (7, 8, 17, 18) THEN 'Peak' ELSE 'Off-peak' END AS period, day_type,
       COUNT(*) AS arrivals, ROUND(AVG(delay_min), 2) AS mean_delay_min,
       ROUND(AVG(CASE WHEN delay_min BETWEEN -1 AND 5 THEN 1.0 ELSE 0 END), 3) AS on_time_share
FROM obs_arrivals WHERE stop_sequence > 1
GROUP BY period, day_type;

-- name: delay_growth
-- Does delay build up along the route? first vs last 20 % of the trip
SELECT route,
       ROUND(AVG(CASE WHEN progress_pct <= 10 THEN mean_delay_min END), 1) AS delay_start_min,
       ROUND(AVG(CASE WHEN progress_pct >= 80 THEN mean_delay_min END), 1) AS delay_end_min
FROM v_delay_by_progress GROUP BY route ORDER BY delay_end_min;
