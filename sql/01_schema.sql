-- =============================================================================
-- Phase 3 - SQL storage layer (SQLite)
-- Relational schema for the static timetable (structured) + the Spark outputs
-- built from the realtime archive (semi-structured -> tabular).
-- Keys make the joins explicit: route -> trip -> stop_time -> stop.
-- AI-assisted (Claude Code), 2026-11-24
-- =============================================================================
PRAGMA foreign_keys = ON;

DROP TABLE IF EXISTS routes;
CREATE TABLE routes (
  route_id         TEXT PRIMARY KEY,
  route            TEXT NOT NULL,           -- short name e.g. J10
  route_long_name  TEXT
);

DROP TABLE IF EXISTS stops;
CREATE TABLE stops (
  stop_id    TEXT PRIMARY KEY,
  stop_code  TEXT,
  stop_name  TEXT NOT NULL,
  stop_lat   REAL NOT NULL CHECK (stop_lat BETWEEN 1.2 AND 2.2),
  stop_lon   REAL NOT NULL CHECK (stop_lon BETWEEN 103.2 AND 104.3)
);

DROP TABLE IF EXISTS trips;
CREATE TABLE trips (
  trip_id        TEXT PRIMARY KEY,
  route_id       TEXT NOT NULL REFERENCES routes(route_id),
  service_id     TEXT,
  direction_id   INTEGER CHECK (direction_id IN (0, 1)),
  shape_id       TEXT,
  trip_headsign  TEXT
);

DROP TABLE IF EXISTS stop_times;
CREATE TABLE stop_times (
  trip_id        TEXT NOT NULL REFERENCES trips(trip_id),
  stop_sequence  INTEGER NOT NULL,
  stop_id        TEXT NOT NULL REFERENCES stops(stop_id),
  arr_sec        INTEGER NOT NULL,          -- seconds after service-day midnight (can exceed 86400)
  dep_sec        INTEGER NOT NULL,
  PRIMARY KEY (trip_id, stop_sequence)
);
CREATE INDEX ix_stop_times_stop ON stop_times(stop_id, arr_sec);

-- Observed arrivals inferred by Spark (01_spark_process.py)
DROP TABLE IF EXISTS obs_arrivals;
CREATE TABLE obs_arrivals (
  trip_id        TEXT NOT NULL REFERENCES trips(trip_id),
  start_date     TEXT NOT NULL,             -- service date yyyymmdd
  stop_sequence  INTEGER NOT NULL,
  vehicle_id     TEXT,
  route          TEXT,
  route_id       TEXT REFERENCES routes(route_id),
  direction_id   INTEGER,
  day_type       TEXT,
  stop_id        TEXT REFERENCES stops(stop_id),
  sched_sec      REAL,
  obs_sec        REAL,
  delay_min      REAL,
  hour           INTEGER,
  trip_progress  REAL,
  n_stops        INTEGER,
  dow            TEXT,
  PRIMARY KEY (trip_id, start_date, stop_sequence)
);
CREATE INDEX ix_obs_arr_route ON obs_arrivals(route, hour);
