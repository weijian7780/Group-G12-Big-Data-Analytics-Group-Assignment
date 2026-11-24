# =============================================================================
# Phase 3 - Step 4: SQL STORAGE + ANALYTICAL QUERIES (SQLite via DBI)
#   * creates a keyed relational schema (sql/01_schema.sql)
#   * loads the latest static GTFS (structured) + Spark outputs (from the JSON archive)
#   * builds analytical views (sql/02_views.sql) and runs the report queries
#     (sql/03_queries.sql) -> output/sql_*.csv
# AI-assisted (Claude Code), 2026-11-24
# =============================================================================
source("00_config.R")
message("== Step 4: SQL storage & queries ==")

run_sql_file <- function(con, path) {
  txt <- paste(readLines(path, warn = FALSE), collapse = "\n")
  txt <- gsub("--[^\n]*", "", txt)                              # strip comments
  stmts <- trimws(strsplit(txt, ";", fixed = TRUE)[[1]])
  for (s in stmts[nzchar(stmts)]) dbExecute(con, s)
  invisible(length(stmts))
}

if (file.exists(DB_PATH)) file.remove(DB_PATH)
con <- dbConnect(SQLite(), DB_PATH)

run_sql_file(con, file.path(SQL_DIR, "01_schema.sql"))

# ---- structured: latest static GTFS, typed, into the keyed tables ---------------------
routes <- read_gtfs("routes") |> transmute(route_id, route = route_short_name,
                                           route_long_name = trimws(gsub("\\s+", " ", route_long_name)))
stops  <- read_gtfs("stops") |> transmute(stop_id, stop_code, stop_name = trimws(stop_name),
                                          stop_lat = as.numeric(stop_lat), stop_lon = as.numeric(stop_lon))
trips  <- read_gtfs("trips") |> transmute(trip_id, route_id, service_id, direction_id = as.integer(direction_id),
                                          shape_id, trip_headsign)
st     <- read_gtfs("stop_times") |> transmute(trip_id, stop_sequence = as.integer(stop_sequence), stop_id,
                                               arr_sec = gtfs_time_to_sec(arrival_time),
                                               dep_sec = gtfs_time_to_sec(departure_time))
dbWithTransaction(con, {
  dbAppendTable(con, "routes", routes); dbAppendTable(con, "stops", stops)
  dbAppendTable(con, "trips", trips);   dbAppendTable(con, "stop_times", st)
})

# ---- Spark outputs (semi-structured feed after flattening) ---------------------------
arr <- rd("p3_arrivals.csv", col_types = cols(start_date = "c", stop_id = "c", trip_id = "c", .default = col_guess()))
dbAppendTable(con, "obs_arrivals", as.data.frame(arr[, dbListFields(con, "obs_arrivals")]))
for (t in c("speed_route_hour", "speed_hour", "headway_route", "headway_hour", "adherence_route_hour",
            "trip_coverage", "footprint", "grid_cells", "quality_log")) {
  f <- file.path(OUT_DIR, paste0("p3_", t, ".csv"))
  if (file.exists(f)) dbWriteTable(con, paste0("obs_", t), as.data.frame(rd(basename(f))), overwrite = TRUE)
}
if (file.exists(file.path(OUT_DIR, "p3_cell_clusters.csv")))
  dbWriteTable(con, "obs_cell_clusters", as.data.frame(rd("p3_cell_clusters.csv")), overwrite = TRUE)

# referential-integrity check: every observed trip/stop must exist in the timetable
fk <- dbGetQuery(con, "PRAGMA foreign_key_check")
message(sprintf("  foreign-key violations: %d", nrow(fk)))
if (nrow(fk) > 0) warning("Foreign-key violations found - see PRAGMA foreign_key_check")

run_sql_file(con, file.path(SQL_DIR, "02_views.sql"))

# ---- report queries ---------------------------------------------------------------
qtxt <- paste(readLines(file.path(SQL_DIR, "03_queries.sql"), warn = FALSE), collapse = "\n")
blocks <- strsplit(qtxt, "-- name:", fixed = TRUE)[[1]][-1]
for (b in blocks) {
  name <- trimws(sub("\n.*", "", b))
  q <- sub(";\\s*$", "", trimws(gsub("--[^\n]*", "", sub("^[^\n]*\n", "", b))))
  res <- tryCatch(dbGetQuery(con, q), error = function(e) { warning(name, ": ", conditionMessage(e)); NULL })
  if (!is.null(res)) {
    write_csv(res, file.path(OUT_DIR, paste0("sql_", name, ".csv")))
    message(sprintf("  sql_%s.csv  (%d rows)", name, nrow(res)))
  }
}
print(dbGetQuery(con, "SELECT name, type FROM sqlite_master WHERE type IN ('table','view') ORDER BY type, name"))
dbDisconnect(con)
message("Step 4 done -> ", DB_PATH)
