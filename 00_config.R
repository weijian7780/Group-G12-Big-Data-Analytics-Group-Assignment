# =============================================================================
# KD44103 Big Data Analytics - Group G12, Topic D (Urban Mobility, BAS.MY Johor Bahru)
# Phase 3: shared R configuration (run R scripts with phase3/ as working directory)
# AI-assisted (Claude Code), 2026-11-24
# =============================================================================

suppressPackageStartupMessages({
  library(readr); library(dplyr); library(tidyr); library(DBI); library(RSQLite)
  library(ggplot2); library(scales)
})

OUT_DIR  <- "output"
FIG_DIR  <- "figures"
SQL_DIR  <- "sql"
GTFS_DIR <- file.path("..", "data", "gtfs_static")
DB_PATH  <- file.path(OUT_DIR, "bas_johor_phase3.sqlite")
for (d in c(OUT_DIR, FIG_DIR)) dir.create(d, showWarnings = FALSE, recursive = TRUE)

need <- c("p3_arrivals.csv", "p3_speed_route_hour.csv", "p3_grid_cells.csv")
miss <- need[!file.exists(file.path(OUT_DIR, need))]
if (length(miss)) stop("Spark outputs missing (", paste(miss, collapse = ", "),
                       "). Run: python 01_spark_process.py  (from phase3/)")

# single-threaded readers: multithreaded vroom crashed R on one team laptop (see Phase 2)
rd <- function(name, ...) read_csv(file.path(OUT_DIR, name), progress = FALSE, show_col_types = FALSE,
                                   num_threads = 1, ...)
read_gtfs <- function(name) read_csv(file.path(GTFS_DIR, paste0(name, ".txt")),
                                     col_types = cols(.default = col_character()),
                                     progress = FALSE, num_threads = 1, na = "")
gtfs_time_to_sec <- function(x) {
  p <- strsplit(x, ":", fixed = TRUE)
  vapply(p, function(v) if (length(v) == 3) sum(as.numeric(v) * c(3600, 60, 1)) else NA_real_, numeric(1))
}

# Okabe-Ito colour-blind safe palette + shared theme
PAL <- c(blue = "#0072B2", orange = "#E69F00", green = "#009E73", red = "#D55E00",
         purple = "#CC79A7", sky = "#56B4E9", yellow = "#F0E442", grey = "#666666")
theme_bda <- function(base = 11) {
  theme_minimal(base_size = base) +
    theme(plot.title = element_text(face = "bold", size = base + 2), plot.title.position = "plot",
          plot.subtitle = element_text(colour = "grey30"),
          plot.caption = element_text(colour = "grey45", size = base - 3, hjust = 0),
          panel.grid.minor = element_blank(), legend.position = "right")
}
save_fig <- function(p, name, w = 9, h = 5.5) {
  f <- file.path(FIG_DIR, paste0(name, ".png"))
  ggsave(f, p, width = w, height = h, dpi = 200, bg = "white")
  message("  saved ", f); invisible(f)
}
vol <- if (file.exists(file.path(OUT_DIR, "p3_volume.csv"))) rd("p3_volume.csv") else NULL
SRC <- if (!is.null(vol)) sprintf("Source: BAS.MY Johor GTFS-Realtime archive, %s to %s (%s snapshots, %s GPS records) + static GTFS",
                                  format(as.POSIXct(vol$first_snapshot), "%d %b %H:%M"),
                                  format(as.POSIXct(vol$last_snapshot), "%d %b %Y %H:%M"),
                                  format(vol$snapshots, big.mark = ","), format(vol$records, big.mark = ",")) else ""
