# =============================================================================
# Phase 3 - run the whole analysis pipeline (after the collector has gathered data)
#   cd phase3 ; Rscript run_phase3.R            # everything
#   Rscript run_phase3.R --skip-benchmark        # skip the slow scaling benchmark
# Python/Spark steps are called through system2(); R steps are sourced.
# AI-assisted (Claude Code), 2026-11-24
# =============================================================================
args <- commandArgs(trailingOnly = TRUE)
skip_bench <- "--skip-benchmark" %in% args
PY <- Sys.getenv("PYTHON", if (.Platform$OS.type == "windows") "python" else "python3")

run_py <- function(script) {
  message("\n>>> ", script, "  (", PY, ")")
  t0 <- Sys.time()
  status <- system2(PY, c("-u", shQuote(script)))
  if (!identical(status, 0L)) stop("Pipeline failed in ", script, " (exit ", status, ")", call. = FALSE)
  message(sprintf("    %s finished in %.0f s", script, as.numeric(difftime(Sys.time(), t0, units = "secs"))))
}
run_r <- function(script) {
  message("\n>>> ", script)
  tryCatch(source(script, local = new.env()),
           error = function(e) stop("Pipeline failed in ", script, ": ", conditionMessage(e), call. = FALSE))
}

t_all <- Sys.time()
run_py("01_spark_process.py")                       # Spark: flatten, clean, speeds, arrivals, headways, grid
run_py("02_spark_ml.py")                            # Spark MLlib: regression + clustering
if (!skip_bench) run_py("03_spark_scaling.py")      # Spark: 1 core vs all cores
run_r("04_store_sql.R")                             # SQLite schema, views, report queries
run_r("05_r_models.R")                              # R: lm / ranger / clustering / tests / dplyr vs SQL
run_r("06_figures.R")                               # ggplot2 figures

if (requireNamespace("rmarkdown", quietly = TRUE)) {
  if (!rmarkdown::pandoc_available() && dir.exists("C:/Program Files/RStudio/resources/app/bin/quarto/bin/tools"))
    Sys.setenv(RSTUDIO_PANDOC = "C:/Program Files/RStudio/resources/app/bin/quarto/bin/tools")
  if (rmarkdown::pandoc_available()) {
    rmarkdown::render("Phase3_Dashboard.Rmd", quiet = TRUE)
    message("Dashboard: phase3/Phase3_Dashboard.html")
    if (file.exists("Group_G12_FinalReport.Rmd")) {
      rmarkdown::render("Group_G12_FinalReport.Rmd", quiet = TRUE)
      message("Report:    phase3/Group_G12_FinalReport.docx  (open in Word -> Save as PDF)")
    }
  } else message("pandoc not found - open the .Rmd files in RStudio and click Knit")
}
if (file.exists("07_make_slides.py")) run_py("07_make_slides.py")  # presentation deck from the same numbers
message(sprintf("\nPhase 3 pipeline finished in %.1f min", as.numeric(difftime(Sys.time(), t_all, units = "mins"))))
