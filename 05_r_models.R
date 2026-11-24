# =============================================================================
# Phase 3 - Step 5: R MODELLING + ALGORITHM COMPARISON + R/SQL BENCHMARK
#   A. Delay regression in R on the SAME train/test split Spark used:
#        lm (OLS)  vs  ranger (random forest)   -> joined with the Spark MLlib metrics
#   B. Clustering in R: Ward hierarchical clustering vs k-means (silhouette),
#      compared with Spark K-means / GMM on the same grid cells
#   C. Statistical test: is the bus slower in the peak? (Wilcoxon rank-sum)
#   D. Benchmark: route x hour aggregation in R dplyr vs SQLite SQL at growing size
# AI-assisted (Claude Code), 2026-11-24
# =============================================================================
source("00_config.R")
suppressPackageStartupMessages({ library(ranger); library(cluster) })
message("== Step 5: R models & comparison ==")
set.seed(2026)

metrics <- function(y, p) c(rmse = sqrt(mean((y - p)^2)), mae = mean(abs(y - p)),
                            r2 = 1 - sum((y - p)^2) / sum((y - mean(y))^2))

# ---- A. Delay regression ------------------------------------------------------------
md <- rd("p3_model_data.csv") |> mutate(route = factor(route))
train <- filter(md, set == "train"); test <- filter(md, set == "test")
test <- filter(test, route %in% unique(train$route)) |> mutate(route = factor(route, levels = levels(train$route)))
fml <- delay_min ~ route + hour + is_peak + is_weekend + direction_id + trip_progress + elapsed_sched_min + stop_sequence

t0 <- Sys.time(); fit_lm <- lm(fml, data = train); t_lm <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
t0 <- Sys.time()
fit_rf <- ranger(fml, data = train, num.trees = 300, mtry = 3, min.node.size = 5, importance = "permutation",
                 num.threads = 1, seed = 2026)
t_rf <- as.numeric(difftime(Sys.time(), t0, units = "secs"))

p_lm <- predict(fit_lm, test); p_rf <- predict(fit_rf, test)$predictions
r_metrics <- bind_rows(
  tibble(model = "Linear regression (R lm)", engine = "R", !!!metrics(test$delay_min, p_lm), train_s = t_lm),
  tibble(model = "Random forest (R ranger)", engine = "R", !!!metrics(test$delay_min, p_rf), train_s = t_rf))
all_metrics <- bind_rows(rd("p3_model_metrics_spark.csv"), r_metrics) |>
  mutate(across(c(rmse, mae, r2), ~ round(.x, 3)), train_s = round(train_s, 2)) |>
  arrange(rmse)
write_csv(all_metrics, file.path(OUT_DIR, "p3_model_comparison.csv"))
print(all_metrics)

write_csv(bind_rows(tibble(model = "Linear regression (R lm)", route = as.character(test$route), delay_min = test$delay_min, prediction = p_lm),
                    tibble(model = "Random forest (R ranger)", route = as.character(test$route), delay_min = test$delay_min, prediction = p_rf)),
          file.path(OUT_DIR, "p3_model_predictions_r.csv"))
write_csv(tibble(feature = names(fit_rf$variable.importance), importance = fit_rf$variable.importance) |>
            arrange(desc(importance)), file.path(OUT_DIR, "p3_feature_importance_r.csv"))

# interpretable lm: per-route intercept shift + effect of trip progress (does delay drift?)
lm_coef <- broom::tidy(fit_lm, conf.int = TRUE)
write_csv(lm_coef, file.path(OUT_DIR, "p3_lm_coefficients.csv"))

# ---- B. Clustering in R (same cells and features as Spark) ---------------------------
cells <- rd("p3_cell_clusters.csv")
FEATS <- c("p50_speed", "p10_speed", "sd_speed", "share_stopped_offstop", "share_dwell_at_stop")
X <- scale(as.matrix(cells[, FEATS])); X[is.na(X)] <- 0
d <- dist(X)
d2 <- d^2                       # Spark's ClusteringEvaluator uses squared Euclidean -> same here, so scores are comparable
hc <- hclust(d, method = "ward.D2")
clus_rows <- list()
for (k in 2:8) {
  km <- kmeans(X, centers = k, nstart = 25, iter.max = 100)
  hk <- cutree(hc, k)
  clus_rows[[length(clus_rows) + 1]] <- tibble(
    k = k,
    `K-means (R)` = mean(silhouette(km$cluster, d2)[, 3]),
    `Hierarchical Ward (R)` = mean(silhouette(hk, d2)[, 3]))
}
r_sil <- bind_rows(clus_rows) |> pivot_longer(-k, names_to = "algorithm", values_to = "silhouette")
sp_sil <- rd("p3_cluster_metrics.csv") |> transmute(k, algorithm = paste(algorithm, "(Spark)"), silhouette)
sil_all <- bind_rows(sp_sil, r_sil)
write_csv(sil_all, file.path(OUT_DIR, "p3_cluster_comparison.csv"))
print(sil_all |> pivot_wider(names_from = algorithm, values_from = silhouette), width = 140)

# agreement between Spark K-means (chosen k) and R Ward with the same k: adjusted Rand index
k_best <- n_distinct(cells$cluster)
ari <- function(a, b) {
  tab <- table(a, b); n <- sum(tab); s <- function(x) sum(choose(x, 2))
  e <- s(rowSums(tab)) * s(colSums(tab)) / choose(n, 2)
  (s(tab) - e) / ((s(rowSums(tab)) + s(colSums(tab))) / 2 - e)
}
agree <- tibble(k = k_best, ari_spark_kmeans_vs_r_ward = ari(cells$cluster, cutree(hc, k_best)))
write_csv(agree, file.path(OUT_DIR, "p3_cluster_agreement.csv")); print(agree)

# ---- C. Peak vs off-peak speed: Wilcoxon rank-sum test -------------------------------
pairs <- read_csv(file.path(OUT_DIR, "p3_pairs_slim.csv.gz"), show_col_types = FALSE, num_threads = 1, progress = FALSE)
mv <- filter(pairs, speed_kmh >= 5) |> mutate(period = ifelse(hour %in% c(7, 8, 17, 18), "Peak", "Off-peak"))
tests <- list()
if (n_distinct(mv$period) == 2) {
  w <- wilcox.test(speed_kmh ~ period, data = mv, conf.int = TRUE)
  tests$peak <- tibble(test = "Moving speed: off-peak vs peak (Wilcoxon)", statistic = unname(w$statistic),
                       p_value = w$p.value, estimate_kmh = unname(w$estimate),
                       median_offpeak = median(mv$speed_kmh[mv$period == "Off-peak"]),
                       median_peak = median(mv$speed_kmh[mv$period == "Peak"]))
}
if (n_distinct(mv$day_type) == 2) {
  w <- wilcox.test(speed_kmh ~ day_type, data = mv, conf.int = TRUE)
  tests$dt <- tibble(test = "Moving speed: weekday vs weekend (Wilcoxon)", statistic = unname(w$statistic),
                     p_value = w$p.value, estimate_kmh = unname(w$estimate),
                     median_offpeak = median(mv$speed_kmh[mv$day_type == "Weekday"]),
                     median_peak = median(mv$speed_kmh[mv$day_type == "Weekend"]))
}
write_csv(bind_rows(tests), file.path(OUT_DIR, "p3_stat_tests.csv")); print(bind_rows(tests))

# ---- D. Benchmark: R dplyr (in memory) vs SQLite SQL ---------------------------------
sizes <- c(1e5, 5e5, 1e6, 2e6, 4e6)
bench <- list()
con <- dbConnect(SQLite(), ":memory:")
for (n in sizes) {
  big <- pairs[rep_len(seq_len(nrow(pairs)), n), c("route", "hour", "day_type", "speed_kmh", "km", "dt_s")]
  t0 <- Sys.time()
  invisible(big |> group_by(route, hour, day_type) |>
              summarise(n = n(), p50 = median(speed_kmh), speed_mean = sum(km) / (sum(dt_s) / 3600),
                        share_stationary = mean(speed_kmh < 5), .groups = "drop"))
  t_dplyr <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
  dbWriteTable(con, "pairs", big, overwrite = TRUE)
  t0 <- Sys.time()
  invisible(dbGetQuery(con, "SELECT route, hour, day_type, COUNT(*) n, SUM(km) / (SUM(dt_s) / 3600.0) speed_mean,
                                    AVG(CASE WHEN speed_kmh < 5 THEN 1.0 ELSE 0 END) share_stationary
                             FROM pairs GROUP BY route, hour, day_type"))
  t_sql <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
  bench[[length(bench) + 1]] <- tibble(engine = c("R dplyr (1 core, in memory)", "SQLite SQL (1 core)"),
                                       rows = n, seconds = c(t_dplyr, t_sql))
  message(sprintf("  rows=%9s dplyr=%.2fs sqlite=%.2fs", format(n, big.mark = ","), t_dplyr, t_sql))
  rm(big); gc(verbose = FALSE)
}
dbDisconnect(con)
write_csv(bind_rows(bench), file.path(OUT_DIR, "p3_scaling_r.csv"))
message("Step 5 done")
