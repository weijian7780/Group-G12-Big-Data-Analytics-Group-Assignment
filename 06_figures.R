# =============================================================================
# Phase 3 - Step 6: PUBLICATION-QUALITY FIGURES (ggplot2) -> figures/
#   O1 speed:     fig01, fig02, fig03
#   O2 service:   fig04, fig05, fig06
#   O3 hotspots:  fig07
#   O4 coverage:  fig08
#   Algorithms:   fig09 (regression), fig10 (clustering), fig11 (scaling), fig12 (importance)
# AI-assisted (Claude Code), 2026-11-24
# =============================================================================
source("00_config.R")
suppressPackageStartupMessages({ library(ggrepel); library(patchwork) })
message("== Step 6: figures ==")

spd  <- rd("p3_speed_route_hour.csv")
sph  <- rd("p3_speed_hour.csv")
arr  <- rd("p3_arrivals.csv", col_types = cols(start_date = "c", stop_id = "c", .default = col_guess())) |>
  filter(stop_sequence > 1)
hwp  <- rd("p3_headway_pairs.csv")
hwr  <- rd("p3_headway_route.csv")
cov  <- rd("p3_trip_coverage.csv", col_types = cols(route = "c", day_type = "c", .default = "d")); fp <- rd("p3_footprint.csv")
cells <- rd("p3_cell_clusters.csv")
shapes <- read_gtfs("shapes") |> transmute(shape_id, lat = as.numeric(shape_pt_lat), lon = as.numeric(shape_pt_lon),
                                           seq = as.integer(shape_pt_sequence)) |> arrange(shape_id, seq)
stops <- read_gtfs("stops") |> transmute(stop_id, stop_name, lat = as.numeric(stop_lat), lon = as.numeric(stop_lon))

# ---- fig01: observed vs scheduled speed per route (dumbbell) -------------------------
f1 <- spd |> filter(!is.na(sched_speed_mean)) |>
  group_by(route) |>
  summarise(obs = sum(speed_mean * n_pairs) / sum(n_pairs), sched = sum(sched_speed_mean * n_pairs) / sum(n_pairs),
            n = sum(n_pairs), .groups = "drop") |>
  filter(n >= 50) |>
  mutate(route = reorder(route, obs - sched), faster = obs > sched)
p1 <- ggplot(f1, aes(y = route)) +
  geom_segment(aes(x = sched, xend = obs, yend = route, colour = faster), linewidth = 1.1) +
  geom_point(aes(x = sched), shape = 21, fill = "white", colour = PAL["grey"], size = 3, stroke = 1) +
  geom_point(aes(x = obs, colour = faster), size = 3.2) +
  scale_colour_manual(values = c(`TRUE` = PAL[["green"]], `FALSE` = PAL[["red"]]),
                      labels = c(`TRUE` = "Faster than timetable", `FALSE` = "Slower than timetable"), name = NULL) +
  labs(title = "Fig 1. Observed bus speed vs timetable speed, by route",
       subtitle = "Open circle = speed implied by the timetable; filled dot = speed measured from GPS (moving buses, layovers removed)",
       x = "Average speed (km/h)", y = NULL, caption = SRC) +
  theme_bda() + theme(legend.position = "top")
save_fig(p1, "fig01_observed_vs_scheduled_speed", 9, 6.5)

# ---- fig02: speed by hour of day (P10-P90 band) vs timetable --------------------------
sched_hour <- spd |> filter(!is.na(sched_speed_mean)) |> group_by(hour) |>
  summarise(sched = sum(sched_speed_mean * n_pairs) / sum(n_pairs), .groups = "drop")
f2 <- sph |> filter(n_pairs >= 100)
p2 <- ggplot(f2, aes(hour)) +
  annotate("rect", xmin = c(7, 17) - 0.5, xmax = c(9, 19) - 0.5, ymin = -Inf, ymax = Inf, fill = PAL["red"], alpha = 0.08) +
  geom_ribbon(aes(ymin = p25, ymax = p75, fill = day_type), alpha = 0.25) +
  geom_line(aes(y = speed_mean, colour = day_type), linewidth = 1.1) +
  geom_point(aes(y = speed_mean, colour = day_type), size = 2) +
  geom_line(data = sched_hour, aes(y = sched), linetype = "dashed", colour = PAL["grey"]) +
  scale_colour_manual(values = c(Weekday = PAL[["blue"]], Weekend = PAL[["orange"]]), name = NULL) +
  scale_fill_manual(values = c(Weekday = PAL[["blue"]], Weekend = PAL[["orange"]]), name = NULL) +
  scale_x_continuous(breaks = 0:23, limits = c(0, 23)) +
  labs(title = "Fig 2. Measured bus speed through the day",
       subtitle = "Line = distance-weighted mean speed, band = interquartile range of 30-s GPS segments; dashed = timetable; shaded = peaks",
       x = "Hour of day (MYT)", y = "Speed (km/h)", caption = SRC) +
  theme_bda() + theme(legend.position = "top")
save_fig(p2, "fig02_speed_by_hour")

# ---- fig03: route x hour heat map of (observed - timetable) speed ---------------------
f3 <- spd |> filter(n_pairs >= 20, !is.na(sched_speed_mean)) |> mutate(gap = speed_mean - sched_speed_mean)
lim <- max(abs(quantile(f3$gap, c(.02, .98), na.rm = TRUE)))
p3 <- ggplot(f3, aes(hour, reorder(route, gap), fill = pmax(pmin(gap, lim), -lim))) +
  geom_tile(colour = "white", linewidth = 0.4) +
  scale_fill_distiller(palette = "RdBu", direction = 1, limits = c(-lim, lim), name = "Observed -\ntimetable\n(km/h)") +
  scale_x_continuous(breaks = seq(0, 23, 2), expand = c(0, 0)) +
  labs(title = "Fig 3. Where and when buses run slower than planned",
       subtitle = "Red = slower than the timetable assumes (congestion or optimistic schedule); blue = faster", x = "Hour of day", y = NULL,
       caption = SRC) + theme_bda()
save_fig(p3, "fig03_speed_gap_heatmap", 9, 6)

# ---- fig04: schedule adherence by route ------------------------------------------------
ord <- arr |> group_by(route) |> summarise(m = median(delay_min), n = n()) |> filter(n >= 30)
f4 <- arr |> semi_join(ord, by = "route") |> mutate(route = factor(route, levels = ord$route[order(ord$m)]))
otp <- f4 |> group_by(route) |> summarise(otp = mean(between(delay_min, -1, 5)), n = n())
p4 <- ggplot(f4, aes(delay_min, route)) +
  annotate("rect", xmin = -1, xmax = 5, ymin = -Inf, ymax = Inf, fill = PAL["green"], alpha = 0.15) +
  geom_vline(xintercept = 0, colour = "grey40") +
  geom_boxplot(outlier.shape = NA, fill = PAL["sky"], alpha = 0.5, width = 0.65, coef = 1) +
  geom_text(data = otp, aes(x = Inf, y = route, label = sprintf("%2.0f%% on time  (n=%s)", 100 * otp, comma(n))),
            hjust = 1.02, size = 2.9, colour = "grey25") +
  coord_cartesian(xlim = c(-45, 40)) +
  labs(title = "Fig 4. How early or late buses reach their stops",
       subtitle = "Observed minus scheduled arrival at each stop. Green band = on time (-1 to +5 min). Negative = early",
       x = "Arrival delay (minutes)", y = NULL, caption = SRC) + theme_bda()
save_fig(p4, "fig04_delay_by_route", 9, 6.5)

# ---- fig05: delay drift along the trip -------------------------------------------------
f5 <- arr |> semi_join(ord, by = "route") |>
  mutate(prog = pmin(floor(trip_progress * 10), 9) * 10 + 5) |>
  group_by(route, prog) |> summarise(delay = mean(delay_min), n = n(), .groups = "drop") |> filter(n >= 5)
end <- f5 |> group_by(route) |> slice_max(prog, n = 1)
hi  <- end |> ungroup() |> slice_max(abs(delay), n = 4) |> pull(route)
p5 <- ggplot(f5, aes(prog, delay, group = route)) +
  annotate("rect", xmin = -Inf, xmax = Inf, ymin = -1, ymax = 5, fill = PAL["green"], alpha = 0.12) +
  geom_hline(yintercept = 0, colour = "grey40") +
  geom_line(colour = "grey75", linewidth = 0.6) +
  geom_line(data = filter(f5, route %in% hi), aes(colour = route), linewidth = 1.2) +
  geom_text_repel(data = filter(end, route %in% hi), aes(label = route, colour = route), nudge_x = 4,
                  direction = "y", hjust = 0, size = 3.5, segment.colour = NA) +
  scale_colour_manual(values = unname(PAL[c("red", "blue", "orange", "purple")]), guide = "none") +
  scale_x_continuous(labels = function(x) paste0(x, "%"), limits = c(0, 108)) +
  labs(title = "Fig 5. Lateness and early running build up along the route",
       subtitle = "Mean arrival delay by position along the trip; coloured = routes with the largest end-of-trip deviation",
       x = "Position along trip (share of stops passed)", y = "Mean delay (minutes)", caption = SRC) + theme_bda()
save_fig(p5, "fig05_delay_along_trip")

# ---- fig06: headway regularity - bunching and long gaps + excess wait time ---------------
f6 <- hwr |> filter(n_pairs >= 15) |>
  transmute(route, `Bunched (< 0.5 x planned gap)` = bunching_share, `Long gap (> 1.5 x planned)` = long_gap_share,
            ewt_min, n_pairs) |>
  pivot_longer(c(`Bunched (< 0.5 x planned gap)`, `Long gap (> 1.5 x planned)`), names_to = "type", values_to = "share")
lvl <- f6 |> group_by(route) |> summarise(s = sum(share)) |> arrange(s) |> pull(route)
p6a <- ggplot(f6, aes(share, factor(route, lvl), fill = type)) + geom_col(width = 0.7) +
  scale_fill_manual(values = c(PAL[["red"]], PAL[["orange"]]), name = NULL) +
  scale_x_continuous(labels = percent) +
  labs(x = "Share of consecutive-bus pairs", y = NULL) + theme_bda() + theme(legend.position = "top")
p6b <- ggplot(distinct(f6, route, ewt_min), aes(ewt_min, factor(route, lvl))) +
  geom_vline(xintercept = 0, colour = "grey40") +
  geom_col(aes(fill = ewt_min > 0), width = 0.7, show.legend = FALSE) +
  scale_fill_manual(values = c(`TRUE` = PAL[["red"]], `FALSE` = PAL[["green"]])) +
  labs(x = "Excess wait time (min)", y = NULL) + theme_bda() + theme(axis.text.y = element_blank())
p6 <- (p6a | p6b) + plot_layout(widths = c(2, 1)) +
  plot_annotation(title = "Fig 6. Irregular gaps between buses",
                  subtitle = "Pairs of buses that follow each other in the timetable, both observed at the same stop. EWT = actual minus scheduled average wait",
                  caption = SRC, theme = theme_bda())
save_fig(p6, "fig06_headway_regularity", 10, 6)

# ---- fig07: congestion hotspot map (K-means clusters of 250 m cells) --------------------
cl_cols <- c("Congested (stopped away from stops)" = PAL[["red"]], "Bus-stop dwell" = PAL[["purple"]],
             "Mixed / moderate" = PAL[["orange"]], "Free-flowing" = PAL[["green"]])
cells$label <- factor(cells$label, levels = names(cl_cols))
p7 <- ggplot() +
  geom_path(data = shapes, aes(lon, lat, group = shape_id), colour = "grey80", linewidth = 0.35) +
  geom_tile(data = cells, aes(lon, lat, fill = label), width = 0.25 / 111.3, height = 0.25 / 110.6, alpha = 0.9) +
  geom_point(data = filter(cells, label == names(cl_cols)[1]) |> slice_max(n_pairs * share_stopped_offstop, n = 8),
             aes(lon, lat), shape = 21, size = 4, stroke = 1, colour = "black") +
  scale_fill_manual(values = cl_cols, drop = FALSE, name = "Cell type (K-means)") +
  coord_quickmap(xlim = quantile(cells$lon, c(.01, .99)) + c(-.02, .02),
                 ylim = quantile(cells$lat, c(.01, .99)) + c(-.02, .02)) +
  labs(title = "Fig 7. Congestion hotspots on the BAS.MY Johor Bahru network",
       subtitle = "250 m grid cells clustered on GPS speed profile; circled = cells where buses most often stand still away from a stop",
       x = "Longitude", y = "Latitude", caption = SRC) + theme_bda()
save_fig(p7, "fig07_hotspot_map", 9, 6.4)

# ---- fig08: coverage - scheduled trips seen + GPS footprint ----------------------------
f8 <- fp |> left_join(cov |> group_by(route) |> summarise(share_seen = sum(trips_seen) / sum(trips_expected)), by = "route") |>
  pivot_longer(c(footprint_share, share_seen), names_to = "metric", values_to = "v") |>
  mutate(metric = recode(metric, footprint_share = "Route length with GPS data", share_seen = "Scheduled trips seen in feed"),
         v = replace_na(v, 0))
p8 <- ggplot(f8, aes(v, reorder(route, v), fill = metric)) +
  geom_col(position = position_dodge(width = 0.75), width = 0.7) +
  scale_x_continuous(labels = percent, limits = c(0, 1)) +
  scale_fill_manual(values = c(PAL[["blue"]], PAL[["sky"]]), name = NULL) +
  labs(title = "Fig 8. How much of the timetable can be observed?",
       subtitle = "Trips: share of scheduled trips (in hours the feed was recorded) that appeared in the realtime feed",
       x = NULL, y = NULL, caption = SRC) + theme_bda() + theme(legend.position = "top")
save_fig(p8, "fig08_coverage", 9, 6.5)

# ---- fig09: regression model comparison -------------------------------------------------
mc <- rd("p3_model_comparison.csv") |> mutate(model = reorder(model, -rmse))
p9a <- ggplot(mc, aes(rmse, model, fill = engine)) + geom_col(width = 0.65) +
  geom_text(aes(label = sprintf("%.2f", rmse)), hjust = -0.15, size = 3) +
  scale_fill_manual(values = c(R = PAL[["blue"]], Spark = PAL[["orange"]]), name = NULL) +
  scale_x_continuous(expand = expansion(mult = c(0, .15))) + labs(x = "Test RMSE (min, lower is better)", y = NULL) +
  theme_bda() + theme(legend.position = "top")
p9b <- ggplot(mc, aes(pmax(r2, 0), model, fill = engine)) + geom_col(width = 0.65, show.legend = FALSE) +
  geom_text(aes(label = sprintf("%.2f", r2)), hjust = -0.15, size = 3) +
  scale_fill_manual(values = c(R = PAL[["blue"]], Spark = PAL[["orange"]])) +
  scale_x_continuous(limits = c(0, 1.1), breaks = seq(0, 1, .25)) + labs(x = expression(Test~R^2~"(higher is better)"), y = NULL) +
  theme_bda() + theme(axis.text.y = element_blank())
p9 <- (p9a | p9b) + plot_annotation(title = "Fig 9. Predicting arrival delay: five algorithms on the same held-out trips",
                                    subtitle = "80/20 split by trip (no trip in both sets). Baseline predicts the training mean",
                                    caption = SRC, theme = theme_bda())
save_fig(p9, "fig09_model_comparison", 10, 5)

# ---- fig10: clustering comparison (silhouette vs k) --------------------------------------
cc <- rd("p3_cluster_comparison.csv")
p10 <- ggplot(cc, aes(k, silhouette, colour = algorithm)) + geom_line(linewidth = 1) + geom_point(size = 2.2) +
  scale_colour_manual(values = unname(PAL[c("orange", "red", "blue", "green")]), name = NULL) +
  scale_x_continuous(breaks = 2:8) +
  labs(title = "Fig 10. Clustering grid cells: which algorithm separates them best?",
       subtitle = "Mean silhouette width (higher = tighter, better-separated clusters) on the same standardised cell features",
       x = "Number of clusters k", y = "Silhouette", caption = SRC) + theme_bda() + theme(legend.position = "top")
save_fig(p10, "fig10_cluster_comparison")

# ---- fig11: scaling benchmark ----------------------------------------------------------
sc <- bind_rows(if (file.exists(file.path(OUT_DIR, "p3_scaling_spark.csv"))) rd("p3_scaling_spark.csv") |> select(engine, rows, seconds),
                if (file.exists(file.path(OUT_DIR, "p3_scaling_r.csv"))) rd("p3_scaling_r.csv"))
if (nrow(sc)) {
  sc <- sc |> mutate(engine = sub("local\\[\\*\\]", "all cores", sub("local\\[1\\]", "1 core", engine)))
  p11 <- ggplot(sc, aes(rows, seconds, colour = engine)) + geom_line(linewidth = 1) + geom_point(size = 2.2) +
    scale_x_log10(labels = label_number(scale_cut = cut_short_scale())) + scale_y_log10() +
    scale_colour_manual(values = unname(PAL[c("blue", "sky", "orange", "red")]), name = NULL) +
    labs(title = "Fig 11. Aggregation time as the GPS archive grows",
         subtitle = "Route x hour x day-type speed summary (the Objective 1 query). Log-log axes; ~1M rows is about one week of the feed",
         x = "GPS segments (rows)", y = "Seconds (best of 3)", caption = "Benchmark on one 16-thread laptop; data replicated from the real archive") +
    theme_bda() + theme(legend.position = "top")
  save_fig(p11, "fig11_scaling")
}

# ---- fig12: what drives delay? (random-forest permutation importance, R) ------------------
fi <- rd("p3_feature_importance_r.csv") |>
  mutate(feature = recode(feature, elapsed_sched_min = "Scheduled minutes since trip start", trip_progress = "Position along trip",
                          stop_sequence = "Stop number", route = "Route", hour = "Hour of day", direction_id = "Direction",
                          is_peak = "Peak hour", is_weekend = "Weekend"))
p12 <- ggplot(fi, aes(importance, reorder(feature, importance))) + geom_col(fill = PAL["blue"], width = 0.65) +
  labs(title = "Fig 12. What explains how early or late a bus is?",
       subtitle = "Random forest (R ranger) permutation importance: increase in error when the feature is shuffled",
       x = "Importance (increase in MSE)", y = NULL, caption = SRC) + theme_bda()
save_fig(p12, "fig12_feature_importance", 8, 4.5)
message("Step 6 done -> ", FIG_DIR)
