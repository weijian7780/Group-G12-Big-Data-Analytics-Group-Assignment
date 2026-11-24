"""
Phase 3 - Step 2: SPARK MLlib MODELLING + ALGORITHM COMPARISON
==============================================================
A. Regression - predict how late/early a bus is at a stop (delay_min) from
   route, direction, hour, day type and how far along the trip the stop is.
     Linear Regression  vs  Random Forest  vs  Gradient-Boosted Trees
   Split by TRIP (80/20, hash of trip+date) so stops of one trip never appear in
   both train and test (avoids leakage). Metrics: RMSE, MAE, R^2, train time.
   The same split is exported so R (lm, ranger) is evaluated on identical rows.

B. Clustering - group 250 m grid cells by movement profile to find congestion hotspots.
     K-means  vs  Gaussian Mixture Model (GMM), k = 2..8, scored by silhouette.
   (Bisecting K-means was tried first but collapsed to one cluster on this data in Spark 4.2.)

Run from the project root:   python phase3/02_spark_ml.py
AI-assisted (Claude Code), 2026-11-24
"""
import time

import pandas as pd
from pyspark.ml import Pipeline
from pyspark.ml.clustering import GaussianMixture, KMeans
from pyspark.ml.evaluation import ClusteringEvaluator, RegressionEvaluator
from pyspark.ml.feature import OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression, RandomForestRegressor
from pyspark.sql import functions as F

from spark_utils import OUT, get_spark, read_gtfs

SEED = 2026
spark = get_spark("G12-02-ml")

# ---------------------------------------------------------------------------
# A. Delay regression
# ---------------------------------------------------------------------------
arr = spark.createDataFrame(pd.read_csv(OUT / "p3_arrivals.csv", dtype={"start_date": str, "stop_id": str}))
st = read_gtfs(spark, "stop_times")
trip_start = (st.groupBy("trip_id")
                .agg(F.min(F.expr("CAST(split(departure_time, ':')[0] AS INT)*3600 + "
                                  "CAST(split(departure_time, ':')[1] AS INT)*60")).alias("trip_start_sec")))

data = (arr.join(trip_start, "trip_id")
           .where("stop_sequence > 1")
           .withColumn("elapsed_sched_min", (F.col("sched_sec") - F.col("trip_start_sec")) / 60.0)
           .withColumn("is_weekend", (F.col("day_type") == "Weekend").cast("double"))
           .withColumn("is_peak", F.col("hour").isin(7, 8, 17, 18).cast("double"))
           .withColumn("direction_id", F.col("direction_id").cast("double"))
           .withColumn("hour", F.col("hour").cast("double"))
           # deterministic trip-level split: every stop of a trip goes to the same side
           .withColumn("set", F.when(F.abs(F.hash("trip_id", "start_date")) % 100 < 80, "train").otherwise("test"))
           .cache())
NUM = ["hour", "is_peak", "is_weekend", "direction_id", "trip_progress", "elapsed_sched_min", "stop_sequence"]
data.select("route", "set", "delay_min", *NUM).toPandas().to_csv(OUT / "p3_model_data.csv", index=False)

train, test = data.where("set = 'train'"), data.where("set = 'test'")
n_train, n_test = train.count(), test.count()
print(f"delay model: {n_train:,} train / {n_test:,} test arrivals")

idx = StringIndexer(inputCol="route", outputCol="route_idx", handleInvalid="keep")
ohe = OneHotEncoder(inputCols=["route_idx"], outputCols=["route_ohe"])
vec_lin = VectorAssembler(inputCols=NUM + ["route_ohe"], outputCol="features")   # linear model needs dummies
vec_tree = VectorAssembler(inputCols=NUM + ["route_idx"], outputCol="features")  # trees split on the index

models = {
    "Linear regression (Spark MLlib)": Pipeline(stages=[idx, ohe, vec_lin,
        LinearRegression(labelCol="delay_min", regParam=0.01)]),
    "Random forest (Spark MLlib)": Pipeline(stages=[idx, vec_tree,
        RandomForestRegressor(labelCol="delay_min", numTrees=200, maxDepth=8, maxBins=32, seed=SEED)]),
    "Gradient-boosted trees (Spark MLlib)": Pipeline(stages=[idx, vec_tree,
        GBTRegressor(labelCol="delay_min", maxIter=60, maxDepth=5, stepSize=0.1, maxBins=32, seed=SEED)]),
}
ev = {m: RegressionEvaluator(labelCol="delay_min", predictionCol="prediction", metricName=m) for m in ("rmse", "mae", "r2")}

mean_train = train.agg(F.avg("delay_min")).first()[0]
base = test.withColumn("prediction", F.lit(mean_train))
rows = [dict(model="Baseline: training mean", engine="Spark", rmse=ev["rmse"].evaluate(base),
             mae=ev["mae"].evaluate(base), r2=ev["r2"].evaluate(base), train_s=0.0)]
preds, importances = [], []
for name, pipe in models.items():
    t0 = time.time()
    fit = pipe.fit(train)
    secs = time.time() - t0
    p = fit.transform(test).cache()
    rows.append(dict(model=name, engine="Spark", rmse=ev["rmse"].evaluate(p), mae=ev["mae"].evaluate(p),
                     r2=ev["r2"].evaluate(p), train_s=secs))
    preds.append(p.select(F.lit(name).alias("model"), "route", "delay_min", "prediction").toPandas())
    last = fit.stages[-1]
    if hasattr(last, "featureImportances"):
        imp = last.featureImportances.toArray()
        importances += [dict(model=name, feature=f, importance=float(v))
                        for f, v in zip(NUM + ["route"], imp)]
    print(f"  {name:40s} RMSE={rows[-1]['rmse']:.2f}  MAE={rows[-1]['mae']:.2f}  R2={rows[-1]['r2']:.3f}  ({secs:.1f}s)")

pd.DataFrame(rows).to_csv(OUT / "p3_model_metrics_spark.csv", index=False)
pd.concat(preds).to_csv(OUT / "p3_model_predictions_spark.csv", index=False)
pd.DataFrame(importances).to_csv(OUT / "p3_feature_importance.csv", index=False)

# ---------------------------------------------------------------------------
# B. Clustering of 250 m grid cells (congestion hotspots)
# ---------------------------------------------------------------------------
cells_pd = pd.read_csv(OUT / "p3_grid_cells.csv")
MIN_PAIRS = 30 if (cells_pd.n_pairs >= 30).sum() >= 100 else 5   # need enough observations per cell
cells_pd = cells_pd[cells_pd.n_pairs >= MIN_PAIRS].fillna({"sd_speed": 0})
FEATS = ["p50_speed", "p10_speed", "sd_speed", "share_stopped_offstop", "share_dwell_at_stop"]
cells = spark.createDataFrame(cells_pd)
cells = VectorAssembler(inputCols=FEATS, outputCol="raw").transform(cells)
cells = StandardScaler(inputCol="raw", outputCol="features", withMean=True, withStd=True).fit(cells).transform(cells).cache()
print(f"clustering {cells.count():,} grid cells with >= {MIN_PAIRS} GPS pairs")

sil = ClusteringEvaluator(featuresCol="features", metricName="silhouette", distanceMeasure="squaredEuclidean")
krows, fitted = [], {}
for k in range(2, 9):
    for name, algo in (("K-means", KMeans(k=k, seed=SEED)), ("Gaussian mixture", GaussianMixture(k=k, seed=SEED))):
        t0 = time.time()
        m = algo.fit(cells)
        p = m.transform(cells)
        if p.select("prediction").distinct().count() < 2:      # degenerate fit (too few distinct cells)
            continue
        krows.append(dict(algorithm=name, k=k, silhouette=sil.evaluate(p),
                          wssse=getattr(m.summary, "trainingCost", float("nan")), train_s=time.time() - t0))
        fitted[(name, k)] = p
kdf = pd.DataFrame(krows)
kdf.to_csv(OUT / "p3_cluster_metrics.csv", index=False)
print(kdf.pivot(index="k", columns="algorithm", values="silhouette").round(3))

# final model: K-means with the best silhouette among k = 3..6 (k = 2 just splits fast vs slow)
best = kdf[(kdf.algorithm == "K-means") & kdf.k.between(3, 6)].sort_values("silhouette").iloc[-1]
k_best = int(best.k)
out = fitted[("K-means", k_best)].select("cx", "cy", "lat", "lon", "n_pairs", *FEATS, "prediction").toPandas()
# label clusters from slowest to fastest median speed
order = out.groupby("prediction").p50_speed.median().sort_values().index.tolist()
out["cluster"] = out.prediction.map({c: i + 1 for i, c in enumerate(order)})
prof = out.groupby("cluster")[FEATS + ["n_pairs"]].median().assign(cells=out.groupby("cluster").size())
# data-driven names: dwell at stops is normal; stopping AWAY from stops is the congestion signal
names = {c: "Mixed / moderate" for c in prof.index}
names[prof.share_dwell_at_stop.idxmax()] = "Bus-stop dwell"
names[prof.share_stopped_offstop.idxmax()] = "Congested (stopped away from stops)"
names[prof.p50_speed.idxmax()] = "Free-flowing"
prof["label"] = prof.index.map(names)
out["label"] = out.cluster.map(prof.label)
out.drop(columns="prediction").to_csv(OUT / "p3_cell_clusters.csv", index=False)
prof.reset_index().to_csv(OUT / "p3_cluster_profile.csv", index=False)
print(f"chosen k = {k_best} (silhouette {best.silhouette:.3f})")
print(prof.round(2).to_string())

spark.stop()
print("Step 2 (Spark MLlib) done -> phase3/output/")
