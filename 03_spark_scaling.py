"""
Phase 3 - Step 3: SCALABILITY BENCHMARK (Spark, MapReduce-style aggregation)
===========================================================================
Question: does Spark keep the route x hour aggregation fast as the archive grows
from one day towards months of 30-second snapshots?

The clean GPS pairs are replicated to N = 0.1M ... 8M rows (8M is roughly
5-6 weeks of the real feed) and the same Objective-1 query is timed with
  * local[1]  - one core  (like a single-machine script)
  * local[*]  - all cores (data split into partitions, map -> shuffle -> reduce)
The R side of the comparison (dplyr in memory vs SQLite SQL) is in 05_benchmark_r.R.

Run from the project root:   python phase3/03_spark_scaling.py
AI-assisted (Claude Code), 2026-11-24
"""
import math
import time

import pandas as pd
from pyspark.sql import functions as F

from spark_utils import OUT, get_spark

SIZES = [100_000, 500_000, 1_000_000, 2_000_000, 4_000_000, 8_000_000]
REPEATS = 3
base = pd.read_csv(OUT / "p3_pairs_slim.csv.gz")
print(f"base GPS pairs: {len(base):,}")

QUERY = """
  SELECT route, hour, day_type, COUNT(*) AS n,
         percentile_approx(speed_kmh, 0.5) AS p50, SUM(km) / (SUM(dt_s) / 3600.0) AS speed_mean,
         AVG(CASE WHEN speed_kmh < 5 THEN 1.0 ELSE 0 END) AS share_stationary
  FROM pairs GROUP BY route, hour, day_type"""

rows = []
for master in ("local[1]", "local[*]"):
    spark = get_spark(f"G12-03-scaling-{master}", cores=master)
    cores = spark.sparkContext.defaultParallelism
    b = spark.createDataFrame(base).cache()
    for n in SIZES:
        rep = math.ceil(n / len(base))
        df = (b.crossJoin(spark.range(rep).withColumnRenamed("id", "rep"))   # replicate the archive
               .limit(n).repartition(max(cores, 1) * 2).cache())
        df.count()                                                          # materialise before timing
        df.createOrReplaceTempView("pairs")
        times = []
        for _ in range(REPEATS):
            t0 = time.time()
            spark.sql(QUERY).collect()
            times.append(time.time() - t0)
        rows.append(dict(engine=f"Spark {master}", cores=cores, rows=n, seconds=min(times), seconds_median=sorted(times)[1]))
        print(f"  {master:9s} cores={cores:2d} rows={n:>9,}  best {min(times):6.2f}s")
        df.unpersist()
    spark.stop()

res = pd.DataFrame(rows)
res.to_csv(OUT / "p3_scaling_spark.csv", index=False)
print(res.pivot(index="rows", columns="engine", values="seconds").round(2))
print("Step 3 (Spark scaling) done -> phase3/output/p3_scaling_spark.csv")
