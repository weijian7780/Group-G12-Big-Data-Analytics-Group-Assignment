"""
Shared helpers for the Phase 3 Spark jobs.
AI-assisted (Claude Code), 2026-11-24
"""
import glob
import os
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
GTFS_DIR = DATA / "gtfs_static"
ARCHIVE = DATA / "realtime_archive"
OUT = ROOT / "phase3" / "output"
OUT.mkdir(parents=True, exist_ok=True)

# Johor Bahru bounding box (same as Phase 2 cleaning)
LAT_MIN, LAT_MAX, LON_MIN, LON_MAX = 1.2, 2.2, 103.2, 104.3


def get_spark(app="G12-BAS.MY", cores="local[*]"):
    """Start a local Spark session that also works on Windows laptops.

    Windows notes (both hit on our machines):
      * Java 22 opens a Unix-domain socket for its internal selector; in the default
        temp folder this fails ("Unable to establish loopback connection"), so we point
        jdk.net.unixdomain.tmpdir at a folder inside the project.
      * Hadoop's native IO (winutils.exe / hadoop.dll) is missing, so Spark must not
        list or write local files itself -> see read_jsonl() / to_csv() below.
    """
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    from pyspark.sql import SparkSession

    sock = ROOT / ".spark_tmp"
    sock.mkdir(exist_ok=True)
    spark = (SparkSession.builder.master(cores).appName(app)
             .config("spark.driver.extraJavaOptions", f'-Djdk.net.unixdomain.tmpdir="{sock.as_posix()}"')
             .config("spark.ui.enabled", "false")
             .config("spark.sql.session.timeZone", "Asia/Kuala_Lumpur")
             .config("spark.sql.shuffle.partitions", "16")
             .config("spark.sql.execution.arrow.pyspark.enabled", "true")
             .config("spark.driver.memory", "4g")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def hadoop_io_ok():
    """True when Spark can read local files directly (Linux/macOS, or Windows with HADOOP_HOME)."""
    return os.name != "nt" or bool(os.environ.get("HADOOP_HOME"))


def read_jsonl(spark, pattern):
    """Raw JSON Lines -> one-column Spark DataFrame `value` (the untouched JSON text).

    On Linux/cluster: spark.read.text(pattern) (distributed read).
    On Windows without Hadoop native libs: Python reads the lines, Spark parses them.
    Either way, ALL JSON parsing / flattening happens in Spark (from_json + explode).
    """
    if hadoop_io_ok():
        return spark.read.text(str(pattern))
    rows = []
    for f in sorted(glob.glob(str(pattern))):
        with open(f, encoding="utf-8") as fh:
            rows.extend(line.rstrip("\n") for line in fh if line.strip())
    if not rows:
        raise FileNotFoundError(f"No realtime snapshots found at {pattern}. Run 00_collect_realtime.py first.")
    return spark.createDataFrame(pd.DataFrame({"value": rows}))


def read_gtfs(spark, name):
    """Static GTFS .txt (CSV) -> Spark DataFrame, all columns as strings (cast later in Spark SQL)."""
    path = GTFS_DIR / f"{name}.txt"
    if not path.exists():
        raise FileNotFoundError(f"Missing GTFS file {path}")
    if hadoop_io_ok():
        return spark.read.option("header", True).csv(str(path))
    return spark.createDataFrame(pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8-sig"))


def to_csv(sdf, name):
    """Small aggregated Spark result -> CSV in phase3/output (via pandas, no Hadoop needed)."""
    pdf = sdf.toPandas()
    path = OUT / f"{name}.csv"
    pdf.to_csv(path, index=False)
    print(f"  wrote {path.relative_to(ROOT)}  ({len(pdf):,} rows)")
    return pdf
