# Observed vs scheduled bus service in Johor Bahru (BAS.MY)

**KD44103 Big Data Analytics, Group G12, Topic D: Urban Mobility & Transport**

The research question is how real bus movements (GPS from the GTFS-Realtime feed) compare with the published BAS.MY timetable (static GTFS). The pipeline measures four things:

1. **Speed** by route and hour (observed vs timetable)
2. **Punctuality and service gaps**: delay at stops, bunching, long gaps, excess wait time
3. **Congestion hotspots**: 250 m grid cells where buses stand still away from stops
4. **Coverage**: how much of the timetable actually appears in the feed

## Pipeline

```
 data.gov.my GTFS-Realtime API ──(Python, every 30 s)──► data/realtime_archive/date=YYYY-MM-DD/positions.jsonl   [semi-structured]
 data.gov.my static GTFS (13 CSV tables) ─────────────► data/gtfs_static/                                       [structured]
                                   │
                    Apache Spark (PySpark + Spark SQL)  ── flatten JSON, clean, GPS speeds, arrival inference,
                                   │                       headways, 250 m grid, MLlib models, scaling test
                                   ▼
                 SQLite (SQL schema, views, queries)  ◄──► R (lm, ranger, clustering, tests, ggplot2)
                                   ▼
                   phase3/figures/*.png  +  Phase3_Dashboard.html  +  technical report
```

| Requirement (brief) | Where it is in the code |
|---|---|
| **R** | `phase2/*.R` (ingest, clean, EDA); `phase3/04_store_sql.R`, `05_r_models.R`, `06_figures.R`, `Phase3_Dashboard.Rmd` |
| **SQL** | SQLite: `phase2/01_ingest_store.R` (raw tables, JSON1 query); `phase3/sql/01_schema.sql` (keys), `02_views.sql`, `03_queries.sql`. **Spark SQL**: every step of `phase3/01_spark_process.py` |
| **Apache Spark** | `phase3/01_spark_process.py` (processing), `02_spark_ml.py` (MLlib), `03_spark_scaling.py` (1 core vs all cores) |
| Structured data | static GTFS, 602K rows in 13 related tables |
| Semi-structured data | GTFS-Realtime vehicle positions (protobuf → nested JSON Lines) |
| Algorithm comparison | delay regression: baseline, LR (R + Spark), RF (R + Spark), GBT; clustering: K-means (R + Spark), GMM, Ward |

## Folder layout

```
G12_BDA_Project/
├── data/
│   ├── gtfs_static/                      latest static GTFS (matches today's realtime trip_ids)
│   ├── gtfs_static_2026-10_phase2/       GTFS copy used in Phase 2 (kept so Phase 2 reproduces)
│   ├── mybas_johor_sample_2026-09-30.json  single snapshot used in Phase 2
│   └── realtime_archive/date=*/positions.jsonl   archive written by the collector
├── phase2/      Phase 2 R pipeline (ingest → SQLite → clean → EDA), unchanged except data paths
└── phase3/
    ├── 00_collect_realtime.py   realtime collector (run for >= 7 days)
    ├── spark_utils.py           Spark session + Windows workarounds
    ├── 01_spark_process.py      Spark processing (Objectives 1–4)
    ├── 02_spark_ml.py           Spark MLlib models
    ├── 03_spark_scaling.py      scalability benchmark
    ├── 00_config.R  04_store_sql.R  05_r_models.R  06_figures.R
    ├── sql/                     schema, views, report queries
    ├── run_phase3.R             runs steps 01–06 + knits the dashboard
    ├── Phase3_Dashboard.Rmd     results dashboard (HTML)
    ├── output/                  CSV results, SQLite DB, Parquet
    └── figures/                 fig01–fig12 (PNG, 200 dpi)
```

## Setup

* **Python 3.10+**: `pip install pyspark==4.2.0 pandas pyarrow psutil gtfs-realtime-bindings`
* **Java 17/21/22** (needed by Spark)
* **R 4.3+**: `install.packages(c("readr","dplyr","tidyr","DBI","RSQLite","ggplot2","scales","ranger","cluster","broom","ggrepel","patchwork","rmarkdown","knitr","jsonlite","lubridate"))`
* pandoc for knitting (bundled with RStudio)

## Run

```powershell
# 1) collect realtime data: leave running for >= 7 days (one snapshot every 30 s)
python phase3/00_collect_realtime.py

# 2) run the whole analysis (Spark → SQL → R → figures → dashboard)
cd phase3
Rscript run_phase3.R                 # add --skip-benchmark to skip the 5-minute scaling test

# Phase 2 still runs on its own
cd ../phase2 ; Rscript run_all.R
```

Each Python step can also be run on its own: `python phase3/01_spark_process.py`.

## Windows notes (we hit both of these)

* **Java 22 "Unable to establish loopback connection"**: `spark_utils.get_spark()` sets `-Djdk.net.unixdomain.tmpdir` to `./.spark_tmp`.
* **No `winutils.exe`/`hadoop.dll`**: Spark cannot list or write local files. On Windows, Python reads the raw lines and Spark does all the parsing, joins and aggregation. On Linux, or with `HADOOP_HOME` set, `spark.read.text()` reads the archive directly.
* R reads CSVs with `num_threads = 1` (a multithreaded reader crashed R 4.6.1 on one laptop).

## Error handling

* The collector skips unchanged feed versions, logs every error and retries with exponential back-off (max 10 min).
* Every step checks that its inputs exist and stops with a clear message naming the step that failed.
* Data-quality counts (duplicates, out-of-region GPS, unknown trip IDs, implausible jumps over 90 km/h) are written to `output/p3_quality_log.csv`. The SQLite load runs `PRAGMA foreign_key_check`.

*AI-assisted (Claude Code) code is labelled in each file header, as the brief requires.*
