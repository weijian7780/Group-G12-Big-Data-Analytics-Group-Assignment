"""
Phase 3 - Step 0: REALTIME COLLECTOR (ingestion of semi-structured data)
------------------------------------------------------------------------
Polls the BAS.MY Johor GTFS-Realtime vehicle-position feed from data.gov.my
every POLL_SEC seconds, decodes the Protocol Buffers message into JSON and
appends ONE JSON document per line (JSON Lines) to a file per day:

    data/realtime_archive/date=YYYY-MM-DD/positions.jsonl

  * date=... folders = Hive-style partitions, so Spark can prune by date.
  * Each line keeps the full nested GTFS-RT structure (header + entity[]),
    i.e. the raw "landing zone" stays immutable; Spark flattens it later.
  * Snapshots whose feed timestamp has not changed are skipped (dedup at source).
  * Network / HTTP / decode errors are logged and retried with back-off,
    so one failure never stops the collector.

Run (from the project root):
    python phase3/00_collect_realtime.py                 # runs until Ctrl+C
    python phase3/00_collect_realtime.py --hours 24      # stop after 24 h

Needs:  pip install gtfs-realtime-bindings requests
AI-assisted (Claude Code), 2026-11-24
"""
import argparse
import datetime as dt
import json
import logging
import time
import urllib.request
from pathlib import Path

from google.protobuf.json_format import MessageToDict
from google.transit import gtfs_realtime_pb2

URL = "https://api.data.gov.my/gtfs-realtime/vehicle-position/mybas-johor/"
MYT = dt.timezone(dt.timedelta(hours=8))
ROOT = Path(__file__).resolve().parent.parent
ARCHIVE = ROOT / "data" / "realtime_archive"


def fetch_feed(timeout=20):
    req = urllib.request.Request(URL, headers={"User-Agent": "KD44103-student-collector/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        payload = r.read()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(payload)
    return MessageToDict(feed), len(payload)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--poll-sec", type=float, default=30)
    ap.add_argument("--hours", type=float, default=0, help="0 = run forever")
    args = ap.parse_args()

    ARCHIVE.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler(ARCHIVE / "collector.log", encoding="utf-8"), logging.StreamHandler()])

    stop_at = time.time() + args.hours * 3600 if args.hours else None
    last_ts, n_saved, n_err, backoff = None, 0, 0, args.poll_sec
    logging.info("collector started, polling every %.0fs -> %s", args.poll_sec, ARCHIVE)

    while stop_at is None or time.time() < stop_at:
        t0 = time.time()
        try:
            doc, nbytes = fetch_feed()
            ts = doc.get("header", {}).get("timestamp")
            if ts == last_ts:
                logging.info("feed unchanged (ts=%s) - skipped", ts)
            else:
                day = dt.datetime.fromtimestamp(int(ts), MYT).strftime("%Y-%m-%d")
                part = ARCHIVE / f"date={day}"
                part.mkdir(exist_ok=True)
                doc["_ingested_at"] = dt.datetime.now(MYT).isoformat(timespec="seconds")
                with open(part / "positions.jsonl", "a", encoding="utf-8") as f:
                    f.write(json.dumps(doc, separators=(",", ":")) + "\n")
                last_ts, n_saved = ts, n_saved + 1
                logging.info("saved snapshot #%d ts=%s vehicles=%d (%d bytes pb)",
                             n_saved, ts, len(doc.get("entity", [])), nbytes)
            backoff = args.poll_sec
        except Exception as e:  # keep running on any network / decode error
            n_err += 1
            backoff = min(backoff * 2, 600)
            logging.warning("poll failed (%s: %s) - retry in %.0fs [errors=%d]",
                            type(e).__name__, e, backoff, n_err)
            time.sleep(backoff)
            continue
        time.sleep(max(0, args.poll_sec - (time.time() - t0)))

    logging.info("collector stopped: %d snapshots saved, %d errors", n_saved, n_err)


if __name__ == "__main__":
    main()
