# Databricks notebook source
# MAGIC %md
# MAGIC # Citi Bike trip data download
# MAGIC
# MAGIC Downloads the monthly Citi Bike trip archives into a Unity Catalog volume and extracts the CSVs.
# MAGIC
# MAGIC * **Idempotent.** A month is downloaded only when its archive is missing or is not a valid ZIP, and a CSV is
# MAGIC   extracted only when it is missing or has the wrong size, so a scheduled rerun costs almost nothing.
# MAGIC * **Discovers new months.** With `discover_new_months = true`, it checks the last few calendar months and also
# MAGIC   downloads any newly published archive, so the scheduled job picks up new data on its own.
# MAGIC * Files are downloaded to the driver's local disk first, checked, then copied to the volume. This avoids leaving
# MAGIC   half-written files in the volume if a download fails partway through.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "citibike", "Schema")
dbutils.widgets.text("volume", "raw", "Volume")
dbutils.widgets.text("extra_months", "", "Extra months (YYYYMM, comma-separated)")
dbutils.widgets.dropdown("discover_new_months", "true", ["true", "false"], "Discover new months")
dbutils.widgets.text("lookback_months", "3", "Months to check when discovering")
dbutils.widgets.text("max_parallel", "3", "Parallel downloads")

# COMMAND ----------

import os
import shutil
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zipfile import ZipFile, is_zipfile

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
VOLUME = dbutils.widgets.get("volume").strip()
DISCOVER_NEW_MONTHS = dbutils.widgets.get("discover_new_months") == "true"
LOOKBACK_MONTHS = int(dbutils.widgets.get("lookback_months") or 0)
MAX_PARALLEL = max(1, int(dbutils.widgets.get("max_parallel") or 1))

# Monthly Citi Bike trip archives used by the analysis.
TRIP_MONTHS = ("202601", "202608", "202609")
# Extra months required by the project proposal, downloaded once and not read by the loaders:
# May to September 2024 (G shutdown validation: June 28 to September 3, 2024, with May as the
# pre period) and February to July 2026 (normal week baseline and the earlier 2026 closure weekends).
TRIP_MONTHS_EXTENDED = (
    "202405", "202406", "202407", "202408", "202409",
    "202602", "202603", "202604", "202605", "202606", "202607",
)
TRIP_ARCHIVE_URL = "https://s3.amazonaws.com/tripdata/{month}-citibike-tripdata.zip"
HEADERS = {"User-Agent": "Mozilla/5.0"}

# Create the schema and volume if this is the first run.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.`{VOLUME}`")

DATA_DIR = Path(f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/datasource")
DATA_DIR.mkdir(parents=True, exist_ok=True)
# Tables generated later (cleaned, aggregated, ready for modeling) are kept apart from the raw source files.
PROCESSED_DIR = DATA_DIR / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

print("Volume path:", DATA_DIR)

# COMMAND ----------

def previous_months(count, today=None):
    """Return the last `count` calendar months before the current one, as YYYYMM strings."""
    today = today or date.today()
    year, month = today.year, today.month
    months = []
    for _ in range(count):
        month -= 1
        if month == 0:
            year, month = year - 1, 12
        months.append(f"{year}{month:02d}")
    return months


def is_published(month):
    """True when the archive for `month` exists on the Citi Bike bucket."""
    request = Request(TRIP_ARCHIVE_URL.format(month=month), headers=HEADERS, method="HEAD")
    try:
        with urlopen(request, timeout=60) as response:
            return response.status == 200
    except HTTPError as error:
        if error.code in (403, 404):  # S3 answers 403 for a missing key in a public bucket
            return False
        raise


def download_with_retry(url, destination, attempts=3):
    """Stream `url` to `destination`, retrying with backoff on network errors."""
    for attempt in range(1, attempts + 1):
        try:
            request = Request(url, headers=HEADERS)
            with urlopen(request, timeout=300) as response, open(destination, "wb") as handle:
                shutil.copyfileobj(response, handle, length=8 * 1024 * 1024)
            return
        except (URLError, TimeoutError, ConnectionError) as error:
            if attempt == attempts:
                raise
            wait = 30 * attempt
            print(f"  retry {attempt}/{attempts - 1} for {url} in {wait}s ({error})", flush=True)
            time.sleep(wait)


def process_month(month):
    """Download (if needed) and extract one month. Returns (month, csv_count, downloaded)."""
    archive = DATA_DIR / f"{month}-citibike-tripdata.zip"
    month_dir = DATA_DIR / f"{month}-citibike-tripdata"
    downloaded = False

    with tempfile.TemporaryDirectory() as tmp:
        source = archive
        if not archive.exists() or not is_zipfile(archive):
            print("Downloading", archive.name, flush=True)
            local = Path(tmp) / archive.name
            download_with_retry(TRIP_ARCHIVE_URL.format(month=month), local)
            if not is_zipfile(local):
                raise ValueError(f"Downloaded file is not a valid ZIP: {archive.name}")
            # A copy that stops partway leaves a file that fails is_zipfile, so the next run downloads it again.
            shutil.copyfile(local, archive)
            source = local  # extract from fast local disk rather than reading back from the volume
            downloaded = True

        # Volumes accept sequential writes, so each member is streamed straight to its target file.
        with ZipFile(source) as bundle:
            for member in bundle.infolist():
                if member.is_dir() or member.filename.startswith("__MACOSX/"):
                    continue
                target = month_dir / member.filename
                if target.exists() and target.stat().st_size == member.file_size:
                    continue
                target.parent.mkdir(parents=True, exist_ok=True)
                with bundle.open(member) as src, open(target, "wb") as dst:
                    shutil.copyfileobj(src, dst, length=8 * 1024 * 1024)

    csv_count = len(list(month_dir.rglob("*.csv")))
    print(f"{month}: {csv_count} trip CSV files ready", flush=True)
    return month, csv_count, downloaded

# COMMAND ----------

# Build the list of months to fetch.
months = list(TRIP_MONTHS + TRIP_MONTHS_EXTENDED)
months += [m.strip() for m in dbutils.widgets.get("extra_months").split(",") if m.strip()]

if DISCOVER_NEW_MONTHS and LOOKBACK_MONTHS > 0:
    for month in previous_months(LOOKBACK_MONTHS):
        if month in months:
            continue
        if is_published(month):
            print(f"Found newly published month: {month}")
            months.append(month)
        else:
            print(f"{month} is not published yet")

months = sorted(set(months))
print("Months to process:", ", ".join(months))

# COMMAND ----------

results, failures = [], []
with ThreadPoolExecutor(max_workers=MAX_PARALLEL) as pool:
    futures = {pool.submit(process_month, month): month for month in months}
    for future in as_completed(futures):
        month = futures[future]
        try:
            results.append(future.result())
        except Exception as error:  # keep going so one bad month does not block the others
            print(f"FAILED {month}: {error}", flush=True)
            failures.append((month, str(error)))

results.sort()
new_months = [month for month, _, downloaded in results if downloaded]
display(spark.createDataFrame(
    [(m, n, d) for m, n, d in results] or [("", 0, False)],
    "month string, csv_files int, downloaded_this_run boolean",
))

# Let downstream tasks in the same job know what changed (for example, only reload new months).
dbutils.jobs.taskValues.set(key="months_ready", value=[m for m, _, _ in results])
dbutils.jobs.taskValues.set(key="new_months", value=new_months)

if failures:
    # Fail the run so the job's retry and alerting settings kick in.
    raise RuntimeError(f"{len(failures)} month(s) failed: {failures}")
