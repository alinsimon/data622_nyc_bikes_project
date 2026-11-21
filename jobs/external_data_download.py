# Databricks notebook source
# MAGIC %md
# MAGIC # External data download (MTA, weather, GBFS)
# MAGIC
# MAGIC Collects every non-trip source that `Citi Bike Demand Analysis.ipynb` fetches and saves it in the Unity Catalog
# MAGIC volume under the same file names the notebook uses, so the analysis can read them unchanged.
# MAGIC
# MAGIC | Kind | Files | Behaviour on each run |
# MAGIC |---|---|---|
# MAGIC | Refreshed | `mta_subway_alerts_2026.csv`, `mta_g_delay_incidents_monthly.csv` | Re-downloaded, so new records are picked up |
# MAGIC | Extended | `greenpoint_weather_hourly_2026.csv` | Seeded once, then every new complete hour is appended on each run |
# MAGIC | Snapshots | live MTA alert feed, GBFS station information and status | A dated copy is added to an archive folder each run, which builds the history the live feeds do not keep. The undated "latest" file is written only if missing, so the analysis inputs do not change under it |
# MAGIC | One time | MTA stations, 2012-2020 alerts, hourly ridership, extended weather (2024 and Feb-Jul 2026) | Downloaded only when the file is missing (or `force_refresh = true`) |
# MAGIC
# MAGIC Weather is requested in UTC and converted to New York clock time in `src/citibike/transformations.py`, because the
# MAGIC archive's own time zone option applies one UTC offset to the whole request and shifts half of the year by an hour.
# MAGIC Set `refresh_weather = true` to rebuild the three weather files (and nothing else) from scratch.
# MAGIC
# MAGIC Each source runs on its own, so one failing API does not block the others; the run fails at the end if any source failed,
# MAGIC which triggers the job's retry and alert settings.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "citibike", "Schema")
dbutils.widgets.text("volume", "raw", "Volume")
dbutils.widgets.text("alerts_start", "2026-01-01", "First date of the subway alert download (YYYY-MM-DD)")
dbutils.widgets.dropdown("force_refresh", "false", ["true", "false"], "Re-download one-time files")
dbutils.widgets.dropdown("refresh_weather", "false", ["true", "false"], "Rebuild the weather files")

# COMMAND ----------

import json
import shutil
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

import sys

import numpy as np
import pandas as pd

# Make src/ importable: walk up from this notebook to the folder that holds databricks.yml.
REPO_ROOT = next((p for p in [Path.cwd(), *Path.cwd().parents] if (p / "databricks.yml").exists()), None)
if REPO_ROOT is None:
    raise FileNotFoundError("Could not find the project root (databricks.yml) above " + str(Path.cwd()))
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))

from citibike.transformations import utc_weather_to_new_york

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
VOLUME = dbutils.widgets.get("volume").strip()
ALERTS_START = dbutils.widgets.get("alerts_start").strip()
FORCE_REFRESH = dbutils.widgets.get("force_refresh") == "true"
REFRESH_WEATHER = dbutils.widgets.get("refresh_weather") == "true"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.`{VOLUME}`")

DATA_DIR = Path(f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/datasource")
DATA_DIR.mkdir(parents=True, exist_ok=True)

API_HEADERS = {"User-Agent": "DATA622-CitiBike-Analysis/1.0"}
SODA_ROOT = "https://data.ny.gov/resource"
MTA_LIVE_ALERTS_URL = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Fsubway-alerts.json"
MTA_STATIONS_URL = "https://data.ny.gov/api/views/39hk-dx4f/rows.csv?accessType=DOWNLOAD"
STATION_STATUS_URL = "https://gbfs.lyft.com/gbfs/1.1/bkn/en/station_status.json"
STATION_INFO_URL = "https://gbfs.lyft.com/gbfs/1.1/bkn/en/station_information.json"
WEATHER_ENDPOINT = "https://archive-api.open-meteo.com/v1/archive"

# Same rectangle and rules the analysis uses to pick the Greenpoint stations.
GREENPOINT_LATITUDE = (40.720, 40.740)
GREENPOINT_LONGITUDE = (-73.965, -73.935)

# Same G station complexes and analysis window the analysis uses for hourly ridership.
G_COMPLEX_IDS = [175, 236, 237, 238, 240, 241, 242, 243, 282, 283, 284, 286, 287,
                 288, 289, 290, 291, 292, 606, 608, 629]
RIDERSHIP_WINDOW = ("2026-08-01T00:00:00", "2026-10-15T00:00:00")

# Weather. The station weather file is seeded once with the months below, then extended forward on every run
# from the last stored hour, so new months arrive without any change here. The extended file is a fixed
# historical range (2024 shutdown check, 2026 baseline) and is downloaded once.
WEATHER_MONTHS = {
    "2026-01": ("2026-01-01", "2026-01-31"),
    "2026-08": ("2026-08-01", "2026-08-31"),
    "2026-09": ("2026-09-01", "2026-09-30"),
}
EXTENDED_WEATHER_RANGES = [("2024-05-01", "2024-09-30"), ("2026-02-01", "2026-07-31")]
# The Open-Meteo archive trails real time by a few days; newer days are requested only once they are published.
WEATHER_ARCHIVE_LAG_DAYS = 5

print("Volume path:", DATA_DIR)

# COMMAND ----------

def fetch_bytes(url, parameters=None, attempts=4):
    """GET `url`, retrying with backoff on network errors, rate limits and server errors."""
    full_url = f"{url}?{urlencode(parameters)}" if parameters else url
    for attempt in range(1, attempts + 1):
        try:
            with urlopen(Request(full_url, headers=API_HEADERS), timeout=120) as response:
                return response.read()
        except HTTPError as error:
            if error.code not in (429, 500, 502, 503, 504) or attempt == attempts:
                raise
            problem = error
        except (URLError, TimeoutError, ConnectionError) as error:
            if attempt == attempts:
                raise
            problem = error
        wait = 20 * attempt
        print(f"  retry {attempt}/{attempts - 1} for {url} in {wait}s ({problem})", flush=True)
        time.sleep(wait)


def fetch_json(url, parameters=None):
    return json.loads(fetch_bytes(url, parameters))


def fetch_socrata(resource, parameters, page_size=50_000):
    """Page through a Socrata resource. Every query must carry a stable $order."""
    records, offset = [], 0
    while True:
        page = fetch_json(f"{SODA_ROOT}/{resource}.json", {**parameters, "$limit": page_size, "$offset": offset})
        if not isinstance(page, list):
            raise TypeError(f"Expected a row list from Socrata resource {resource}.")
        records.extend(page)
        if len(page) < page_size:
            break
        offset += page_size
    if not records:
        raise ValueError(f"Socrata resource {resource} returned no rows for the requested query.")
    return pd.DataFrame.from_records(records)


def write_file(path, write):
    """Build the file on local disk, then copy it to the volume, so a failure never leaves a partial file there."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as tmp:
        local = Path(tmp) / path.name
        write(local)
        shutil.copyfile(local, path)


def save_csv(frame, path):
    write_file(path, lambda local: frame.to_csv(local, index=False))


def save_bytes(data, path):
    write_file(path, lambda local: local.write_bytes(data))


def is_missing(path):
    return FORCE_REFRESH or not path.exists()

# COMMAND ----------

# Refreshed on every run

def mta_subway_alerts():
    end = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")
    frame = fetch_socrata("7kct-peq7", {
        "$select": "date,agency,status_label,affected,header",
        "$where": f"agency = 'NYCT Subway' AND date >= '{ALERTS_START}T00:00:00' AND date < '{end}T00:00:00'",
        "$order": "date ASC",
    })
    frame["date"] = pd.to_datetime(frame["date"], errors="raise")
    save_csv(frame, DATA_DIR / "mta_subway_alerts_2026.csv")
    return f"refreshed: {len(frame):,} rows, {frame['date'].min():%Y-%m-%d} to {frame['date'].max():%Y-%m-%d}"


def g_delay_incidents():
    frame = fetch_socrata("g937-7k7c", {"$where": "line = 'G' AND month >= '2025-01-01T00:00:00'", "$order": "month"})
    frame["month"] = pd.to_datetime(frame["month"])
    frame["incidents"] = pd.to_numeric(frame["incidents"])
    save_csv(frame, DATA_DIR / "mta_g_delay_incidents_monthly.csv")
    return f"refreshed: {len(frame):,} rows"

# COMMAND ----------

# Snapshots: a dated archive copy on every run, and the undated "latest" file only when it is missing.

def archive_snapshot(data, latest_name, archive_dir, stamp):
    archive_path = DATA_DIR / archive_dir / f"{Path(latest_name).stem}_{stamp}.json"
    save_bytes(data, archive_path)
    latest = DATA_DIR / latest_name
    if not latest.exists():
        save_bytes(data, latest)
    return archive_path.name


def live_alerts_snapshot():
    data = fetch_bytes(MTA_LIVE_ALERTS_URL)
    feed = json.loads(data)
    if "entity" not in feed or "timestamp" not in feed.get("header", {}):
        raise ValueError("The MTA live alert feed has an unexpected structure.")
    stamp = pd.to_datetime(feed["header"]["timestamp"], unit="s", utc=True).strftime("%Y%m%dT%H%M%SZ")
    name = archive_snapshot(data, "mta_live_alerts_snapshot.json", "mta_live_alerts", stamp)
    return f"archived {name} ({len(feed['entity']):,} alerts)"


def gbfs_snapshot(url, latest_name, archive_dir):
    data = fetch_bytes(url)
    feed = json.loads(data)
    if not feed.get("data", {}).get("stations"):
        raise ValueError(f"The GBFS feed {url} returned no stations.")
    stamp = datetime.fromtimestamp(feed["last_updated"], tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    name = archive_snapshot(data, latest_name, archive_dir, stamp)
    return f"archived {name} ({len(feed['data']['stations']):,} stations)"

# COMMAND ----------

# One time downloads

def mta_subway_stations():
    path = DATA_DIR / "MTA_Subway_Stations_20261003.csv"
    if not is_missing(path):
        return "skipped: already present"
    data = fetch_bytes(MTA_STATIONS_URL)
    header = data.split(b"\n", 1)[0].decode("utf-8-sig")
    if "GTFS Stop ID" not in header:
        raise ValueError(f"The MTA stations export has unexpected columns: {header}")
    save_bytes(data, path)
    return "downloaded"


def legacy_g_alerts():
    path = DATA_DIR / "mta_g_alerts_2012_2020.csv"
    if not is_missing(path):
        return "skipped: already present"
    frame = fetch_socrata("3h5b-5ktz", {
        "$select": "date,agency,status_label,affected,header",
        "$where": (
            "agency = 'Subway' AND (upper(header) like '%[G]%' "
            "OR upper(header) like '%G TRAIN%' OR upper(header) like '%NO G %')"
        ),
        "$order": "date ASC",
    })
    frame["date"] = pd.to_datetime(frame["date"], errors="raise")
    save_csv(frame, path)
    return f"downloaded: {len(frame):,} rows"


def g_station_ridership():
    path = DATA_DIR / "mta_g_station_hourly_ridership_2026.csv"
    if not is_missing(path):
        return "skipped: already present"
    id_list = ",".join(f"'{i}'" for i in G_COMPLEX_IDS)
    frame = fetch_socrata("5wq4-mkjj", {
        "$select": "transit_timestamp,station_complex_id,station_complex,sum(ridership) as ridership",
        "$where": (
            f"transit_timestamp >= '{RIDERSHIP_WINDOW[0]}' AND transit_timestamp < '{RIDERSHIP_WINDOW[1]}' "
            f"AND station_complex_id in({id_list})"
        ),
        "$group": "transit_timestamp,station_complex_id,station_complex",
        "$order": "transit_timestamp,station_complex_id",
    })
    frame["transit_timestamp"] = pd.to_datetime(frame["transit_timestamp"], errors="raise")
    frame["ridership"] = pd.to_numeric(frame["ridership"], errors="raise")
    save_csv(frame, path)
    return f"downloaded: {len(frame):,} rows"

# COMMAND ----------

# Weather. Station coordinates come from the station_info.parquet export when it has been uploaded to the volume
# (the exact station set the analysis uses), otherwise from the live GBFS station information feed.

def greenpoint_stations():
    parquet_path = DATA_DIR / "station_info.parquet"
    if parquet_path.exists():
        info = pd.read_parquet(parquet_path)
        info = info.loc[info["snapshot_date"].eq(info["snapshot_date"].max())]
    else:
        info = pd.DataFrame(fetch_json(STATION_INFO_URL)["data"]["stations"])
    info = info[["station_id", "lat", "lon", "capacity"]].copy()
    info["capacity"] = pd.to_numeric(info["capacity"], errors="coerce")
    stations = info.loc[
        info["lat"].between(*GREENPOINT_LATITUDE)
        & info["lon"].between(*GREENPOINT_LONGITUDE)
        & info["capacity"].gt(0),
        ["station_id", "lat", "lon"],
    ].drop_duplicates()
    stations["station_id"] = stations["station_id"].astype("string")
    if stations.empty:
        raise ValueError("The Greenpoint screening rectangle returned no active stations.")
    return stations


def utc_request_dates(start_date, end_date):
    """UTC dates that cover New York dates start_date to end_date (New York runs 4 or 5 hours behind UTC)."""
    return (pd.Timestamp(start_date).strftime("%Y-%m-%d"),
            (pd.Timestamp(end_date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d"))


def fetch_hourly(latitude, longitude, start_date, end_date):
    """Hourly temperature and precipitation for one weather grid cell, in New York clock time."""
    utc_start, utc_end = utc_request_dates(start_date, end_date)
    response = fetch_json(WEATHER_ENDPOINT, {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": utc_start,
        "end_date": utc_end,
        "hourly": "temperature_2m,precipitation",
        "timezone": "GMT",
    })
    return utc_weather_to_new_york(pd.DataFrame(response["hourly"]), start=start_date, end=end_date)


def build_station_weather(lookup_path, weather_path):
    stations = greenpoint_stations()
    lookup_records, weather_parts = [], []
    for month, (start_date, end_date) in WEATHER_MONTHS.items():
        utc_start, utc_end = utc_request_dates(start_date, end_date)
        responses = fetch_json(WEATHER_ENDPOINT, {
            "latitude": ",".join(stations["lat"].astype(str)),
            "longitude": ",".join(stations["lon"].astype(str)),
            "start_date": utc_start,
            "end_date": utc_end,
            "hourly": "temperature_2m,precipitation",
            "timezone": "GMT",
            "cell_selection": "land",
            "elevation": ",".join(["nan"] * len(stations)),
        })
        if isinstance(responses, dict):
            responses = [responses]
        if len(responses) != len(stations):
            raise ValueError(f"Weather API returned {len(responses)} locations for {len(stations)} stations.")

        for station, response in zip(stations.itertuples(index=False), responses, strict=True):
            cell_id = f"open_meteo_{response['latitude']:.5f}_{response['longitude']:.5f}"
            lookup_records.append({
                "station_id": station.station_id,
                "station_latitude": station.lat,
                "station_longitude": station.lon,
                "weather_cell_id": cell_id,
                "weather_latitude": response["latitude"],
                "weather_longitude": response["longitude"],
            })
            hourly = response.get("hourly")
            if not hourly or not hourly.get("time"):
                raise ValueError(f"Weather API returned no hourly records for {month}.")
            part = utc_weather_to_new_york(pd.DataFrame(hourly), start=start_date, end=end_date)
            part["month"] = month
            part["weather_cell_id"] = cell_id
            part["weather_latitude"] = response["latitude"]
            part["weather_longitude"] = response["longitude"]
            weather_parts.append(part)

    lookup = pd.DataFrame(lookup_records).drop_duplicates("station_id")
    observations = pd.concat(weather_parts, ignore_index=True)
    duplicated = observations.duplicated(["weather_cell_id", "time"], keep=False)
    disagreement = observations.loc[duplicated].groupby(["weather_cell_id", "time"])[
        ["temperature_2m", "precipitation"]
    ].nunique(dropna=False)
    if disagreement.gt(1).any().any():
        raise ValueError("Weather responses disagree for the same grid cell and local hour.")
    weather = observations.drop_duplicates(["weather_cell_id", "time"]).sort_values(["weather_cell_id", "time"])
    if weather[["temperature_2m", "precipitation"]].notna().sum().eq(0).any():
        raise ValueError("Weather API returned no usable temperature or precipitation values.")
    save_csv(lookup, lookup_path)
    save_csv(weather, weather_path)
    return len(lookup), len(weather)


def extend_station_weather(lookup, weather_path):
    """Append every complete hour after the last stored one, up to the newest day the archive has published."""
    existing = pd.read_csv(weather_path, parse_dates=["time"])
    last_time = existing["time"].max()
    first_day = (last_time + pd.Timedelta(hours=1)).normalize()
    last_day = pd.Timestamp(datetime.now(ZoneInfo("America/New_York")).date() - timedelta(days=WEATHER_ARCHIVE_LAG_DAYS))
    if first_day > last_day:
        return 0

    new_parts = []
    for cell in lookup.drop_duplicates("weather_cell_id").itertuples(index=False):
        # One request per calendar month keeps each response small.
        for month_start in pd.date_range(first_day.replace(day=1), last_day, freq="MS"):
            start = max(first_day, month_start)
            end = min(last_day, month_start + pd.offsets.MonthEnd(0))
            part = fetch_hourly(cell.weather_latitude, cell.weather_longitude, start, end)
            part["month"] = part["time"].dt.strftime("%Y-%m")
            part["weather_cell_id"] = cell.weather_cell_id
            part["weather_latitude"] = cell.weather_latitude
            part["weather_longitude"] = cell.weather_longitude
            new_parts.append(part)

    new = pd.concat(new_parts, ignore_index=True)
    # Drop hours the archive has not filled in yet, so they are fetched again on a later run.
    new = new[new["time"].gt(last_time) & new[["temperature_2m", "precipitation"]].notna().any(axis=1)]
    if new.empty:
        return 0
    combined = pd.concat([existing, new], ignore_index=True)
    combined = combined.drop_duplicates(["weather_cell_id", "time"]).sort_values(["weather_cell_id", "time"])
    save_csv(combined, weather_path)
    return len(new)


def build_extended_weather(cell, path):
    parts = []
    for start_date, end_date in EXTENDED_WEATHER_RANGES:
        parts.append(fetch_hourly(cell["weather_latitude"], cell["weather_longitude"], start_date, end_date))
    frame = pd.concat(parts, ignore_index=True)
    save_csv(frame, path)
    return len(frame)


def greenpoint_weather():
    lookup_path = DATA_DIR / "greenpoint_station_weather_lookup.csv"
    weather_path = DATA_DIR / "greenpoint_weather_hourly_2026.csv"
    extended_path = DATA_DIR / "greenpoint_weather_hourly_extended.csv"
    notes = []
    if REFRESH_WEATHER or is_missing(lookup_path) or is_missing(weather_path):
        stations, rows = build_station_weather(lookup_path, weather_path)
        notes.append(f"seeded {stations} stations, {rows:,} hourly rows")
    lookup = pd.read_csv(lookup_path)
    added = extend_station_weather(lookup, weather_path)
    latest = pd.read_csv(weather_path, parse_dates=["time"])["time"].max()
    notes.append(f"added {added:,} new hourly rows, data through {latest:%Y-%m-%d %H:%M}")
    if REFRESH_WEATHER or is_missing(extended_path):
        notes.append(f"extended weather: {build_extended_weather(lookup.iloc[0], extended_path):,} hourly rows")
    return "; ".join(notes)

# COMMAND ----------

stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
steps = [
    ("mta_subway_alerts_2026", mta_subway_alerts),
    ("mta_g_delay_incidents_monthly", g_delay_incidents),
    ("mta_live_alerts_snapshot", live_alerts_snapshot),
    ("gbfs_station_information", lambda: gbfs_snapshot(STATION_INFO_URL, "station_information.json", "gbfs_station_information")),
    ("gbfs_station_status", lambda: gbfs_snapshot(STATION_STATUS_URL, "station_status.json", "gbfs_station_status")),
    ("MTA_Subway_Stations", mta_subway_stations),
    ("mta_g_alerts_2012_2020", legacy_g_alerts),
    ("mta_g_station_hourly_ridership_2026", g_station_ridership),
    ("greenpoint_weather", greenpoint_weather),
]

results, failures = [], []
for name, step in steps:
    print(f"--- {name}", flush=True)
    try:
        outcome = step()
        results.append((name, "ok", outcome))
        print(" ", outcome, flush=True)
    except Exception as error:  # keep going so one failing source does not block the others
        results.append((name, "failed", str(error)))
        failures.append(name)
        print(f"  FAILED: {error}", flush=True)

display(spark.createDataFrame(results, "source string, status string, detail string"))
dbutils.jobs.taskValues.set(key="failed_sources", value=failures)

if failures:
    # Fail the run so the job's retry and alerting settings kick in.
    raise RuntimeError(f"{len(failures)} source(s) failed: {', '.join(failures)}")
