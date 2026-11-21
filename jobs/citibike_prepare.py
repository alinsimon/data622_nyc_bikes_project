# Databricks notebook source
# MAGIC %md
# MAGIC # Citi Bike data preparation
# MAGIC
# MAGIC Turns the raw files in the volume into Delta tables, ready to analyse. It reproduces the cleaning and table
# MAGIC building of the analysis notebook (sections 2.1 to 2.6) without the figures. The study logic lives in
# MAGIC `src/citibike/transformations.py`, which the analysis notebook also uses.
# MAGIC
# MAGIC **Raw tables** (one per reference file in the volume, values unchanged, column names in snake case):
# MAGIC `raw_station_info`, `raw_mta_subway_stations`, `raw_mta_subway_alerts_2026`, `raw_mta_g_alerts_2012_2020`,
# MAGIC `raw_mta_g_delay_incidents_monthly`, `raw_mta_g_station_hourly_ridership_2026`, `raw_mta_live_alerts_snapshot`,
# MAGIC `raw_greenpoint_station_weather_lookup`, `raw_greenpoint_weather_hourly_2026`, `raw_greenpoint_weather_hourly_extended`.
# MAGIC
# MAGIC **Cleaned and analysis tables:**
# MAGIC
# MAGIC | Table | Grain | Built from |
# MAGIC |---|---|---|
# MAGIC | `trips` | one cleaned trip | trip CSVs, every month in the volume |
# MAGIC | `trips_ingest_log` | trip month | per-month file signature and quality counts |
# MAGIC | `trips_station_hourly` | station code (`short_name`) by hour, hours with trips | `trips` |
# MAGIC | `station_status_hourly` | station (`station_id`) by hour | `station_status/` snapshots |
# MAGIC | `stations` | station, latest snapshot | `station_info.parquet` |
# MAGIC | `weather_hourly` | hour | Open-Meteo CSVs |
# MAGIC | `mta_g_stops` | G stop | MTA subway stations CSV |
# MAGIC | `g_service_closures_confirmed`, `g_service_closures_confirmed_pre_august` | closure weekend | MTA notices |
# MAGIC | `g_service_reduction_windows_inferred` | window | MTA hourly ridership |
# MAGIC | `greenpoint_station_exposure` | Greenpoint station | stations and G stops |
# MAGIC | `model_station_sets` | station | corridor and control stations |
# MAGIC | `mta_subway_shutdowns` | event by affected stop | closure events |
# MAGIC | `analysis_station_hour_panel` | station by hour, Greenpoint, Aug to Sep 2026 | section 2.5 panel |
# MAGIC | `analysis_station_hour_panel_extended` | station by hour, 2024 and 2026 | **main modeling table** |
# MAGIC | `baseline_accuracy_by_month` | month by station group | baseline errors |
# MAGIC | `shutdown_effect_screening` | event by station group | actual to profile demand ratios (Figure 10) |
# MAGIC | `data_quality_checks` | check | results of this run |
# MAGIC
# MAGIC * **Incremental trips.** A trip month is loaded only when it is new or its CSV files changed size, so a weekly
# MAGIC   run with nothing new only rebuilds the small tables. Set `full_refresh = true` to reload every month.
# MAGIC * **Times** are New York clock times stored as `TIMESTAMP_NTZ`. Status fetch times are converted from UTC.
# MAGIC * `target_schema` writes the tables to another schema of the same catalog (the raw files are still read from
# MAGIC   `schema`). It is used to check changes without touching the real tables.

# COMMAND ----------

import sys
from pathlib import Path

# Make src/ importable: walk up from this notebook to the folder that holds databricks.yml.
REPO_ROOT = next((p for p in [Path.cwd(), *Path.cwd().parents] if (p / "databricks.yml").exists()), None)
if REPO_ROOT is None:
    raise FileNotFoundError("Could not find the project root (databricks.yml) above " + str(Path.cwd()))
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))

from citibike.config import load_config

dbutils.widgets.dropdown("full_refresh", "false", ["true", "false"], "Reload every trip month")
dbutils.widgets.text("target_schema", "", "Write tables to schema (empty: same as schema)")
CONFIG = load_config(dbutils)
TARGET = CONFIG.with_schema(dbutils.widgets.get("target_schema").strip() or CONFIG.schema)
FULL_REFRESH = dbutils.widgets.get("full_refresh") == "true"

# COMMAND ----------

import json
import re
import shutil
import tempfile
from datetime import datetime, timezone

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import types as T

from citibike import study
from citibike import transformations as tf

CATALOG, SCHEMA = TARGET.catalog, TARGET.schema
DATA_DIR = Path(CONFIG.raw_data_dir)
# pandas tables are staged as parquet here before they are loaded into Delta (the local notebook's processed folder)
PROCESSED_DIR = DATA_DIR / ("processed" if TARGET == CONFIG else f"processed_{TARGET.schema}")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
RUN_AT = datetime.now(timezone.utc)

# Every time in this project is a New York clock time without an offset. With the session in UTC Spark never
# shifts those values, and tables store them as TIMESTAMP_NTZ so readers in any time zone see the same clock time.
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")


def fq(name):
    return TARGET.table(name)


def table_exists(name):
    return spark.catalog.tableExists(f"{CATALOG}.{SCHEMA}.{name}")


def to_ntz(frame):
    for field in frame.schema.fields:
        if isinstance(field.dataType, T.TimestampType):
            frame = frame.withColumn(field.name, F.col(field.name).cast("timestamp_ntz"))
    return frame


def set_comment(name, comment):
    spark.sql(f"COMMENT ON TABLE {fq(name)} IS '{comment.replace(chr(39), '')}'")


def save_spark(frame, name, comment):
    frame.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq(name))
    set_comment(name, comment)
    print(f"Saved {CATALOG}.{SCHEMA}.{name}: {spark.table(fq(name)).count():,} rows")


def snake_case(column):
    return re.sub(r"[^0-9a-zA-Z]+", "_", str(column)).strip("_").lower()


def save_pandas(frame, name, comment):
    """Stage a pandas table as parquet in the volume, then load that file into a Delta table."""
    frame = frame.copy()
    # Delta column names cannot contain spaces or punctuation, e.g. "North Direction Label" -> north_direction_label
    frame.columns = [snake_case(column) for column in frame.columns]
    for column in frame.columns:
        if isinstance(frame[column].dtype, pd.CategoricalDtype):
            frame[column] = frame[column].astype("string")
    target = PROCESSED_DIR / f"{name}.parquet"
    with tempfile.TemporaryDirectory() as tmp:
        local = Path(tmp) / target.name
        frame.to_parquet(local, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
        shutil.copyfile(local, target)
    save_spark(to_ntz(spark.read.parquet(str(target))), name, comment)


quality = []


def check(section, name, value):
    quality.append((section, name, str(value)))
    print(f"{name}: {value}")


def require(path):
    if not path.exists():
        raise FileNotFoundError(f"Required file not found in the volume: {path}")
    return path


print("Raw files:", DATA_DIR)
print("Tables written to:", f"{CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Trips
# MAGIC
# MAGIC Cleaning rules (section 2.3 of the analysis notebook):
# MAGIC * station codes are kept as strings, so codes such as `4665.02` keep their exact form;
# MAGIC * trips with a missing or unreadable start or end time, or an end time at or before the start, are dropped;
# MAGIC * trips without a start or end station are kept, because the other end can still be counted;
# MAGIC * repeated `ride_id` values are dropped (the local notebook did not check this; the count is logged).

# COMMAND ----------

TRIP_COLUMNS = [
    "ride_id", "rideable_type", "started_at", "ended_at", "start_station_name", "start_station_id",
    "end_station_name", "end_station_id", "start_lat", "start_lng", "end_lat", "end_lng", "member_casual",
]
TRIP_SCHEMA = T.StructType([T.StructField(column, T.StringType()) for column in TRIP_COLUMNS])
TRIP_FOLDER = re.compile(r"^(\d{4})(\d{2})-citibike-tripdata$")
LOG_SCHEMA = (
    "trip_month string, csv_files int, csv_bytes long, rows_read long, missing_start_station long, "
    "missing_end_station long, invalid_time long, nonpositive_duration long, duplicate_ride_id long, "
    "rows_kept long, loaded_at timestamp"
)


def trip_sources():
    sources = {}
    for folder in sorted(DATA_DIR.iterdir()):
        match = TRIP_FOLDER.match(folder.name)
        if match and folder.is_dir():
            files = sorted(folder.glob("*.csv"))
            if files:
                sources[f"{match[1]}-{match[2]}"] = (folder, len(files), sum(f.stat().st_size for f in files))
    return sources


def blank_to_null(column):
    value = F.trim(F.col(column))
    return F.when(value == "", None).otherwise(value).alias(column)


def load_trip_month(month, folder, replace):
    raw = spark.read.csv(f"{folder}/*.csv", header=True, schema=TRIP_SCHEMA)
    parsed = raw.select(
        blank_to_null("ride_id"),
        blank_to_null("rideable_type"),
        F.expr("try_to_timestamp(started_at)").alias("started_at"),
        F.expr("try_to_timestamp(ended_at)").alias("ended_at"),
        blank_to_null("start_station_id"),
        blank_to_null("start_station_name"),
        blank_to_null("end_station_id"),
        blank_to_null("end_station_name"),
        F.expr("try_cast(start_lat AS DOUBLE)").alias("start_lat"),
        F.expr("try_cast(start_lng AS DOUBLE)").alias("start_lng"),
        F.expr("try_cast(end_lat AS DOUBLE)").alias("end_lat"),
        F.expr("try_cast(end_lng AS DOUBLE)").alias("end_lng"),
        blank_to_null("member_casual"),
    )
    times_ok = F.col("started_at").isNotNull() & F.col("ended_at").isNotNull()
    stats = parsed.agg(
        F.count(F.lit(1)).alias("rows_read"),
        F.sum(F.col("start_station_id").isNull().cast("long")).alias("missing_start_station"),
        F.sum(F.col("end_station_id").isNull().cast("long")).alias("missing_end_station"),
        F.sum((~times_ok).cast("long")).alias("invalid_time"),
        F.sum((times_ok & (F.col("ended_at") <= F.col("started_at"))).cast("long")).alias("nonpositive_duration"),
    ).first().asDict()

    clean = (
        parsed.where(times_ok & (F.col("ended_at") > F.col("started_at")))
        .dropDuplicates(["ride_id"])
        .withColumn("duration_min", F.expr("(unix_micros(ended_at) - unix_micros(started_at)) / 60000000.0"))
        .withColumn("trip_month", F.lit(month))
    )
    writer = to_ntz(clean).write.format("delta").mode("overwrite")
    if replace:
        writer = writer.option("replaceWhere", f"trip_month = '{month}'")
    else:
        writer = writer.option("overwriteSchema", "true").partitionBy("trip_month")
    writer.saveAsTable(fq("trips"))

    rows_kept = spark.table(fq("trips")).where(F.col("trip_month") == month).count()
    valid_rows = stats["rows_read"] - stats["invalid_time"] - stats["nonpositive_duration"]
    return {**{k: int(v or 0) for k, v in stats.items()}, "duplicate_ride_id": valid_rows - rows_kept, "rows_kept": rows_kept}


sources = trip_sources()
if not sources:
    raise FileNotFoundError(f"No extracted trip folders (YYYYMM-citibike-tripdata) found in {DATA_DIR}")

keep_log = table_exists("trips") and table_exists("trips_ingest_log") and not FULL_REFRESH
log = {row["trip_month"]: row.asDict() for row in spark.table(fq("trips_ingest_log")).collect()} if keep_log else {}
to_load = [month for month, (_, _, size) in sources.items() if month not in log or log[month]["csv_bytes"] != size]
print(f"Trip months in the volume: {', '.join(sources)}")
print(f"Months to load: {', '.join(to_load) if to_load else 'none, trips table is up to date'}")

table_ready = keep_log
for month in to_load:
    folder, file_count, size = sources[month]
    print(f"Loading {month} ({file_count} files, {size / 1e9:.2f} GB)", flush=True)
    result = load_trip_month(month, folder, replace=table_ready)
    table_ready = True
    log[month] = {"trip_month": month, "csv_files": file_count, "csv_bytes": size, **result, "loaded_at": RUN_AT}
    spark.createDataFrame([tuple(log[m][c.split()[0]] for c in LOG_SCHEMA.split(", ")) for m in sorted(log)], LOG_SCHEMA) \
        .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq("trips_ingest_log"))
    print(f"  kept {result['rows_kept']:,} of {result['rows_read']:,} rows", flush=True)

if to_load:
    set_comment("trips", "Cleaned Citi Bike trips, one row per ride. Times are New York local time. Built by citibike_prepare.")
    set_comment("trips_ingest_log", "Per trip month: source file signature, rows read, rows dropped by each rule and rows kept.")

ingest = spark.table(fq("trips_ingest_log")).toPandas()
for column in ["rows_read", "missing_start_station", "missing_end_station", "invalid_time",
               "nonpositive_duration", "duplicate_ride_id", "rows_kept"]:
    check("trips", f"Trips: {column.replace('_', ' ')}", f"{int(ingest[column].sum()):,}")
display(ingest)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Hourly trips per station and hourly station status

# COMMAND ----------

trips = spark.table(fq("trips"))


def hourly_flow(station_column, time_column, name):
    return (
        trips.where(F.col(station_column).isNotNull())
        .groupBy(
            F.col(station_column).alias("short_name"),
            F.date_trunc("hour", F.col(time_column).cast("timestamp")).alias("hour"),
        )
        .agg(F.count(F.lit(1)).alias(name))
    )


trips_station_hourly = (
    hourly_flow("start_station_id", "started_at", "departures")
    .join(hourly_flow("end_station_id", "ended_at", "arrivals"), ["short_name", "hour"], "full")
    .fillna(0, ["departures", "arrivals"])
)
save_spark(
    to_ntz(trips_station_hourly),
    "trips_station_hourly",
    "Departures and arrivals per station code (short_name) and New York clock hour. Only hours with trips have rows.",
)

# Station status snapshots: fetch times are UTC, converted to New York clock time and summarized per hour.
status_dir = require(DATA_DIR / "station_status")
status = spark.read.parquet(str(status_dir)).select(
    "station_id", "fetched_at", "num_bikes_available", "num_docks_available"
)
status_stats = status.agg(
    F.count(F.lit(1)).alias("rows"),
    F.sum((F.col("station_id").isNull() | F.col("fetched_at").isNull() | F.col("num_bikes_available").isNull()
           | F.col("num_docks_available").isNull()).cast("long")).alias("missing"),
    F.sum(((F.col("num_bikes_available") < 0) | (F.col("num_docks_available") < 0)).cast("long")).alias("negative"),
).first()
check("status", "Status snapshots read", f"{status_stats['rows']:,}")
check("status", "Status snapshots with a missing required value", f"{int(status_stats['missing'] or 0):,}")
check("status", "Status snapshots with negative bike or dock counts", f"{int(status_stats['negative'] or 0):,}")

station_status_hourly = (
    status.where(F.col("station_id").isNotNull() & F.col("fetched_at").isNotNull())
    .withColumn(
        "hour",
        F.date_trunc("hour", F.from_utc_timestamp(F.col("fetched_at").cast("timestamp"), "America/New_York")),
    )
    .groupBy("station_id", "hour")
    .agg(
        F.avg("num_bikes_available").alias("bikes_available_mean"),
        F.avg("num_docks_available").alias("docks_available_mean"),
        F.min("num_docks_available").alias("min_docks_available"),
        F.coalesce(F.sum((F.col("num_bikes_available") == 0).cast("long")), F.lit(0)).alias("zero_bike_snapshots"),
        F.coalesce(F.sum((F.col("num_docks_available") == 0).cast("long")), F.lit(0)).alias("zero_dock_snapshots"),
        F.count(F.lit(1)).alias("snapshot_count"),
    )
)
save_spark(
    to_ntz(station_status_hourly),
    "station_status_hourly",
    "Bikes and docks available per GBFS station_id and New York clock hour, summarized from status snapshots.",
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Raw reference files as tables
# MAGIC
# MAGIC Each small reference file in the volume becomes a `raw_` table with the same values, so notebooks can read every
# MAGIC source as a table (also from a local editor through Databricks Connect). Only dates are parsed.

# COMMAND ----------

RAW_FILES = {
    "raw_mta_subway_stations": ("MTA_Subway_Stations_20261003.csv",
                                dict(dtype={"GTFS Stop ID": "string", "Station ID": "string", "Complex ID": "string"}), [],
                                "MTA subway stations file (all lines)."),
    "raw_mta_subway_alerts_2026": ("mta_subway_alerts_2026.csv", {}, ["date"],
                                   "MTA subway alert updates for 2026 (NYCT Subway), refreshed daily by external-data-download."),
    "raw_mta_g_alerts_2012_2020": ("mta_g_alerts_2012_2020.csv", {}, ["date"],
                                   "Historical MTA alert updates mentioning the G train, 2012 to 2020."),
    "raw_mta_g_delay_incidents_monthly": ("mta_g_delay_incidents_monthly.csv", {}, ["month"],
                                          "Monthly G line delay incidents by reporting category."),
    "raw_mta_g_station_hourly_ridership_2026": ("mta_g_station_hourly_ridership_2026.csv", {}, ["transit_timestamp"],
                                                "Hourly ridership at the G station complexes, August to mid October 2026."),
    "raw_greenpoint_station_weather_lookup": ("greenpoint_station_weather_lookup.csv",
                                              dict(dtype={"station_id": "string", "weather_cell_id": "string"}), [],
                                              "Greenpoint station to Open-Meteo grid cell lookup."),
    "raw_greenpoint_weather_hourly_2026": ("greenpoint_weather_hourly_2026.csv", {}, ["time"],
                                           "Hourly Open-Meteo weather for 2026, extended daily by external-data-download."),
    "raw_greenpoint_weather_hourly_extended": ("greenpoint_weather_hourly_extended.csv", {}, ["time"],
                                               "Hourly Open-Meteo weather for May to Sep 2024 and Feb to Jul 2026."),
}
raw = {}
for name, (file_name, read_options, date_columns, comment) in RAW_FILES.items():
    frame = pd.read_csv(require(DATA_DIR / file_name), **read_options)
    for column in date_columns:
        frame[column] = pd.to_datetime(frame[column], errors="coerce")
        if frame[column].isna().any():
            raise ValueError(f"{file_name}: {int(frame[column].isna().sum())} values of {column} could not be parsed.")
    raw[name] = frame
    save_pandas(frame, name, comment)

# Station information snapshots (manual export) and the live MTA alert feed snapshot
station_info_all = pd.read_parquet(require(DATA_DIR / "station_info.parquet"))
save_pandas(station_info_all, "raw_station_info", "Daily Citi Bike station information snapshots (manual GBFS export).")
live_alert_text = require(DATA_DIR / "mta_live_alerts_snapshot.json").read_text(encoding="utf8")
live_alert_header = json.loads(live_alert_text)["header"]
save_pandas(
    pd.DataFrame({"feed_timestamp": [pd.to_datetime(int(live_alert_header["timestamp"]), unit="s")],
                  "snapshot_json": [live_alert_text]}),
    "raw_mta_live_alerts_snapshot",
    "The MTA live subway alert feed snapshot used by the analysis, stored as one JSON document (feed_timestamp is UTC).",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Stations, G stops, weather and closure windows

# COMMAND ----------

station_info, latest_snapshot = tf.latest_station_snapshot(station_info_all)
greenpoint_stations = tf.greenpoint_screen(station_info)
greenpoint_ids = greenpoint_stations["station_id"].dropna().unique()
greenpoint_trip_ids = greenpoint_stations["trip_station_id"].dropna().unique()
check("stations", "Latest station information snapshot", latest_snapshot)
check("stations", "Stations in latest snapshot", len(station_info))
check("stations", "Duplicate station IDs in latest snapshot", int(station_info["station_id"].duplicated().sum()))
check("stations", "Stations in Greenpoint screening rectangle", len(greenpoint_stations))

mta_g_stops = tf.g_stops_from_subway_stations(raw["raw_mta_subway_stations"])
segment_stop_ids = tf.affected_segment_stop_ids(mta_g_stops)
mta_g_stops["affected_segment"] = mta_g_stops["gtfs_stop_id"].isin(segment_stop_ids)
check("mta", "G train stops", len(mta_g_stops))
save_pandas(mta_g_stops, "mta_g_stops", "Subway stops served by the G train, with the 2026 affected segment flag.")

greenpoint_weather = raw["raw_greenpoint_weather_hourly_2026"]
weather_all = tf.combine_weather(greenpoint_weather, raw["raw_greenpoint_weather_hourly_extended"])
check("weather", "Hourly weather rows", len(weather_all))
check("weather", "Hourly weather missing values", int(weather_all[["temperature_2m", "precipitation"]].isna().sum().sum()))
save_pandas(weather_all, "weather_hourly",
            "Hourly temperature (C) and precipitation (mm) for the Greenpoint grid cell, New York local time.")

ridership_check = tf.infer_reduction_windows(raw["raw_mta_g_station_hourly_ridership_2026"])
check("closures", "Inferred G service reduction windows", len(ridership_check.windows))
save_pandas(ridership_check.windows, "g_service_reduction_windows_inferred",
            "G service reduction windows inferred from hourly ridership at Greenpoint Av and Nassau Av.")

g_confirmed_windows = tf.confirmed_closure_windows()
g_confirmed_pre_august = tf.pre_august_closure_windows()
closure_hours = tf.closure_hour_index(g_confirmed_windows)
check("closures", "Confirmed closure hours (August and September 2026)", len(closure_hours))
save_pandas(g_confirmed_windows, "g_service_closures_confirmed",
            "The four August and September 2026 G closure weekends from MTA Weekender notices.")
save_pandas(g_confirmed_pre_august, "g_service_closures_confirmed_pre_august",
            "The nine earlier 2026 G closure weekends. times_assumed marks notices whose times were inferred.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Greenpoint exposure and the section 2.5 panel (Greenpoint, August and September 2026)

# COMMAND ----------

station_exposure = tf.greenpoint_station_exposure(greenpoint_stations, mta_g_stops, segment_stop_ids)
check("exposure", "Greenpoint stations within 0.25 mile of an affected stop",
      f"{int(station_exposure['exposed_station'].sum())} of {len(station_exposure)}")
save_pandas(station_exposure, "greenpoint_station_exposure",
            "Greenpoint stations with the nearest stop of the affected G segment, exposed_station and distance_band.")

sets = tf.station_sets(station_info, greenpoint_stations, mta_g_stops)
model_stations = sets.model_stations

# Hourly trips for the model stations (replaces the chunked CSV pass of the local notebook)
model_codes = [str(code) for code in model_stations["short_name"]]
trip_hourly = spark.table(fq("trips_station_hourly")).where(F.col("short_name").isin(model_codes)).toPandas()
trip_hourly["short_name"] = trip_hourly["short_name"].astype(str)
trip_hourly["hour"] = pd.to_datetime(trip_hourly["hour"]).astype("datetime64[ns]")
trip_hourly[["departures", "arrivals"]] = trip_hourly[["departures", "arrivals"]].astype("int64")

greenpoint_trips = trip_hourly[trip_hourly["short_name"].isin(greenpoint_trip_ids)].merge(
    greenpoint_stations[["station_id", "trip_station_id"]].assign(trip_station_id=lambda f: f["trip_station_id"].astype(str)),
    left_on="short_name", right_on="trip_station_id", how="left", validate="many_to_one",
)
greenpoint_status = spark.table(fq("station_status_hourly")).where(
    F.col("station_id").isin([str(i) for i in greenpoint_ids])
).toPandas()
greenpoint_status["hour"] = pd.to_datetime(greenpoint_status["hour"]).astype("datetime64[ns]")
check("status", "Greenpoint status station hours", f"{len(greenpoint_status):,}")
check("status", "Greenpoint status coverage", f"{greenpoint_status['hour'].min()} to {greenpoint_status['hour'].max()}")

panel = tf.greenpoint_panel(station_exposure, greenpoint_trips, greenpoint_status, greenpoint_weather, closure_hours)
check("panel", "Greenpoint panel rows", f"{len(panel):,} ({panel['station_id'].nunique()} stations)")
check("panel", "Greenpoint panel rows with status", f"{int(panel['status_observed'].sum()):,}")
check("panel", "Greenpoint panel rows missing weather", int(panel["temperature_2m"].isna().sum()))
save_pandas(panel, "analysis_station_hour_panel",
            "Section 2.5 panel: Greenpoint station by hour, August and September 2026, with status, weather and closure flags.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Shutdown events and the extended panel (main modeling table)

# COMMAND ----------

shutdown_events = tf.shutdown_events(g_confirmed_pre_august, g_confirmed_windows, segment_stop_ids)
event_station_exposure, mta_subway_shutdowns = tf.event_exposure(shutdown_events, sets.model_pairs, sets.g_only_stops)
check("events", "Shutdown events", len(shutdown_events))
check("events", "Corridor / control stations",
      f"{int((model_stations['station_group'] == 'corridor').sum())} / {int((model_stations['station_group'] == 'control').sum())}")
save_pandas(mta_subway_shutdowns, "mta_subway_shutdowns",
            "Shutdown table: one row per event and affected G stop, with Citi Bike stations within a quarter mile.")
save_pandas(model_stations, "model_station_sets",
            "Model stations: station_group corridor or control, and distance to the nearest G stop. Maps short_name to station_id.")

stations = station_info.merge(
    sets.all_stations[["short_name", "miles_to_nearest_g_stop", "station_group"]], on="short_name", how="left"
)
stations["in_greenpoint_screen"] = stations["station_id"].isin(greenpoint_ids)
save_pandas(stations, "stations",
            "Citi Bike stations from the latest snapshot: GBFS station_id, trip code short_name, location, capacity, study group.")

panel_extended = tf.build_extended_panel(trip_hourly, model_stations, weather_all, shutdown_events,
                                         event_station_exposure, panel)
check("panel", "Extended panel rows", f"{len(panel_extended):,} ({panel_extended['short_name'].nunique()} stations)")
check("panel", "Extended panel rows in 2024 / 2026",
      f"{int(panel_extended['period'].eq('2024').sum()):,} / {int(panel_extended['period'].eq('2026').sum()):,}")
check("panel", "Extended panel rows missing weather", int(panel_extended["temperature_2m"].isna().sum()))
check("panel", "Extended panel closure station hours", int(panel_extended["g_closure_hour"].sum()))
check("panel", "Extended panel rows outside a station active span", int((~panel_extended["station_active"]).sum()))
for split, rows in panel_extended["split"].value_counts().sort_index().items():
    check("panel", f"Extended panel split {split}", f"{rows:,}")
save_pandas(panel_extended, "analysis_station_hour_panel_extended",
            "Main modeling table: station by hour for May to Sep 2024 and Jan to Sep 2026 with outcomes, weather, "
            "calendar, shutdown flags, baseline, profile and split. Filter station_active = true before modeling.")

# COMMAND ----------

baseline_accuracy = tf.baseline_accuracy(panel_extended)
save_pandas(baseline_accuracy, "baseline_accuracy_by_month",
            "Mean absolute error and Poisson deviance of the four week baseline by month and station group.")
display(baseline_accuracy.round(3))

event_effect_table = tf.shutdown_effect_screening(panel_extended)
save_pandas(event_effect_table, "shutdown_effect_screening",
            "Actual to normal profile demand at exposed and control stations for each shutdown type and the placebo weekends.")
display(event_effect_table)

# COMMAND ----------

quality_frame = spark.createDataFrame(
    [(section, name, result, RUN_AT) for section, name, result in quality],
    "section string, check string, result string, run_at timestamp",
)
save_spark(quality_frame, "data_quality_checks", "Data quality checks recorded by the latest citibike_prepare run.")
display(quality_frame)
