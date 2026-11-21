"""Read the project's Delta tables, in Databricks or from a local editor.

* Inside Databricks (notebooks and jobs) the notebook's own Spark session is used.
* Anywhere else, Databricks Connect runs the Spark side on serverless compute in the workspace; the local
  machine only receives the rows that are converted to pandas. Authentication comes from the Databricks CLI
  profile (``databricks auth login``) or the usual ``DATABRICKS_*`` environment variables; no secrets are
  stored in this repository.

Prefer ``read_table`` (Spark) for large data and filter before converting. ``read_pandas`` refuses to pull more
than ``max_rows`` rows so a missing filter cannot silently download a whole table.
"""

from __future__ import annotations

import os

import pandas as pd

from citibike.config import ProjectConfig, load_config

DEFAULT_MAX_ROWS = 3_000_000


def running_in_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def get_spark():
    """Return a Spark session: the active one in Databricks, or a Databricks Connect session elsewhere."""
    try:
        from pyspark.sql import SparkSession

        active = SparkSession.getActiveSession()
        if active is not None:
            return active
        if running_in_databricks():
            return SparkSession.builder.getOrCreate()
    except ImportError:
        pass
    try:
        from databricks.connect import DatabricksSession
    except ImportError as error:
        raise RuntimeError(
            "Spark is not available. Outside Databricks install the local extras "
            "(pip install -e \".[local]\") and log in with `databricks auth login`."
        ) from error
    builder = DatabricksSession.builder
    if not os.environ.get("DATABRICKS_CLUSTER_ID"):
        builder = builder.serverless(True)
    return builder.getOrCreate()


def read_table(name, columns=None, where=None, *, spark=None, config: ProjectConfig | None = None):
    """Spark DataFrame for a project table, optionally restricted to ``columns`` and a SQL ``where`` filter."""
    config = config or load_config()
    spark = spark or get_spark()
    frame = spark.table(config.table(name))
    if columns:
        frame = frame.select(*columns)
    if where:
        frame = frame.where(where)
    return frame


def to_pandas(frame, max_rows: int | None = DEFAULT_MAX_ROWS, label: str = "query") -> pd.DataFrame:
    """Convert a Spark DataFrame to pandas, refusing results larger than ``max_rows``."""
    if max_rows is not None:
        frame = frame.limit(max_rows + 1)
    result = frame.toPandas()
    if max_rows is not None and len(result) > max_rows:
        raise ValueError(
            f"{label} returned more than {max_rows:,} rows. Filter it with `columns`/`where`, aggregate in Spark, "
            f"or pass a larger max_rows if the full table is really needed."
        )
    return normalize_timestamps(result)


def read_pandas(name, columns=None, where=None, *, max_rows: int | None = DEFAULT_MAX_ROWS, spark=None,
                config: ProjectConfig | None = None) -> pd.DataFrame:
    """pandas DataFrame for a project table (see ``read_table`` for the arguments)."""
    frame = read_table(name, columns, where, spark=spark, config=config)
    return to_pandas(frame, max_rows=max_rows, label=name)


def hourly_station_flows(trip_months, station_codes=None, *, spark=None, config: ProjectConfig | None = None,
                         max_rows: int | None = DEFAULT_MAX_ROWS) -> pd.DataFrame:
    """Departures and arrivals per station code and clock hour, counted in Spark from the cleaned ``trips`` table.

    ``trip_months`` are trip file months ("YYYY-MM"), so trips are attributed to the monthly file they came from,
    as when the CSV files were read directly (arrivals early in the next month are included). ``station_codes``
    restricts the result to those trip station codes (``short_name``).
    """
    from pyspark.sql import functions as F

    trips = read_table("trips", spark=spark, config=config).where(F.col("trip_month").isin(list(trip_months)))
    codes = None if station_codes is None else [str(code) for code in station_codes]

    def flow(station_column, time_column, name):
        frame = trips.where(F.col(station_column).isNotNull())
        if codes is not None:
            frame = frame.where(F.col(station_column).isin(codes))
        return frame.groupBy(
            F.col(station_column).alias("short_name"),
            # formatting a TIMESTAMP_NTZ never applies a time zone, so the hour is the recorded New York clock hour
            F.date_format(time_column, "yyyy-MM-dd HH:00:00").alias("hour"),
        ).agg(F.count(F.lit(1)).alias(name))

    joined = (
        flow("start_station_id", "started_at", "departures")
        .join(flow("end_station_id", "ended_at", "arrivals"), ["short_name", "hour"], "full")
        .fillna(0, ["departures", "arrivals"])
    )
    result = to_pandas(joined, max_rows=max_rows, label="hourly station flows")
    result["hour"] = pd.to_datetime(result["hour"]).astype("datetime64[ns]")
    result[["departures", "arrivals"]] = result[["departures", "arrivals"]].astype("int64")
    result["short_name"] = result["short_name"].astype(str)
    return result.sort_values(["short_name", "hour"]).reset_index(drop=True)


def trip_station_codes(trip_months, *, spark=None, config: ProjectConfig | None = None) -> set:
    """Distinct start and end station codes in the given trip file months."""
    from pyspark.sql import functions as F

    trips = read_table("trips", spark=spark, config=config).where(F.col("trip_month").isin(list(trip_months)))
    codes = (
        trips.select(F.col("start_station_id").alias("code"))
        .union(trips.select(F.col("end_station_id").alias("code")))
        .where(F.col("code").isNotNull()).distinct()
    )
    return set(to_pandas(codes, max_rows=None, label="station codes")["code"])


def normalize_timestamps(frame: pd.DataFrame) -> pd.DataFrame:
    """Use nanosecond, time zone naive timestamps, as the analysis code expects (New York clock times)."""
    for column in frame.columns:
        dtype = frame[column].dtype
        if isinstance(dtype, pd.DatetimeTZDtype):
            frame[column] = frame[column].dt.tz_localize(None)
        if pd.api.types.is_datetime64_any_dtype(frame[column].dtype):
            frame[column] = frame[column].astype("datetime64[ns]")
    return frame
