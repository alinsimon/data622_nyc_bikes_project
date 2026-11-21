"""Where the project's data lives in Databricks.

Every notebook and job gets the catalog, schema and volume from ``load_config`` instead of hard-coding them.
Values are resolved in this order (later wins):

1. defaults: ``workspace`` / ``citibike`` / ``raw`` (the Free Edition setup used by ``databricks.yml``)
2. environment variables ``CITIBIKE_CATALOG``, ``CITIBIKE_SCHEMA``, ``CITIBIKE_VOLUME`` (handy for local runs)
3. notebook widgets or job parameters named ``catalog``, ``schema``, ``volume`` (when ``dbutils`` is passed)
4. keyword overrides
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, replace

DEFAULTS = {"catalog": "workspace", "schema": "citibike", "volume": "raw"}
ENV_VARS = {"catalog": "CITIBIKE_CATALOG", "schema": "CITIBIKE_SCHEMA", "volume": "CITIBIKE_VOLUME"}

# Tables written by jobs/citibike_prepare.py. Reading anything else through ``ProjectConfig.table`` is an error,
# which catches typos before a query is sent.
RAW_TABLES = frozenset({
    "raw_station_info",
    "raw_mta_subway_stations",
    "raw_mta_subway_alerts_2026",
    "raw_mta_g_alerts_2012_2020",
    "raw_mta_g_delay_incidents_monthly",
    "raw_mta_g_station_hourly_ridership_2026",
    "raw_mta_live_alerts_snapshot",
    "raw_greenpoint_station_weather_lookup",
    "raw_greenpoint_weather_hourly_2026",
    "raw_greenpoint_weather_hourly_extended",
})
CLEAN_TABLES = frozenset({
    "trips",
    "trips_ingest_log",
    "trips_station_hourly",
    "station_status_hourly",
    "stations",
    "weather_hourly",
    "mta_g_stops",
    "g_service_closures_confirmed",
    "g_service_closures_confirmed_pre_august",
    "g_service_reduction_windows_inferred",
    "greenpoint_station_exposure",
    "model_station_sets",
    "mta_subway_shutdowns",
    "analysis_station_hour_panel",
    "analysis_station_hour_panel_extended",
    "baseline_accuracy_by_month",
    "shutdown_effect_screening",
    "data_quality_checks",
})
TABLES = RAW_TABLES | CLEAN_TABLES

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _check_identifier(kind: str, value: str) -> str:
    value = value.strip()
    if not _IDENTIFIER.match(value):
        raise ValueError(f"Invalid {kind} name {value!r}: use letters, digits and underscores only.")
    return value


@dataclass(frozen=True)
class ProjectConfig:
    catalog: str = DEFAULTS["catalog"]
    schema: str = DEFAULTS["schema"]
    volume: str = DEFAULTS["volume"]

    def __post_init__(self):
        for kind in ("catalog", "schema", "volume"):
            object.__setattr__(self, kind, _check_identifier(kind, getattr(self, kind)))

    def table(self, name: str) -> str:
        """Fully qualified, quoted name of a project table, e.g. ```workspace`.`citibike`.`trips```."""
        if name not in TABLES:
            raise KeyError(f"Unknown table {name!r}. Known tables: {', '.join(sorted(TABLES))}")
        return f"`{self.catalog}`.`{self.schema}`.`{name}`"

    @property
    def volume_root(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume}"

    @property
    def raw_data_dir(self) -> str:
        """Folder in the volume where the download jobs store the raw files."""
        return f"{self.volume_root}/datasource"

    def with_schema(self, schema: str) -> "ProjectConfig":
        return replace(self, schema=schema)


def load_config(dbutils=None, **overrides) -> ProjectConfig:
    """Resolve the project configuration (see the module docstring for the order)."""
    values = dict(DEFAULTS)
    for key, variable in ENV_VARS.items():
        if os.environ.get(variable):
            values[key] = os.environ[variable]
    if dbutils is not None:
        for key in DEFAULTS:
            try:
                dbutils.widgets.text(key, values[key], key.capitalize())
                values[key] = dbutils.widgets.get(key) or values[key]
            except Exception:  # widgets are not available in every context (e.g. Databricks Connect)
                pass
    values.update({key: value for key, value in overrides.items() if value})
    unknown = set(values) - set(DEFAULTS)
    if unknown:
        raise TypeError(f"Unknown configuration keys: {sorted(unknown)}")
    return ProjectConfig(**values)
