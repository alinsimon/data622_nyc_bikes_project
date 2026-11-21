import pandas as pd
import pytest

from citibike import data_access
from citibike.config import ProjectConfig


class FakeFrame:
    """Minimal stand-in for a Spark DataFrame that records the calls made on it."""

    def __init__(self, data, calls):
        self.data = data
        self.calls = calls

    def select(self, *columns):
        self.calls.append(("select", columns))
        return FakeFrame(self.data[list(columns)], self.calls)

    def where(self, condition):
        self.calls.append(("where", condition))
        return self

    def limit(self, count):
        self.calls.append(("limit", count))
        return FakeFrame(self.data.head(count), self.calls)

    def toPandas(self):
        return self.data.copy()


class FakeSpark:
    def __init__(self, data):
        self.data = data
        self.calls = []

    def table(self, name):
        self.calls.append(("table", name))
        return FakeFrame(self.data, self.calls)


@pytest.fixture
def spark():
    return FakeSpark(pd.DataFrame({
        "short_name": ["a", "b", "c"],
        "hour": pd.to_datetime(["2026-08-01 00:00", "2026-08-01 01:00", "2026-08-01 02:00"]).tz_localize("UTC"),
        "departures": [1, 2, 3],
    }))


def test_read_pandas_uses_the_configured_table_and_filters(spark):
    config = ProjectConfig(schema="scratch")
    result = data_access.read_pandas("trips_station_hourly", columns=["short_name", "hour"], where="departures > 1",
                                     spark=spark, config=config)
    assert ("table", "`workspace`.`scratch`.`trips_station_hourly`") in spark.calls
    assert ("where", "departures > 1") in spark.calls
    assert list(result.columns) == ["short_name", "hour"]


def test_read_pandas_returns_naive_nanosecond_timestamps(spark):
    result = data_access.read_pandas("trips_station_hourly", spark=spark, config=ProjectConfig())
    assert result["hour"].dtype == "datetime64[ns]"
    assert result["hour"].iloc[0] == pd.Timestamp("2026-08-01 00:00")


def test_read_pandas_refuses_large_results(spark):
    with pytest.raises(ValueError, match="more than 2 rows"):
        data_access.read_pandas("trips_station_hourly", max_rows=2, spark=spark, config=ProjectConfig())
    assert ("limit", 3) in spark.calls


def test_read_pandas_without_limit(spark):
    result = data_access.read_pandas("trips_station_hourly", max_rows=None, spark=spark, config=ProjectConfig())
    assert len(result) == 3
    assert not any(call[0] == "limit" for call in spark.calls)


def test_unknown_table_fails_before_any_query(spark):
    with pytest.raises(KeyError):
        data_access.read_table("not_a_table", spark=spark, config=ProjectConfig())
    assert spark.calls == []
