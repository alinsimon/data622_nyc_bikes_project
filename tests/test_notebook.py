import pandas as pd

from citibike.notebook import connect


class FakeFrame:
    def __init__(self, data):
        self.data = data

    def select(self, *columns):
        return FakeFrame(self.data[list(columns)])

    def where(self, condition):
        return self

    def limit(self, count):
        return FakeFrame(self.data.head(count))

    def toPandas(self):
        return self.data.copy()


class FakeSpark:
    def __init__(self):
        self.tables = []

    def table(self, name):
        self.tables.append(name)
        return FakeFrame(pd.DataFrame({"short_name": ["a"], "departures": [1]}))


def test_connect_reads_tables_with_the_resolved_config(monkeypatch):
    monkeypatch.setenv("CITIBIKE_SCHEMA", "scratch")
    spark = FakeSpark()
    config, session, read_table = connect(spark=spark)
    assert session is spark and config.schema == "scratch"
    frame = read_table("trips_station_hourly", columns=["short_name"])
    assert list(frame.columns) == ["short_name"]
    assert spark.tables == ["`workspace`.`scratch`.`trips_station_hourly`"]
