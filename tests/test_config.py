import pytest

from citibike.config import TABLES, ProjectConfig, load_config


class FakeWidgets:
    def __init__(self, values):
        self.values = values
        self.created = {}

    def text(self, name, default, label):
        self.created[name] = default

    def get(self, name):
        return self.values.get(name, self.created.get(name, ""))


class FakeDbutils:
    def __init__(self, values):
        self.widgets = FakeWidgets(values)


def test_defaults(monkeypatch):
    for variable in ("CITIBIKE_CATALOG", "CITIBIKE_SCHEMA", "CITIBIKE_VOLUME"):
        monkeypatch.delenv(variable, raising=False)
    config = load_config()
    assert (config.catalog, config.schema, config.volume) == ("workspace", "citibike", "raw")
    assert config.raw_data_dir == "/Volumes/workspace/citibike/raw/datasource"


def test_precedence_env_then_widgets_then_overrides(monkeypatch):
    monkeypatch.setenv("CITIBIKE_CATALOG", "env_catalog")
    monkeypatch.setenv("CITIBIKE_SCHEMA", "env_schema")
    dbutils = FakeDbutils({"schema": "widget_schema"})
    config = load_config(dbutils, volume="override_volume")
    assert config.catalog == "env_catalog"
    assert config.schema == "widget_schema"
    assert config.volume == "override_volume"
    # the widgets are created with the resolved defaults
    assert dbutils.widgets.created["catalog"] == "env_catalog"


def test_table_names_are_quoted_and_checked():
    config = ProjectConfig()
    assert config.table("trips") == "`workspace`.`citibike`.`trips`"
    with pytest.raises(KeyError):
        config.table("trip")


def test_invalid_identifiers_are_rejected():
    with pytest.raises(ValueError):
        ProjectConfig(schema="citibike; DROP TABLE x")


def test_with_schema_keeps_catalog_and_volume():
    config = ProjectConfig().with_schema("scratch")
    assert config.table("stations") == "`workspace`.`scratch`.`stations`"
    assert config.raw_data_dir == "/Volumes/workspace/scratch/raw/datasource"


def test_every_table_name_is_a_valid_identifier():
    for name in TABLES:
        ProjectConfig().table(name)
