"""Setup shared by the project notebooks (data preparation and analysis)."""

from __future__ import annotations

from collections import namedtuple

from citibike import data_access
from citibike.config import load_config

NotebookContext = namedtuple("NotebookContext", "config spark read_table")


def connect(dbutils=None, spark=None) -> NotebookContext:
    """Resolve catalog, schema and volume, open the Spark session and return a ``read_table`` helper.

    ``dbutils`` is the notebook's ``dbutils`` in Databricks (for the widgets) and ``None`` elsewhere.
    ``read_table(name, columns=None, where=None, max_rows=...)`` loads a project table as pandas; the column selection
    and the filter run in Databricks before any rows are transferred.
    """
    config = load_config(dbutils)
    spark = spark or data_access.get_spark()

    def read_table(name, columns=None, where=None, max_rows=data_access.DEFAULT_MAX_ROWS):
        return data_access.read_pandas(name, columns=columns, where=where, max_rows=max_rows, spark=spark,
                                       config=config)

    return NotebookContext(config, spark, read_table)
