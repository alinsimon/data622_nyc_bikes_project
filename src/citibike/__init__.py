"""Shared code for the Citi Bike and G train disruption project.

* ``config``          catalog, schema, volume and table names, in one place
* ``data_access``     Spark session and readers for the project's Delta tables
* ``study``           study definitions: screening area, closure notices, periods, thresholds
* ``transformations`` pandas functions that build the analysis tables (used by the prepare job and the notebook)
"""

from citibike.config import ProjectConfig, load_config

__all__ = ["ProjectConfig", "load_config"]
