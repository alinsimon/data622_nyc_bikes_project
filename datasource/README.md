# Data sources

The notebook stores downloaded and locally collected data in this folder. Raw
data files are intentionally excluded from Git because the public archives and
the collected station-status data are large.

## Citi Bike trip history

The notebook's `fetch-data` step downloads the January 2026 archive on first
use and saves it in this folder, along with the live GBFS station-status and
station-information JSON files:

- [January 2026 trip archive](https://s3.amazonaws.com/tripdata/202601-citibike-tripdata.zip)
- [Citi Bike system data page](https://citibikenyc.com/system-data)

The archive is about 338 MB. The notebook reads the first CSV directly from the
ZIP file and does not need the CSVs to be extracted.

## MTA service alerts

The notebook queries the MTA service-alert dataset from the NYC Open Data API:

- [MTA service alerts API](https://data.ny.gov/resource/7kct-peq7.csv)

## Historical Citi Bike station data

The notebook's station-status analysis uses the project's historical snapshots
and station reference data. These are not included in Git and must be available
locally at:

- `datasource/station_status/station_status/`
- `datasource/station_info.parquet`

The current Citi Bike GBFS station-status feed provides live station status,
not the historical snapshots used by this analysis.
