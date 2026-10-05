# Data sources

The notebook stores downloaded and locally collected data in this folder. Raw
data files are intentionally excluded from Git because the public archives and
the collected station-status data are large.

## Citi Bike trip history

The notebook's `fetch-data` step downloads the January and September 2026 archives on first
use and saves it in this folder, along with the live GBFS station-status and
station-information JSON files:

- [January 2026 trip archive](https://s3.amazonaws.com/tripdata/202601-citibike-tripdata.zip)
- [September 2026 trip archive](https://s3.amazonaws.com/tripdata/202609-citibike-tripdata.zip)
- [Citi Bike system data page](https://citibikenyc.com/system-data)

The January archive is about 338 MB; September is about 927 MiB. The same step
unzips them into `datasource/202601-citibike-tripdata/` and
`datasource/202609-citibike-tripdata/`, respectively. Existing archives and
extracted files are reused. The trip glimpse still reads January's first CSV.
September contains the full month's trip records, including September 14–20.
It does not replace the historical station-status Parquet dataset.

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
