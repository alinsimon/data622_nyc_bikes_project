# Data sources

The project stores downloaded collected data in this folder.

## Citi Bike trip history

The proposal notebook contains a `fetch-data` step that downloads the January
and September 2026 archives and saves live GBFS station status and station
information JSON files. The separate analysis notebook has active loading
cells for the extracted archives and local historical status files.

Existing station status and station information JSON files are reused without
downloading again. To fetch a fresh live snapshot, remove the corresponding
JSON file before running the proposal notebook's fetch step.

- [January 2026 trip archive](https://s3.amazonaws.com/tripdata/202601-citibike-tripdata.zip)
- [August 2026 trip archive](https://s3.amazonaws.com/tripdata/202608-citibike-tripdata.zip), downloaded once by the `download-trip-data` notebook cell and skipped when already present
- [September 2026 trip archive](https://s3.amazonaws.com/tripdata/202609-citibike-tripdata.zip)
- [Citi Bike system data page](https://citibikenyc.com/system-data)

The January archive is about 338 MB; September is about 927 MiB. The same step
unzips them into `datasource/202601-citibike-tripdata/` and
`datasource/202609-citibike-tripdata/`, respectively. Existing archives and
extracted files are reused. The trip glimpse still reads January's first CSV.
September contains the full month's trip records, including September 14â€“20.
It does not replace the historical station-status Parquet dataset.

## MTA service alerts

The analysis notebook retrieves New York City Transit Subway alert updates for
2026 from the NYC Open Data API and caches the result as
`mta_subway_alerts_2026.csv` (a copy is also shared on Google Drive). The public history currently ends on August 11,
2026, so it does not overlap the local station status period beginning August
21. Alert rows describe notices; their publication date is not necessarily the
start or end of a service interruption.

- [MTA service alerts API](https://data.ny.gov/resource/7kct-peq7.json)

The notebook reads G train stop names and coordinates from the local file
`MTA_Subway_Stations_20261003.csv` (496 stops, 21 with G service), exported
from the MTA subway stations dataset:

- [MTA subway stations](https://data.ny.gov/resource/39hk-dx4f)

The notebook also reads the live MTA GTFS realtime subway alert feed as a
snapshot (`mta_live_alerts_snapshot.json`) and parses G train alerts into one
row per active period with affected stop ids and start and end times
(`mta_g_train_service_events.csv`). The snapshot taken on October 25, 2026
contains 28 G event periods from October 19 to November 23, 2026, so none
overlap the August 21 to October 1 station status period. A haversine table of
every Greenpoint station to every G stop, with a quarter mile flag, is saved as
`greenpoint_station_g_stop_exposure.csv`. Monthly G line delay causing incident
counts from NY Open Data (`g937-7k7c`) are saved as
`mta_g_delay_incidents_monthly.csv`. The live feed only holds current and
future work, so closure hours inside the status window must come from an archive
kept by daily collection or a hand built calendar.

The notebook also caches G train alerts from the older
[MTA Service Alerts 2012 to 2020](https://data.ny.gov/resource/3h5b-5ktz.json)
archive as `mta_g_alerts_2012_2020.csv`. It ends on March 31, 2020, uses the
agency label `Subway`, and does not overlap the 2026 data.

Because no alert source covers the status period, the notebook also pulls hourly
ridership for 21 G station complexes from the
[MTA subway hourly ridership](https://data.ny.gov/resource/5wq4-mkjj) dataset
(`mta_g_station_hourly_ridership_2026.csv`, August 1 to October 14, 2026). Hours
where Greenpoint Av and Nassau Av both fall below 35 percent of their typical
weekday and hour level are grouped into inferred G service reduction windows
(`g_service_reduction_windows_inferred.csv`). These are a proxy for closure
hours and should be checked against MTA planned work notices.

- [MTA live subway alerts](https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Fsubway-alerts.json)
- [MTA subway delay causing incidents](https://data.ny.gov/resource/g937-7k7c.json)

## Historical weather

The analysis notebook requests hourly temperature and precipitation from the
Open Meteo historical archive for January, August and September 2026, which
covers all three trip months and the full station status window.
It maps each screened Citi Bike station coordinate to the weather grid
coordinates returned by the API, following the station to grid lookup pattern
in the [CUNY SPS Citi Bike pipeline](https://github.com/cuny-sps-datascience/citibike-data-pipeline/blob/main/3-update_station_weather_mapping.ipynb).
It caches the lookup as `greenpoint_station_weather_lookup.csv` and the unique
grid cell observations as `greenpoint_weather_hourly_2026.csv`. All 43 screened
stations currently map to one grid cell, so these data do not represent
station level weather variation.

- [Open Meteo historical weather API](https://open-meteo.com/en/docs/historical-weather-api)

The weather API files are reused after the first request. To refresh a source,
remove only its corresponding cached file before rerunning the API collection
cell. The MTA service history is also a historical archive; the live
feed must be archived on a schedule to build a history.

## Historical Citi Bike station data

The station analysis uses historical station status data obtained from BigQuery
and saved locally as Parquet files, along with station information records.
These files are not included in Git and must be available locally at:

- `datasource/station_status/`
- `datasource/station_info.parquet`

The historical files contain fields such as fetch time, station identifier,
available bikes, and open docks. They are an NYC export partitioned by
`nyc_date` (2026-08-21 to 2026-10-01). The related New York BigQuery tables are
[`bigquery-public-data.new_york_citibike.citibike_stations`](https://console.cloud.google.com/bigquery?project=eliot-463900&ws=!1m6!1m5!4m3!1sbigquery-public-data!2snew_york_citibike!3scitibike_stations!23sRESOURCE_LIST) and
[`bigquery-public-data.new_york_citibike.citibike_trips`](https://console.cloud.google.com/bigquery?project=eliot-463900&ws=!1m6!1m5!4m3!1sbigquery-public-data!2snew_york_citibike!3scitibike_trips!23sRESOURCE_LIST).
These links are references only. The data is cached locally in this folder and
the notebook never queries BigQuery, which avoids the cost of repeated calls.
Those two tables hold station records and trips; no BigQuery table was supplied
for station status itself, so the provenance comes from the Parquet metadata: a GBFS status poller (version 1.2, run manually) exported by a Spark job on October 1, 2026, partitioned by `nyc_date` from August 21 to October 1, 2026. The original query is not recorded. The current Citi Bike GBFS station
status feed provides live status, not the historical snapshots used here.

## Analysis outputs

All tables generated by the notebook are written to `datasource/processed/`. The raw downloads and API caches stay in `datasource/`.

- analysis_station_hour_panel.parquet: station by hour panel (trips, status, weather, exposure, closure flags).
- greenpoint_station_exposure.csv: station distance to the nearest affected G stop and exposure band.
- g_service_closures_confirmed.csv: G closure windows from MTA Weekender notices (no G trains between Court Sq and Bedford-Nostrand Avs, 22:00 Friday to 04:00 Monday): [Aug 7 to 10](https://www.mta.info/article/mta-weekender-august-7-10-2026), [Aug 21 to 24](https://www.mta.info/article/mta-weekender-august-21-24-2026), [Sep 11 to 14](https://www.mta.info/article/mta-weekender-september-11-14-2026), [Sep 25 to 28](https://www.mta.info/article/mta-weekender-september-25-28-2026). The Labor Day weekend notice lists no G work.
- g_service_reduction_windows_inferred.csv: closure windows inferred from G ridership, used only as a check on the confirmed windows.

## Extended trip months and weather (downloaded once)

These raw files are intentionally not stored in Git because the trip archives total
about 9 GB. Download them directly from Citi Bike's public S3 trip-data archive:

| Month | Direct download |
|---|---|
| May 2024 | [202405-citibike-tripdata.zip](https://s3.amazonaws.com/tripdata/202405-citibike-tripdata.zip) |
| June 2024 | [202406-citibike-tripdata.zip](https://s3.amazonaws.com/tripdata/202406-citibike-tripdata.zip) |
| July 2024 | [202407-citibike-tripdata.zip](https://s3.amazonaws.com/tripdata/202407-citibike-tripdata.zip) |
| August 2024 | [202408-citibike-tripdata.zip](https://s3.amazonaws.com/tripdata/202408-citibike-tripdata.zip) |
| September 2024 | [202409-citibike-tripdata.zip](https://s3.amazonaws.com/tripdata/202409-citibike-tripdata.zip) |
| February 2026 | [202602-citibike-tripdata.zip](https://s3.amazonaws.com/tripdata/202602-citibike-tripdata.zip) |
| March 2026 | [202603-citibike-tripdata.zip](https://s3.amazonaws.com/tripdata/202603-citibike-tripdata.zip) |
| April 2026 | [202604-citibike-tripdata.zip](https://s3.amazonaws.com/tripdata/202604-citibike-tripdata.zip) |
| May 2026 | [202605-citibike-tripdata.zip](https://s3.amazonaws.com/tripdata/202605-citibike-tripdata.zip) |
| June 2026 | [202606-citibike-tripdata.zip](https://s3.amazonaws.com/tripdata/202606-citibike-tripdata.zip) |
| July 2026 | [202607-citibike-tripdata.zip](https://s3.amazonaws.com/tripdata/202607-citibike-tripdata.zip) |

The archive URL pattern is
`https://s3.amazonaws.com/tripdata/YYYYMM-citibike-tripdata.zip`. To fetch and
extract all required months automatically, run the `download-trip-data` cell in
[`Citi Bike Demand Analysis.ipynb`](../Citi%20Bike%20Demand%20Analysis.ipynb)
from the project root. It saves each ZIP as
`datasource/YYYYMM-citibike-tripdata.zip` and extracts its CSV files into
`datasource/YYYYMM-citibike-tripdata/`. Existing valid ZIPs are reused, so the
cell skips downloading them again. You can also download an individual archive
using the links above and place it in `datasource/`; the cell will extract it.

The hourly weather file, `greenpoint_weather_hourly_extended.csv`, is a small
cached result rather than a published static download. It is fetched from the
[Open-Meteo Historical Weather API](https://archive-api.open-meteo.com/v1/archive)
for the Greenpoint grid cell, hourly `temperature_2m` and `precipitation`, in
`America/New_York` time, for May 1 to September 30, 2024 and February 1 to July
31, 2026. The analysis notebook creates it on the first run; the API's
[documentation](https://open-meteo.com/en/docs/historical-weather-api) describes
the source and request parameters.

`g_service_closures_confirmed_pre_august.csv` is a small generated file, not a
separately hosted download. Regenerate it by running the
`confirmed-g-closures-pre-august` cell in
[`Citi Bike Demand Analysis.ipynb`](../Citi%20Bike%20Demand%20Analysis.ipynb).
It has one row per weekend, with local start/end timestamps, the source notice
URL, and a `times_assumed` flag. The notice pages are the underlying sources:

| Weekend | MTA Weekender notice |
|---|---|
| Feb 27â€“Mar 2, 2026 | [Feb 27â€“Mar 2 notice](https://www.mta.info/article/mta-weekender-february-27-march-2-2026) |
| Mar 6â€“9, 2026 | [Mar 6â€“9 notice](https://www.mta.info/article/mta-weekender-march-6-9-2026) |
| Mar 13â€“16, 2026 | [Mar 13â€“16 notice](https://www.mta.info/article/mta-weekender-march-13-16-2026) |
| Mar 20â€“23, 2026 | [Mar 20â€“23 notice](https://www.mta.info/article/mta-weekender-march-20-23-2026) |
| Mar 27â€“30, 2026 | [Mar 27â€“30 notice](https://www.mta.info/article/mta-weekender-march-27-30-2026) |
| Apr 17â€“20, 2026 | [Apr 17â€“20 notice](https://www.mta.info/article/mta-weekender-april-17-20-2026) |
| Jun 5â€“8, 2026 | [Jun 5â€“8 notice](https://www.mta.info/article/mta-weekender-june-5-8-2026) |
| Jun 19â€“22, 2026 | [Jun 19â€“22 notice](https://www.mta.info/article/mta-weekender-june-19-22-2026) |
| Jul 10â€“13, 2026 | [Jul 10â€“13 notice](https://www.mta.info/article/mta-weekender-july-10-13-2026) |

Only the Feb 27 notice was read in full to verify the G closure times. The
notebook assigns 21:45 Friday to 05:00 Monday to the other eight weekends, which
are marked `times_assumed = True`; treat their timestamps as estimates rather
than verified notice details. 2024 closure dates still need a source.

## Modeling datasets (Section 2.6)

These files are in `datasource/processed/`.

Built by the notebook from the trip months above and cached here. All files are gitignored and can be rebuilt by running the notebook.

| File | Content |
|---|---|
| `mta_subway_shutdowns.csv` | One row per shutdown event and affected G stop (13 confirmed 2026 weekends and the three 2024 phases), with coordinates, window, and the Citi Bike stations within 0.25 mile |
| `model_station_sets.csv` | 94 corridor stations and 60 control stations (seed 622, more than 3 miles from any G stop) |
| `trip_hourly_model_stations.parquet` | Hourly departures and arrivals per model station, May to September 2024 and January to September 2026 |
| `analysis_station_hour_panel_extended.parquet` | Station hour panel with weather, calendar, shutdown and placebo flags, four week baseline, normal profile, chronological split and status derived proxy |
| `baseline_accuracy_by_month.csv` | MAE and Poisson deviance of the baseline by month and station group |
| `shutdown_effect_screening.csv` | Actual to profile ratios at exposed and control stations by event type |

Limits: the 2024 phase dates come from the MTA notice text and use whole days; 3 of the 16 closure weekends named in the proposal were not found in MTA notices; eight pre August 2026 weekends use assumed times.
