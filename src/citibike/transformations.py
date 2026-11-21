"""pandas functions that turn cleaned inputs into the analysis tables.

They reproduce sections 2.1 to 2.6 of "Citi Bike Demand Analysis" step by step and are used both by
``jobs/citibike_prepare.py`` (which saves the results as Delta tables) and by the analysis notebook (for the
small lookups it rebuilds on the fly). None of them reads or writes files.
"""

from __future__ import annotations

from collections import namedtuple

import numpy as np
import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar

from citibike import study

STATION_INFO_COLUMNS = ["station_id", "short_name", "name", "lat", "lon", "capacity", "snapshot_date"]
STATUS_COLUMNS = ["bikes_available_mean", "docks_available_mean", "zero_bike_snapshots", "zero_dock_snapshots",
                  "snapshot_count"]
SUBWAY_STATION_RENAMES = {
    "GTFS Stop ID": "gtfs_stop_id", "Station ID": "station_id", "Complex ID": "complex_id",
    "Line": "line", "Stop Name": "stop_name", "Borough": "borough",
    "Daytime Routes": "daytime_routes", "GTFS Latitude": "gtfs_latitude",
    "GTFS Longitude": "gtfs_longitude",
}

StationSets = namedtuple("StationSets", "all_stations control_pool model_stations model_pairs g_only_stops")
RidershipCheck = namedtuple("RidershipCheck", "windows ridership_wide ridership_ratio")


def haversine_km(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(np.radians, (lat1, lon1, lat2, lon2))
    a = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    return 6371.0088 * 2 * np.arcsin(np.sqrt(a))


def haversine_miles(lat1, lon1, lat2, lon2):
    return haversine_km(lat1, lon1, lat2, lon2) / study.KM_PER_MILE


# ----------------------------------------------------------------------------------------------------------------
# Stations and G stops
# ----------------------------------------------------------------------------------------------------------------

def latest_station_snapshot(station_info_all: pd.DataFrame):
    """Rows of the latest station information snapshot, typed as in the analysis. Returns (frame, snapshot date)."""
    latest = station_info_all["snapshot_date"].max()
    station_info = station_info_all.loc[station_info_all["snapshot_date"].eq(latest), STATION_INFO_COLUMNS].copy()
    station_info["station_id"] = station_info["station_id"].astype("string")
    station_info["short_name"] = station_info["short_name"].astype("string")
    station_info["capacity"] = pd.to_numeric(station_info["capacity"], errors="coerce")
    return station_info, latest


def greenpoint_screen(station_info: pd.DataFrame) -> pd.DataFrame:
    """Stations with capacity inside the Greenpoint screening rectangle, with the trip station code."""
    stations = station_info.loc[
        station_info["lat"].between(*study.GREENPOINT_LATITUDE)
        & station_info["lon"].between(*study.GREENPOINT_LONGITUDE)
        & station_info["capacity"].gt(0)
    ].copy()
    if stations.empty:
        raise ValueError("The Greenpoint screening rectangle returned no active stations.")
    stations["trip_station_id"] = stations["short_name"].astype("string")
    return stations


def g_stops_from_subway_stations(subway_stations: pd.DataFrame) -> pd.DataFrame:
    """G train stops from the MTA subway stations file (original column names), with numeric coordinates."""
    stations = subway_stations.rename(columns=SUBWAY_STATION_RENAMES)
    for column in ("gtfs_stop_id", "station_id", "complex_id"):
        stations[column] = stations[column].astype("string")
    g_stops = stations[
        stations["daytime_routes"].fillna("").str.split().apply(lambda routes: "G" in routes)
    ].sort_values("stop_name").reset_index(drop=True)
    g_stops["gtfs_latitude"] = pd.to_numeric(g_stops["gtfs_latitude"], errors="coerce")
    g_stops["gtfs_longitude"] = pd.to_numeric(g_stops["gtfs_longitude"], errors="coerce")
    if g_stops[["gtfs_latitude", "gtfs_longitude"]].isna().any().any():
        raise ValueError("MTA G train stop records contain missing or invalid coordinates.")
    return g_stops


def affected_segment_stop_ids(g_stops: pd.DataFrame) -> pd.Series:
    """GTFS stop ids of the segment named in the MTA notices (G22 to G32)."""
    g_line = g_stops[g_stops["gtfs_stop_id"].str.startswith("G")].copy()
    g_line["stop_number"] = g_line["gtfs_stop_id"].str[1:].astype(int)
    return g_line.loc[g_line["stop_number"].between(*study.AFFECTED_SEGMENT_STOP_NUMBERS), "gtfs_stop_id"]


def greenpoint_station_exposure(greenpoint_stations, g_stops, segment_stop_ids) -> pd.DataFrame:
    """Nearest affected stop for each Greenpoint station, the quarter mile exposure flag and the distance band."""
    affected_stops = g_stops[g_stops["gtfs_stop_id"].isin(segment_stop_ids)]
    points = greenpoint_stations[["station_id", "name", "lat", "lon", "capacity"]].drop_duplicates("station_id")
    pairs = points.merge(affected_stops[["gtfs_stop_id", "stop_name", "gtfs_latitude", "gtfs_longitude"]], how="cross")
    pairs["distance_miles"] = haversine_miles(pairs["lat"], pairs["lon"], pairs["gtfs_latitude"], pairs["gtfs_longitude"])
    exposure = (
        pairs.sort_values("distance_miles")
        .drop_duplicates("station_id")
        .rename(columns={"stop_name": "nearest_affected_stop", "distance_miles": "nearest_affected_stop_miles"})
        .drop(columns=["gtfs_stop_id", "gtfs_latitude", "gtfs_longitude"])
        .reset_index(drop=True)
    )
    exposure["exposed_station"] = exposure["nearest_affected_stop_miles"] <= study.EXPOSURE_MILES
    exposure["distance_band"] = distance_band(exposure["nearest_affected_stop_miles"])
    return exposure


def distance_band(miles: pd.Series) -> pd.Series:
    return pd.cut(miles, [0, 0.25, 0.5, np.inf], labels=study.DISTANCE_BANDS, include_lowest=True)


def as_distance_band(values: pd.Series) -> pd.Series:
    """Restore the ordered categories of a distance band column read back from a table."""
    return pd.Categorical(values, categories=study.DISTANCE_BANDS, ordered=True)


def station_sets(station_info, greenpoint_stations, g_stops) -> StationSets:
    """Corridor stations (Greenpoint screen or within a quarter mile of a G stop) and a seeded control sample."""
    all_stations = station_info.loc[
        station_info["capacity"].gt(0) & station_info["short_name"].notna(),
        ["station_id", "short_name", "name", "lat", "lon", "capacity"],
    ].drop_duplicates("short_name").reset_index(drop=True)
    g_only_stops = g_stops.loc[
        g_stops["gtfs_stop_id"].str.startswith("G"), ["gtfs_stop_id", "stop_name", "gtfs_latitude", "gtfs_longitude"]
    ].copy()
    all_pairs = all_stations[["short_name", "lat", "lon"]].merge(g_only_stops, how="cross")
    all_pairs["distance_miles"] = haversine_miles(
        all_pairs["lat"], all_pairs["lon"], all_pairs["gtfs_latitude"], all_pairs["gtfs_longitude"]
    )
    all_stations["miles_to_nearest_g_stop"] = all_stations["short_name"].map(
        all_pairs.groupby("short_name")["distance_miles"].min()
    )
    in_corridor = (
        all_stations["short_name"].isin(greenpoint_stations["short_name"])
        | all_stations["miles_to_nearest_g_stop"].le(study.EXPOSURE_MILES)
    )
    control_pool = all_stations[~in_corridor & all_stations["miles_to_nearest_g_stop"].gt(study.CONTROL_MIN_MILES)]
    control_sample = control_pool.sample(study.CONTROL_SAMPLE_SIZE, random_state=study.SELECTION_SEED)
    all_stations["station_group"] = np.where(in_corridor, "corridor", None)
    all_stations.loc[control_sample.index, "station_group"] = "control"
    model_stations = all_stations[all_stations["station_group"].notna()].reset_index(drop=True)
    model_pairs = all_pairs[all_pairs["short_name"].isin(model_stations["short_name"])]
    return StationSets(all_stations, control_pool, model_stations, model_pairs, g_only_stops)


# ----------------------------------------------------------------------------------------------------------------
# G service reductions
# ----------------------------------------------------------------------------------------------------------------

def infer_reduction_windows(g_ridership: pd.DataFrame) -> RidershipCheck:
    """Windows where both detector stations keep under 35 percent of their typical ridership for 3+ hours."""
    ridership_wide = g_ridership.pivot_table(
        index="transit_timestamp", columns="station_complex", values="ridership", aggfunc="sum"
    )
    holidays = set(pd.to_datetime(study.RIDERSHIP_HOLIDAYS).date)
    hour_keys = [ridership_wide.index.dayofweek, ridership_wide.index.hour]
    ratio = pd.DataFrame({
        station: ridership_wide[station]
        / ridership_wide[station].groupby(hour_keys).transform("median").replace(0, np.nan)
        for station in study.DETECTOR_STATIONS
    })
    low = (
        (ratio.max(axis=1) < study.RIDERSHIP_THRESHOLD)
        & (ridership_wide.index.hour >= 6)
        & ~pd.Series(ridership_wide.index.date, index=ridership_wide.index).isin(holidays)
    )
    runs = (low != low.shift()).cumsum()
    raw_windows = [
        (group.index[0], group.index[-1] + pd.Timedelta(hours=1))
        for _, group in low.groupby(runs)
        if group.iloc[0] and len(group) >= 3
    ]
    merged = []
    for start, end in raw_windows:
        if merged and start - merged[-1][1] <= pd.Timedelta(hours=2):
            merged[-1] = (merged[-1][0], end)
        else:
            merged.append((start, end))
    windows = pd.DataFrame(merged, columns=["start_local", "end_local"])
    windows["hours"] = (windows["end_local"] - windows["start_local"]).dt.total_seconds() / 3600
    windows["median_ratio"] = [float(ratio.loc[s:e - pd.Timedelta(hours=1)].max(axis=1).median()) for s, e in merged]
    windows["in_status_window"] = (
        (windows["start_local"] >= study.STATUS_WINDOW[0]) & (windows["start_local"] < study.STATUS_WINDOW[1])
    )
    return RidershipCheck(windows, ridership_wide, ratio)


def _weekend_window(friday):
    friday = pd.Timestamp(friday)
    return friday + study.WEEKEND_CLOSURE_START, friday + study.WEEKEND_CLOSURE_END


def window_hours(start, end) -> pd.DatetimeIndex:
    """Whole clock hours fully inside a window, e.g. 22:00 Friday through 04:00 Monday."""
    return pd.date_range(pd.Timestamp(start).ceil("h"), pd.Timestamp(end) - pd.Timedelta(hours=1), freq="h")


def confirmed_closure_windows() -> pd.DataFrame:
    """The four August and September 2026 closure weekends, with the number of whole closure hours."""
    rows = [dict(zip(["start_local", "end_local"], _weekend_window(friday)), notice_url=url)
            for friday, url in study.MTA_WEEKENDER_NOTICES]
    windows = pd.DataFrame(rows)
    windows["closure_hours"] = [len(window_hours(r.start_local, r.end_local)) for r in windows.itertuples()]
    return windows


def pre_august_closure_windows() -> pd.DataFrame:
    rows = [dict(zip(["start_local", "end_local"], _weekend_window(friday)),
                 notice_url=f"https://www.mta.info/article/{slug}", times_assumed=assumed)
            for friday, slug, assumed in study.PRE_AUGUST_NOTICES]
    return pd.DataFrame(rows)


def closure_hour_index(windows: pd.DataFrame) -> pd.DatetimeIndex:
    return pd.DatetimeIndex(np.concatenate([
        window_hours(row.start_local, row.end_local).to_numpy() for row in windows.itertuples()
    ]))


def shutdown_events(pre_august_windows, confirmed_windows, segment_stop_ids) -> pd.DataFrame:
    """One row per shutdown event: thirteen 2026 closure weekends and the three 2024 phases."""
    rows = []
    for row in pre_august_windows.itertuples():
        rows.append(dict(event_type="2026 weekend closure", start_local=row.start_local, end_local=row.end_local,
                         stop_ids=list(segment_stop_ids), source=row.notice_url, times_assumed=row.times_assumed))
    for row in confirmed_windows.itertuples():
        rows.append(dict(event_type="2026 weekend closure", start_local=row.start_local, end_local=row.end_local,
                         stop_ids=list(segment_stop_ids), source=row.notice_url, times_assumed=False))
    for label, start, end, stops in study.PHASES_2024:
        rows.append(dict(event_type=label, start_local=pd.Timestamp(start), end_local=pd.Timestamp(end),
                         stop_ids=list(segment_stop_ids) if stops is None else list(stops),
                         source=study.PHASE_NOTICE, times_assumed=True))
    events = pd.DataFrame(rows).sort_values("start_local").reset_index(drop=True)
    events["event_id"] = [f"E{index + 1:02d}" for index in events.index]
    return events


def event_exposure(events, model_pairs, g_only_stops):
    """Per event: distance of each model station to the affected stops, and the event by stop shutdown table."""
    exposure_parts, shutdown_rows = [], []
    for event in events.itertuples():
        pairs = model_pairs[model_pairs["gtfs_stop_id"].isin(event.stop_ids)]
        nearest = pairs.groupby("short_name")["distance_miles"].min()
        exposure_parts.append(pd.DataFrame({
            "event_id": event.event_id, "short_name": nearest.index,
            "distance_to_event_miles": nearest.to_numpy(),
            "event_exposed": (nearest <= study.EXPOSURE_MILES).to_numpy(),
        }))
        for stop in g_only_stops[g_only_stops["gtfs_stop_id"].isin(event.stop_ids)].itertuples():
            near = pairs[(pairs["gtfs_stop_id"] == stop.gtfs_stop_id) & pairs["distance_miles"].le(study.EXPOSURE_MILES)]
            shutdown_rows.append(dict(
                event_id=event.event_id, event_type=event.event_type, line="G",
                gtfs_stop_id=stop.gtfs_stop_id, stop_name=stop.stop_name,
                latitude=stop.gtfs_latitude, longitude=stop.gtfs_longitude,
                start_local=event.start_local, end_local=event.end_local,
                times_assumed=event.times_assumed, source=event.source,
                citi_bike_stations_within_quarter_mile=near["short_name"].nunique(),
                citi_bike_station_ids=";".join(sorted(near["short_name"].unique())),
            ))
    return pd.concat(exposure_parts, ignore_index=True), pd.DataFrame(shutdown_rows)


# ----------------------------------------------------------------------------------------------------------------
# Panels
# ----------------------------------------------------------------------------------------------------------------

def utc_weather_to_new_york(frame: pd.DataFrame, start=None, end=None) -> pd.DataFrame:
    """Turn hourly weather requested in UTC into New York clock hours.

    Open-Meteo's ``timezone=America/New_York`` option applies a single UTC offset to the whole response (the one in
    force on the day of the request), so half of the year comes back one hour off. Requesting UTC and converting here
    gives the right clock hour on every date. On the night clocks fall back the repeated hour gets the mean temperature
    and the summed precipitation; the hour skipped when clocks spring forward gets an interpolated temperature and no
    precipitation, so the hourly series has no gaps. ``start`` and ``end`` (New York dates, inclusive) trim the result.
    """
    local = (pd.to_datetime(frame["time"]).dt.tz_localize("UTC")
             .dt.tz_convert(study.TIMEZONE).dt.tz_localize(None))
    hours = (
        frame.assign(time=local)
        .groupby("time")
        .agg(temperature_2m=("temperature_2m", "mean"),
             precipitation=("precipitation", lambda values: values.sum(min_count=1)))
    )
    grid = pd.date_range(hours.index.min(), hours.index.max(), freq="h")
    skipped = grid[grid.tz_localize(study.TIMEZONE, nonexistent="NaT", ambiguous=np.ones(len(grid), dtype=bool)).isna()]
    if len(skipped):
        hours = hours.reindex(hours.index.union(skipped))
        interpolated = hours["temperature_2m"].interpolate(limit_area="inside")
        hours.loc[skipped, "temperature_2m"] = interpolated.loc[skipped]
        hours.loc[skipped, "precipitation"] = 0.0
    hours = hours.rename_axis("time").reset_index()
    if start is not None:
        hours = hours[hours["time"] >= pd.Timestamp(start).normalize()]
    if end is not None:
        hours = hours[hours["time"] < pd.Timestamp(end).normalize() + pd.Timedelta(days=1)]
    return hours.reset_index(drop=True)


def combine_weather(greenpoint_weather, extended_weather) -> pd.DataFrame:
    return (
        pd.concat([greenpoint_weather[["time", "temperature_2m", "precipitation"]],
                   extended_weather[["time", "temperature_2m", "precipitation"]]])
        .drop_duplicates("time").rename(columns={"time": "hour"}).sort_values("hour").reset_index(drop=True)
    )


def greenpoint_panel(station_exposure, greenpoint_trips, greenpoint_status, greenpoint_weather, closure_hours):
    """Section 2.5 panel: one row per Greenpoint station and clock hour, August and September 2026."""
    panel_hours = pd.date_range(study.PANEL_START, study.PANEL_END, freq="h")
    panel = pd.MultiIndex.from_product(
        [station_exposure["station_id"].astype(str), panel_hours], names=["station_id", "hour"]
    ).to_frame(index=False)

    trip_columns = greenpoint_trips[["station_id", "hour", "departures", "arrivals"]].copy()
    trip_columns["station_id"] = trip_columns["station_id"].astype(str)
    status_columns = greenpoint_status[["station_id", "hour", *STATUS_COLUMNS]].copy()
    status_columns["station_id"] = status_columns["station_id"].astype(str)
    weather_columns = greenpoint_weather[["time", "temperature_2m", "precipitation"]].rename(columns={"time": "hour"})

    panel = (
        panel.merge(trip_columns, on=["station_id", "hour"], how="left", validate="one_to_one")
        .merge(status_columns, on=["station_id", "hour"], how="left", validate="one_to_one")
        .merge(weather_columns, on="hour", how="left", validate="many_to_one")
        .merge(
            station_exposure[["station_id", "name", "lat", "lon", "capacity", "nearest_affected_stop",
                              "nearest_affected_stop_miles", "exposed_station", "distance_band"]]
            .assign(station_id=lambda f: f["station_id"].astype(str)),
            on="station_id", how="left", validate="many_to_one",
        )
    )
    panel["trip_observed"] = panel["hour"] >= study.TRIP_COVERAGE_START
    panel["status_observed"] = panel["snapshot_count"].notna()
    for column in ("departures", "arrivals"):
        panel[column] = panel[column].where(~panel["trip_observed"], panel[column].fillna(0))
    panel["hour_of_day"] = panel["hour"].dt.hour
    panel["day_of_week"] = panel["hour"].dt.dayofweek
    panel["is_weekend"] = panel["day_of_week"] >= 5
    panel["hour_sin"] = np.sin(2 * np.pi * panel["hour_of_day"] / 24)
    panel["hour_cos"] = np.cos(2 * np.pi * panel["hour_of_day"] / 24)
    panel["g_closure_hour"] = panel["hour"].isin(closure_hours)
    panel["exposed_closure_hour"] = panel["g_closure_hour"] & panel["exposed_station"]
    panel["precipitation_any"] = panel["precipitation"] > 0
    panel["net_flow"] = panel["arrivals"] - panel["departures"]
    panel["zero_dock_share"] = panel["zero_dock_snapshots"] / panel["snapshot_count"]
    panel["zero_bike_share"] = panel["zero_bike_snapshots"] / panel["snapshot_count"]
    panel = panel.sort_values(["station_id", "hour"]).reset_index(drop=True)
    for lag in (24, 168):
        panel[f"departures_lag_{lag}h"] = panel.groupby("station_id")["departures"].shift(lag)
    panel["total_trips"] = panel["departures"] + panel["arrivals"]

    if panel.duplicated(["station_id", "hour"]).any():
        raise ValueError("The analysis panel contains duplicate station hour keys.")
    if len(panel) != len(station_exposure) * len(panel_hours):
        raise ValueError(f"Panel has {len(panel):,} rows, expected {len(station_exposure) * len(panel_hours):,}.")
    return panel


def extended_panel_base(trip_hourly, model_stations, weather_all, events, event_station_exposure, panel):
    """Section 2.6 panel before the baseline: outcomes, weather, calendar, shutdown flags, split, status, activity."""
    trip_hourly = trip_hourly.assign(short_name=trip_hourly["short_name"].astype(str))
    model_stations = model_stations.assign(short_name=model_stations["short_name"].astype(str))
    period_frames = []
    for period, start, end in study.EXTENDED_PERIODS:
        active = trip_hourly.loc[trip_hourly["hour"].between(start, end), "short_name"].unique()
        stations_in_period = model_stations[model_stations["short_name"].isin(active)]
        frame = pd.MultiIndex.from_product(
            [stations_in_period["short_name"], pd.date_range(start, end, freq="h")], names=["short_name", "hour"]
        ).to_frame(index=False)
        frame["period"] = period
        period_frames.append(frame)
    pe = pd.concat(period_frames, ignore_index=True)
    pe = (
        pe.merge(trip_hourly[["short_name", "hour", "departures", "arrivals"]], on=["short_name", "hour"],
                 how="left", validate="one_to_one")
        .merge(model_stations[["short_name", "station_id", "name", "lat", "lon", "capacity", "station_group",
                               "miles_to_nearest_g_stop"]], on="short_name", how="left", validate="many_to_one")
    )
    pe[["departures", "arrivals"]] = pe[["departures", "arrivals"]].fillna(0).astype("int64")
    pe = pe.merge(weather_all, on="hour", how="left", validate="many_to_one")

    holiday_dates = USFederalHolidayCalendar().holidays("2024-01-01", "2026-12-31")
    pe["hour_of_day"] = pe["hour"].dt.hour
    pe["day_of_week"] = pe["hour"].dt.dayofweek
    pe["is_weekend"] = pe["day_of_week"] >= 5
    pe["is_holiday"] = pe["hour"].dt.normalize().isin(holiday_dates)
    pe["hour_sin"] = np.sin(2 * np.pi * pe["hour_of_day"] / 24)
    pe["hour_cos"] = np.cos(2 * np.pi * pe["hour_of_day"] / 24)
    pe["precipitation_any"] = pe["precipitation"] > 0
    pe["total_trips"] = pe["departures"] + pe["arrivals"]
    pe["net_flow"] = pe["arrivals"] - pe["departures"]

    # Shutdown flags: each event covers the whole clock hours that fall inside its window
    event_hours = pd.concat([
        pd.DataFrame({"event_id": e.event_id, "event_type": e.event_type, "hour": window_hours(e.start_local, e.end_local)})
        for e in events.itertuples()
    ], ignore_index=True)
    exposure = event_station_exposure.assign(short_name=event_station_exposure["short_name"].astype(str))
    pe = (
        pe.merge(event_hours, on="hour", how="left", validate="many_to_one")
        .merge(exposure[["event_id", "short_name", "distance_to_event_miles", "event_exposed"]],
               on=["event_id", "short_name"], how="left", validate="many_to_one")
    )
    pe["g_closure_hour"] = pe["event_id"].notna()
    pe["event_exposed"] = pe["event_exposed"].fillna(False).astype(bool)
    pe["exposed_closure_hour"] = pe["g_closure_hour"] & pe["event_exposed"]

    # Stations near the 2026 segment (used for placebo comparisons) and near any 2024 phase stop
    def exposed_for(event_ids):
        return set(exposure.loc[exposure["event_id"].isin(event_ids) & exposure["event_exposed"], "short_name"])
    pe["exposed_station"] = pe["short_name"].isin(
        exposed_for(events.loc[events["event_type"].eq("2026 weekend closure"), "event_id"]))
    pe["exposed_station_2024"] = pe["short_name"].isin(
        exposed_for(events.loc[events["event_type"].str.startswith("2024"), "event_id"]))

    # Placebo weekends: the same Friday 22:00 to Monday 04:00 window on weekends without a known closure
    weekend_hours = (
        ((pe["day_of_week"] == 4) & (pe["hour_of_day"] >= 22))
        | pe["day_of_week"].isin([5, 6])
        | ((pe["day_of_week"] == 0) & (pe["hour_of_day"] < 4))
    )
    pe["placebo_weekend_hour"] = weekend_hours & ~pe["g_closure_hour"] & pe["period"].eq("2026")

    # Chronological split for the 2026 data, with 2024 held out for the shutdown validation
    month_number = pe["hour"].dt.month
    pe["split"] = np.select(
        [pe["period"].eq("2024"), month_number.le(6), month_number.eq(7)],
        ["validation_2024", "train", "validation"], default="test",
    )

    # Availability exists only where the status feed was collected (Greenpoint, Aug 21 to Sep 30, 2026)
    pe["station_id"] = pe["station_id"].astype(str)
    pe = pe.merge(
        panel[["station_id", "hour", *STATUS_COLUMNS]].assign(station_id=lambda f: f["station_id"].astype(str)),
        on=["station_id", "hour"], how="left", validate="many_to_one",
    )
    pe["status_observed"] = pe["snapshot_count"].notna()
    pe = pe.sort_values(["short_name", "period", "hour"]).reset_index(drop=True)

    # A station is active from its first to its last recorded trip in each period; other hours are not demand
    span = pe.loc[pe["total_trips"].gt(0)].groupby(["short_name", "period"])["hour"].agg(first_trip="min", last_trip="max")
    pe = pe.merge(span.reset_index(), on=["short_name", "period"], how="left")
    pe["station_active"] = pe["hour"].between(pe["first_trip"], pe["last_trip"])
    pe = pe.drop(columns=["first_trip", "last_trip"])
    if pe.duplicated(["short_name", "hour"]).any():
        raise ValueError("The extended panel contains duplicate station hour keys.")
    return pe


def add_baseline_and_profile(pe: pd.DataFrame) -> pd.DataFrame:
    """Four week baseline (closure hours excluded, at least two weeks) and the normal day of week by hour profile."""
    pe = pe.copy()
    group_keys = ["short_name", "period"]
    for flow in ("departures", "arrivals"):
        normal_values = pe[flow].where(~pe["g_closure_hour"] & pe["station_active"])
        lagged = pd.concat(
            [normal_values.groupby([pe[key] for key in group_keys]).shift(168 * week)
             for week in range(1, study.BASELINE_WEEKS + 1)], axis=1,
        )
        pe[f"baseline_{flow}"] = lagged.mean(axis=1).where(lagged.notna().sum(axis=1) >= study.MIN_BASELINE_WEEKS)
    reference_window = (
        (pe["period"].eq("2026") & pe["hour"].between("2026-02-01", "2026-07-31 23:00"))
        | (pe["period"].eq("2024") & pe["hour"].lt("2024-06-28"))
    ) & ~pe["g_closure_hour"] & pe["station_active"]
    profile_keys = ["short_name", "period", "day_of_week", "hour_of_day"]
    profile = (
        pe.loc[reference_window].groupby(profile_keys)[["departures", "arrivals"]].mean()
        .add_prefix("profile_").reset_index()
    )
    pe = pe.merge(profile, on=profile_keys, how="left", validate="many_to_one")
    pe["baseline_total_trips"] = pe["baseline_departures"] + pe["baseline_arrivals"]
    pe["profile_total_trips"] = pe["profile_departures"] + pe["profile_arrivals"]
    return pe


def status_proxy_frame(pe: pd.DataFrame) -> pd.DataFrame:
    """Hours with status: change in mean bikes available and the demand proxies built from it."""
    status_panel = pe[pe["status_observed"]].sort_values(["short_name", "hour"]).copy()
    by_station = status_panel.groupby("short_name")
    status_panel["bikes_change"] = by_station["bikes_available_mean"].diff()
    contiguous = (status_panel["hour"].diff().eq(pd.Timedelta(hours=1))
                  & status_panel["short_name"].eq(status_panel["short_name"].shift()))
    status_panel["bikes_change"] = status_panel["bikes_change"].where(contiguous)
    # The mean over hour t minus the mean over hour t-1 reflects flows from both hours, so compare with their average
    status_panel["net_flow_two_hour_mean"] = (status_panel["net_flow"] + by_station["net_flow"].shift()) / 2
    status_panel["departures_two_hour_mean"] = (status_panel["departures"] + by_station["departures"].shift()) / 2
    status_panel["status_net_proxy"] = -status_panel["bikes_change"]
    status_panel["status_departure_proxy"] = status_panel["bikes_change"].clip(upper=0).abs()
    status_panel["status_arrival_proxy"] = status_panel["bikes_change"].clip(lower=0)
    return status_panel


PROXY_COLUMNS = ["bikes_change", "status_net_proxy", "status_departure_proxy", "status_arrival_proxy"]


def add_status_proxies(pe: pd.DataFrame) -> pd.DataFrame:
    status_panel = status_proxy_frame(pe)
    return pe.drop(columns=[c for c in PROXY_COLUMNS if c in pe.columns]).merge(
        status_panel[["short_name", "hour", *PROXY_COLUMNS]], on=["short_name", "hour"], how="left",
        validate="one_to_one",
    )


def proxy_check_table(status_panel: pd.DataFrame) -> pd.DataFrame:
    valid = status_panel.dropna(subset=["bikes_change", "net_flow_two_hour_mean"])
    nonzero = valid[valid["bikes_change"].ne(0) & valid["net_flow_two_hour_mean"].ne(0)]
    return pd.DataFrame([
        ("Station hours with a status change", len(valid)),
        ("Correlation of bikes change with net flow (arrivals minus departures)",
         valid["bikes_change"].corr(valid["net_flow_two_hour_mean"])),
        ("Correlation of departure proxy with departures",
         valid["status_departure_proxy"].corr(valid["departures_two_hour_mean"])),
        ("Share of nonzero hours where the signs agree",
         np.mean(np.sign(nonzero["bikes_change"]) == np.sign(nonzero["net_flow_two_hour_mean"]))),
        ("Mean absolute error of the proxy for net flow",
         (valid["bikes_change"] - valid["net_flow_two_hour_mean"]).abs().mean()),
    ], columns=["Check", "Result"])


def build_extended_panel(trip_hourly, model_stations, weather_all, events, event_station_exposure, panel):
    pe = extended_panel_base(trip_hourly, model_stations, weather_all, events, event_station_exposure, panel)
    return add_status_proxies(add_baseline_and_profile(pe))


# ----------------------------------------------------------------------------------------------------------------
# Summaries
# ----------------------------------------------------------------------------------------------------------------

def poisson_deviance(actual, predicted):
    actual = np.asarray(actual, dtype=float)
    predicted = np.clip(np.asarray(predicted, dtype=float), 0.1, None)
    ratio_term = np.where(actual > 0, actual * np.log(np.where(actual > 0, actual, 1) / predicted), 0.0)
    return 2 * np.mean(ratio_term - (actual - predicted))


def baseline_accuracy(pe: pd.DataFrame) -> pd.DataFrame:
    """Error of the four week baseline by month and station group, on normal active non-holiday hours."""
    scored = pe[pe["baseline_total_trips"].notna() & ~pe["g_closure_hour"] & ~pe["is_holiday"]
                & pe["station_active"]].copy()
    scored["month"] = scored["hour"].dt.to_period("M").astype(str)
    rows = []
    for (month, group), frame in scored.groupby(["month", "station_group"]):
        rows.append({
            "month": month, "station_group": group,
            "station_hours": len(frame),
            "mean_actual": frame["total_trips"].mean(),
            "baseline_mae": (frame["total_trips"] - frame["baseline_total_trips"]).abs().mean(),
            "baseline_poisson_deviance": poisson_deviance(frame["total_trips"], frame["baseline_total_trips"]),
        })
    return pd.DataFrame(rows)


def shutdown_effect_screening(pe: pd.DataFrame) -> pd.DataFrame:
    """Actual to profile demand ratio at exposed and control stations for each event type and the placebo weekends."""
    frame = pe[pe["profile_total_trips"].notna() & pe["station_active"]].copy()
    frame["event_exposed_or_segment"] = np.where(frame["g_closure_hour"], frame["event_exposed"], frame["exposed_station"])

    def ratio_rows(part_frame, label):
        rows = []
        for group_name, mask in (("Exposed stations", part_frame["event_exposed_or_segment"]),
                                 ("Control stations", part_frame["station_group"].eq("control"))):
            part = part_frame[mask & part_frame["profile_total_trips"].gt(0)]
            rows.append({"event": label, "group": group_name, "station_hours": len(part),
                         "actual_to_profile": part["total_trips"].sum() / part["profile_total_trips"].sum()})
        return rows

    rows = ratio_rows(frame[frame["event_type"].eq("2026 weekend closure")], "2026 closure weekends")
    rows += ratio_rows(frame[frame["placebo_weekend_hour"]], "2026 placebo weekends")
    for phase, *_ in study.PHASES_2024:
        rows += ratio_rows(frame[frame["event_type"].eq(phase)], phase)
    return pd.DataFrame(rows)
