import numpy as np
import pandas as pd
import pytest

from citibike import study
from citibike import transformations as tf


def test_haversine_known_distance():
    # One degree of latitude is about 111.2 km
    assert tf.haversine_km(40.0, -73.0, 41.0, -73.0) == pytest.approx(111.2, abs=0.1)
    assert tf.haversine_miles(40.0, -73.0, 40.0, -73.0) == 0


def _utc_weather(start, end):
    times = pd.date_range(start, end, freq="h")
    return pd.DataFrame({"time": times, "temperature_2m": np.arange(len(times), dtype=float),
                         "precipitation": np.ones(len(times))})


def test_weather_uses_daylight_saving_offsets():
    # Summer noon in New York is 16:00 UTC (EDT, UTC-4); winter noon is 17:00 UTC (EST, UTC-5)
    summer = tf.utc_weather_to_new_york(_utc_weather("2024-07-01 00:00", "2024-07-02 23:00"))
    assert summer.set_index("time").loc["2024-07-01 12:00", "temperature_2m"] == 16
    winter = tf.utc_weather_to_new_york(_utc_weather("2026-01-01 00:00", "2026-01-02 23:00"))
    assert winter.set_index("time").loc["2026-01-01 12:00", "temperature_2m"] == 17


def test_weather_spring_forward_hour_is_filled():
    # 2026-03-08: clocks jump from 02:00 to 03:00 in New York
    local = tf.utc_weather_to_new_york(_utc_weather("2026-03-08 00:00", "2026-03-09 04:00"), start="2026-03-08",
                                       end="2026-03-08").set_index("time")
    assert len(local) == 24 and local.index.is_unique
    skipped = local.loc["2026-03-08 02:00"]
    assert skipped["precipitation"] == 0
    assert skipped["temperature_2m"] == (local.loc["2026-03-08 01:00", "temperature_2m"]
                                         + local.loc["2026-03-08 03:00", "temperature_2m"]) / 2


def test_weather_fall_back_hour_is_merged():
    # 2026-11-01: 01:00 happens twice in New York (05:00 and 06:00 UTC)
    local = tf.utc_weather_to_new_york(_utc_weather("2026-11-01 00:00", "2026-11-02 04:00"), start="2026-11-01",
                                       end="2026-11-01").set_index("time")
    assert len(local) == 24 and local.index.is_unique
    assert local.loc["2026-11-01 01:00", "temperature_2m"] == 5.5     # mean of the 05:00 and 06:00 UTC values
    assert local.loc["2026-11-01 01:00", "precipitation"] == 2        # both hours of rain are kept


def test_weather_trim_and_missing_values():
    frame = _utc_weather("2026-08-01 00:00", "2026-08-03 23:00")
    frame.loc[frame.index[-3:], ["temperature_2m", "precipitation"]] = np.nan   # not yet published by the archive
    local = tf.utc_weather_to_new_york(frame, start="2026-08-01", end="2026-08-02")
    assert local["time"].min() == pd.Timestamp("2026-08-01 00:00")
    assert local["time"].max() == pd.Timestamp("2026-08-02 23:00")
    assert local["precipitation"].notna().all()


def test_confirmed_windows_have_55_closure_hours_each():
    windows = tf.confirmed_closure_windows()
    assert len(windows) == 4
    assert windows["closure_hours"].tolist() == [55, 55, 55, 55]
    hours = tf.closure_hour_index(windows)
    assert len(hours) == 220
    # first whole hour is 22:00 Friday, last is 04:00 Monday
    assert hours[0] == pd.Timestamp("2026-08-07 22:00")
    assert hours[54] == pd.Timestamp("2026-08-10 04:00")


def test_pre_august_windows():
    windows = tf.pre_august_closure_windows()
    assert len(windows) == 9
    assert windows["times_assumed"].sum() == 8
    assert windows["notice_url"].str.startswith("https://www.mta.info/article/").all()


def _g_stops():
    return pd.DataFrame({
        "gtfs_stop_id": pd.array(["G20", "G22", "G26", "G32", "G33", "G36"], dtype="string"),
        "stop_name": ["A", "Court Sq", "Greenpoint Av", "Myrtle", "Bedford-Nostrand", "Fulton"],
        "gtfs_latitude": [40.70, 40.747, 40.731, 40.695, 40.689, 40.687],
        "gtfs_longitude": [-73.94, -73.946, -73.954, -73.949, -73.953, -73.975],
    })


def test_affected_segment_is_g22_to_g32():
    assert tf.affected_segment_stop_ids(_g_stops()).tolist() == ["G22", "G26", "G32"]


def test_g_stops_from_subway_stations_keeps_g_routes_only():
    raw = pd.DataFrame({
        "GTFS Stop ID": ["G26", "A1"], "Station ID": ["1", "2"], "Complex ID": ["10", "20"],
        "Stop Name": ["Greenpoint Av", "Other"], "Daytime Routes": ["G", "A C"],
        "GTFS Latitude": ["40.731", "40.7"], "GTFS Longitude": ["-73.954", "-73.9"],
    })
    stops = tf.g_stops_from_subway_stations(raw)
    assert stops["gtfs_stop_id"].tolist() == ["G26"]
    assert stops["gtfs_latitude"].dtype == float


def test_station_exposure_bands():
    stations = pd.DataFrame({
        "station_id": ["near", "mid", "far"], "name": ["n", "m", "f"],
        "lat": [40.7312, 40.7350, 40.7400], "lon": [-73.9540, -73.9540, -73.9700], "capacity": [10, 10, 10],
    })
    exposure = tf.greenpoint_station_exposure(stations, _g_stops(), tf.affected_segment_stop_ids(_g_stops()))
    by_station = exposure.set_index("station_id")
    assert bool(by_station.loc["near", "exposed_station"]) is True
    assert by_station.loc["near", "distance_band"] == "Within 0.25 mile"
    assert by_station.loc["mid", "distance_band"] == "0.25 to 0.5 mile"
    assert by_station.loc["far", "distance_band"] == "Beyond 0.5 mile"
    assert list(exposure["distance_band"].cat.categories) == study.DISTANCE_BANDS


def test_infer_reduction_windows_finds_a_joint_drop():
    hours = pd.date_range("2026-08-01", "2026-09-30 23:00", freq="h")
    rows = []
    for station in study.DETECTOR_STATIONS + ["Other (G)"]:
        values = np.full(len(hours), 100.0)
        if station in study.DETECTOR_STATIONS:
            drop = (hours >= "2026-08-22 08:00") & (hours < "2026-08-22 14:00")
            values[drop] = 1.0
        rows.append(pd.DataFrame({"transit_timestamp": hours, "station_complex": station, "ridership": values}))
    check = tf.infer_reduction_windows(pd.concat(rows))
    assert len(check.windows) == 1
    window = check.windows.iloc[0]
    assert window["start_local"] == pd.Timestamp("2026-08-22 08:00")
    assert window["end_local"] == pd.Timestamp("2026-08-22 14:00")
    assert window["hours"] == 6
    assert bool(window["in_status_window"]) is True


def test_shutdown_events_ids_and_stops():
    segment = pd.Series(["G22", "G26", "G32"])
    events = tf.shutdown_events(tf.pre_august_closure_windows(), tf.confirmed_closure_windows(), segment)
    assert len(events) == 16
    assert events["event_id"].tolist()[:2] == ["E01", "E02"]
    assert events["start_local"].is_monotonic_increasing
    phase2 = events[events["event_type"].eq("2024 phase 2")].iloc[0]
    assert phase2["stop_ids"] == ["G22", "G26", "G32"]


def _panel_extended(weeks=6):
    hours = pd.date_range("2026-02-02 00:00", periods=24 * 7 * weeks, freq="h")
    frame = pd.DataFrame({"short_name": "s1", "period": "2026", "hour": hours})
    frame["departures"] = np.where(frame["hour"].dt.hour == 8, 4, 1)
    frame["arrivals"] = 1
    frame["total_trips"] = frame["departures"] + frame["arrivals"]
    frame["day_of_week"] = frame["hour"].dt.dayofweek
    frame["hour_of_day"] = frame["hour"].dt.hour
    frame["g_closure_hour"] = False
    frame["station_active"] = True
    frame["is_holiday"] = False
    frame["station_group"] = "corridor"
    return frame


def test_baseline_needs_two_earlier_weeks_and_matches_weekly_pattern():
    pe = tf.add_baseline_and_profile(_panel_extended())
    first_week = pe["hour"] < pd.Timestamp("2026-02-16")
    assert pe.loc[first_week, "baseline_departures"].isna().all()
    later = pe[~first_week]
    assert later["baseline_departures"].notna().all()
    # demand is the same every week, so the baseline equals the outcome
    np.testing.assert_allclose(later["baseline_total_trips"], later["total_trips"])
    np.testing.assert_allclose(pe["profile_total_trips"], pe["total_trips"])


def test_baseline_excludes_closure_hours():
    frame = _panel_extended()
    closure = frame["hour"].between("2026-02-02 08:00", "2026-02-02 08:00")
    frame.loc[closure, "departures"] = 100
    frame.loc[closure, "g_closure_hour"] = True
    pe = tf.add_baseline_and_profile(frame)
    one_week_later = pe[pe["hour"] == pd.Timestamp("2026-02-23 08:00")].iloc[0]
    assert one_week_later["baseline_departures"] == 4


def test_baseline_accuracy_and_poisson_deviance():
    assert tf.poisson_deviance([2, 0], [2, 0.05]) == pytest.approx(0.1)
    pe = tf.add_baseline_and_profile(_panel_extended())
    accuracy = tf.baseline_accuracy(pe)
    assert list(accuracy.columns) == ["month", "station_group", "station_hours", "mean_actual", "baseline_mae",
                                      "baseline_poisson_deviance"]
    assert (accuracy["baseline_mae"] == 0).all()


def test_status_proxies_use_contiguous_hours_only():
    hours = pd.to_datetime(["2026-08-21 10:00", "2026-08-21 11:00", "2026-08-21 13:00"])
    pe = pd.DataFrame({
        "short_name": "s1", "hour": hours, "status_observed": True,
        "bikes_available_mean": [10.0, 7.0, 9.0], "net_flow": [0, -3, 2], "departures": [0, 3, 0],
    })
    status = tf.status_proxy_frame(pe)
    assert np.isnan(status["bikes_change"].iloc[0])
    assert status["bikes_change"].iloc[1] == -3
    assert status["status_departure_proxy"].iloc[1] == 3
    assert np.isnan(status["bikes_change"].iloc[2])  # 12:00 is missing, so 13:00 has no change
    merged = tf.add_status_proxies(pe)
    assert len(merged) == 3 and "status_net_proxy" in merged


def test_greenpoint_g_stop_distances_flags_quarter_mile():
    stations = pd.DataFrame({"station_id": ["near", "far"], "lat": [40.7312, 40.7400], "lon": [-73.9540, -73.9700]})
    pairs = tf.greenpoint_g_stop_distances(stations, _g_stops())
    assert len(pairs) == 2 * len(_g_stops())
    near_stops = set(pairs.loc[pairs["within_quarter_mile"], "station_id"])
    assert near_stops == {"near"}


def test_g_service_events_parses_g_alerts_only():
    feed = {"entity": [
        {"id": "a1", "alert": {
            "informed_entity": [{"route_id": "G", "stop_id": "G26"}, {"route_id": "G", "stop_id": "G22"}],
            "active_period": [{"start": 1792942944, "end": 1792950000}, {"start": 1793000000}],
            "header_text": {"translation": [{"text": "No G trains"}]},
            "transit_realtime.mercury_alert": {"alert_type": "Planned - Part Suspended"}}},
        {"id": "a2", "alert": {"informed_entity": [{"route_id": "A"}], "active_period": [{"start": 1792942944}]}},
    ]}
    events = tf.g_service_events(feed, exposed_stop_ids={"G26"})
    assert len(events) == 2
    assert events["alert_id"].unique().tolist() == ["a1"]
    assert events["g_stop_ids"].iloc[0] == "G22;G26"
    assert events["touches_greenpoint_stops"].all()
    assert events["end_utc"].isna().sum() == 1
    assert str(events["start_utc"].dt.tz) == "UTC"


def test_greenpoint_status_frame_adds_attributes_and_calendar():
    status = pd.DataFrame({"station_id": ["a", "a", "z"],
                           "hour": pd.to_datetime(["2026-08-22 08:00", "2026-08-24 17:00", "2026-08-22 08:00"]),
                           "snapshot_count": [12, 12, 12]})
    stations = pd.DataFrame({"station_id": ["a"], "name": ["A"], "lat": [40.73], "lon": [-73.95], "capacity": [20]})
    frame = tf.greenpoint_status_frame(status, stations)
    assert frame["station_id"].tolist() == ["a", "a"]          # stations outside the screen are dropped
    assert frame["day_type"].tolist() == ["Weekend", "Weekday"]
    assert frame["hour_of_day"].tolist() == [8, 17]
    assert frame["name"].eq("A").all()
