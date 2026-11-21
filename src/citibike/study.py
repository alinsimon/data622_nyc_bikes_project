"""Study definitions: the fixed choices of the analysis, kept in one place.

These values come from "Citi Bike Demand Analysis" (sections 2.1 to 2.6). Changing one changes the analysis tables,
so record the reason in the notebook when you do.
"""

import pandas as pd

# All times in the analysis are naive New York clock times
TIMEZONE = "America/New_York"

# Greenpoint screening rectangle (latest station snapshot, stations with capacity)
GREENPOINT_LATITUDE = (40.720, 40.740)
GREENPOINT_LONGITUDE = (-73.965, -73.935)

QUARTER_MILE_KM = 0.402336
KM_PER_MILE = 1.609344
EXPOSURE_MILES = 0.25
DISTANCE_BANDS = ["Within 0.25 mile", "0.25 to 0.5 mile", "Beyond 0.5 mile"]

# Corridor and control station sets for the extended panel
CONTROL_MIN_MILES = 3.0
CONTROL_SAMPLE_SIZE = 60
SELECTION_SEED = 622

# The MTA notices name the affected segment: no G trains between Court Sq (G22) and Bedford-Nostrand Avs,
# i.e. G stops G22 to G32.
AFFECTED_SEGMENT_STOP_NUMBERS = (22, 32)

# Ridership check of the G service reduction
G_COMPLEX_IDS = [175, 236, 237, 238, 240, 241, 242, 243, 282, 283, 284, 286, 287,
                 288, 289, 290, 291, 292, 606, 608, 629]
DETECTOR_STATIONS = ["Greenpoint Av (G)", "Nassau Av (G)"]
RIDERSHIP_HOLIDAYS = ["2026-09-04", "2026-09-07", "2026-09-08"]
RIDERSHIP_THRESHOLD = 0.35
STATUS_WINDOW = ("2026-08-21", "2026-10-02")

# MTA Weekender notices: no G trains between Court Sq and Bedford-Nostrand Avs, 9:45 pm Friday to 5 am Monday
MTA_WEEKENDER_NOTICES = [
    ("2026-08-07", "https://www.mta.info/article/mta-weekender-august-7-10-2026"),
    ("2026-08-21", "https://www.mta.info/article/mta-weekender-august-21-24-2026"),
    ("2026-09-11", "https://www.mta.info/article/mta-weekender-september-11-14-2026"),
    ("2026-09-25", "https://www.mta.info/article/mta-weekender-september-25-28-2026"),
]
# Earlier 2026 closure weekends: (Friday, notice slug, whether the times were assumed rather than stated)
PRE_AUGUST_NOTICES = [
    ("2026-02-27", "mta-weekender-february-27-march-2-2026", False),
    ("2026-03-06", "mta-weekender-march-6-9-2026", True),
    ("2026-03-13", "mta-weekender-march-13-16-2026", True),
    ("2026-03-20", "mta-weekender-march-20-23-2026", True),
    ("2026-03-27", "mta-weekender-march-27-30-2026", True),
    ("2026-04-17", "mta-weekender-april-17-20-2026", True),
    ("2026-06-05", "mta-weekender-june-5-8-2026", True),
    ("2026-06-19", "mta-weekender-june-19-22-2026", True),
    ("2026-07-10", "mta-weekender-july-10-13-2026", True),
]
WEEKEND_CLOSURE_START = pd.Timedelta(hours=21, minutes=45)   # Friday 9:45 pm
WEEKEND_CLOSURE_END = pd.Timedelta(days=3, hours=5)          # Monday 5 am
# Summer 2024 G line shutdown phases (dates from the MTA notice text)
PHASES_2024 = [
    ("2024 phase 1", "2024-06-28", "2024-07-05", ["G22", "G24", "G26"]),
    ("2024 phase 2", "2024-07-05", "2024-08-12", None),   # None: the whole affected segment
    ("2024 phase 3", "2024-08-12", "2024-09-03", ["G34", "G35", "G36"]),
]
PHASE_NOTICE = "MTA summer 2024 G line service change (phase dates from the MTA notice text)"

# Section 2.5 panel: Greenpoint stations, August and September 2026
PANEL_START = pd.Timestamp("2026-08-01 00:00")
PANEL_END = pd.Timestamp("2026-09-30 23:00")
TRIP_COVERAGE_START = pd.Timestamp("2026-08-01 00:00")

# Section 2.6 extended panel: May to September 2024 and January to September 2026
EXTENDED_PERIODS = [
    ("2024", pd.Timestamp("2024-05-01 00:00"), pd.Timestamp("2024-09-30 23:00")),
    ("2026", pd.Timestamp("2026-01-01 00:00"), pd.Timestamp("2026-09-30 23:00")),
]
BASELINE_WEEKS = 4
MIN_BASELINE_WEEKS = 2

# Trip months loaded by section 2.1 (January, August, September 2026) and by section 2.6 (14 months)
TRIP_MONTHS = ("2026-01", "2026-08", "2026-09")
EXTENDED_MONTHS = (
    "2024-05", "2024-06", "2024-07", "2024-08", "2024-09",
    "2026-01", "2026-02", "2026-03", "2026-04", "2026-05", "2026-06", "2026-07", "2026-08", "2026-09",
)
