"""Club release detection, scheduling and magnitude (spec §4.4). WS-A fills this in."""
from __future__ import annotations

from typing import List

import pandas as pd

from ..config import EngineParams
from ..types import Release, ScheduledRelease


def detect_releases(club: pd.Series, params: EngineParams) -> List[Release]:
    raise NotImplementedError("WS-A")


def default_schedule(releases: List[Release], origin: pd.Timestamp, years: int = 2) -> List[ScheduledRelease]:
    raise NotImplementedError("WS-A")
