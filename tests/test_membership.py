import numpy as np
import pandas as pd

from forecast.config import EngineParams
from forecast.models.club import detect_releases
from forecast.models.membership import (attrition_base_series, project_stock,
                                        spike_component, spike_profile)
from forecast.types import ScheduledRelease
from tests.synth import make_history

P = EngineParams()


def test_spike_profile_recovers_planted_decay():
    df = make_history()
    rel = detect_releases(df[4], P)
    prof = spike_profile(df[25], rel, df[23], P.attrition_spike_days, P.release_lookback_n)
    assert prof.shape == (21,)
    expected = 0.004 * np.exp(-np.arange(21) / 5)
    assert np.allclose(prof[:5], expected[:5], rtol=0.3)
    assert prof[0] > prof[10] > prof[20] >= 0


def test_attrition_base_series_interpolates_over_spike_windows():
    df = make_history()
    rel = detect_releases(df[4], P)
    base = attrition_base_series(df[25], rel, P.attrition_spike_days)
    assert base.index.equals(df.index) and base.notna().all()
    for r in rel:
        assert base.loc[r.dates].max() < 12       # interpolated between ordinary Poisson(3) days


def test_spike_component_uses_members_at_release():
    dates = pd.date_range("2027-02-20", periods=30)
    prof = np.array([2.0, 1.0] + [0.0] * 19) / 1000
    sched = [ScheduledRelease(start=pd.Timestamp("2027-02-24"), days=2, label="x")]
    comp = spike_component(sched, prof, lambda d: 4000.0, dates)
    assert comp[pd.Timestamp("2027-02-24")] == 8.0 and comp[pd.Timestamp("2027-02-25")] == 4.0
    assert comp.sum() == 12.0


def test_project_stock_is_running_balance():
    dates = pd.date_range("2027-01-01", periods=3)
    new = pd.Series([5.0, 0.0, 2.0], index=dates)
    attr = pd.Series([1.0, 1.0, 1.0], index=dates)
    assert project_stock(100.0, new, attr).tolist() == [104.0, 103.0, 104.0]
