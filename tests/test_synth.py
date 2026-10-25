import numpy as np
import pandas as pd

from forecast.identities import apply_identities
from tests.synth import make_history, release_starts


def test_history_shape_and_identities():
    df = make_history()
    assert list(df.columns) == list(range(1, 32))
    assert df.index[0] == pd.Timestamp("2023-07-01") and df.index[-1] == pd.Timestamp("2026-10-23")
    assert not df.isna().any().any()
    chk = apply_identities(df)
    for k in [3, 6, 11, 14, 17, 20, 21, 26, 27]:
        assert np.allclose(chk[k], df[k]), k


def test_releases_are_planted():
    df = make_history()
    starts = release_starts(df.index[0], df.index[-1])
    assert len(starts) == 13            # Sep23 Nov23 Feb24 May24 Sep24 Nov24 Feb25 May25 Sep25 Nov25 Feb26 May26 Sep26
    for s in starts:
        assert s.dayofweek == 2
        assert df.loc[s, 4] > 100000 and df.loc[s + pd.Timedelta(days=1), 4] > df.loc[s, 4]
        assert df.loc[s + pd.Timedelta(days=3), 4] == 0
    # attrition spike right after a release
    s = starts[-1]
    assert df.loc[s, 25] > df.loc[s - pd.Timedelta(days=30):s - pd.Timedelta(days=1), 25].mean() + 5


def test_seed_is_deterministic():
    assert make_history(seed=3).equals(make_history(seed=3))
    assert not make_history(seed=3).equals(make_history(seed=4))
