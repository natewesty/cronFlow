import numpy as np
import pandas as pd

from forecast.identities import aggregate_period, apply_identities


def _frame():
    idx = pd.date_range("2026-01-01", periods=3, freq="D")
    df = pd.DataFrame(0.0, index=idx, columns=list(range(1, 32)))
    df[1] = [100, 200, 300]; df[2] = [10, 20, 30]; df[4] = [0, 1000, 0]; df[5] = [1, 1, 1]
    df[7] = 5; df[8] = 6; df[9] = 7; df[10] = 8; df[12] = 9; df[13] = 10
    df[15] = [2, 4, 0]; df[16] = [8, 12, 0]; df[18] = [6, 10, 0]; df[30] = [3, 5, 0]
    df[24] = [2, 0, 1]; df[25] = [1, 1, 1]; df[23] = [100, 101, 101]
    df[28] = [3, 0, 4]; df[29] = [50, 0, 70]; df[31] = [7, 9, 0]
    return df


def test_sums_and_diffs():
    out = apply_identities(_frame())
    assert list(out.columns) == list(range(1, 32))
    assert out[3].tolist() == [110, 220, 330]
    assert out[6].tolist() == [1, 1001, 1]
    assert out[11].tolist() == [15, 15, 15]
    assert out.loc["2026-01-02", 14] == 200 + 20 + 1000 + 1 + 5 + 6 + 7 + 8 + 9 + 10
    assert out[26].tolist() == [1, -1, 0]


def test_identities_zero_denominator_gives_zero():
    out = apply_identities(_frame())
    assert out.loc["2026-01-03", 17] == 0 and out.loc["2026-01-03", 21] == 0
    assert out.loc["2026-01-01", 17] == 4.0
    assert out.loc["2026-01-01", 20] == (10 + 1 + 8) / 8
    assert out.loc["2026-01-01", 21] == 50.0
    assert out.loc["2026-01-01", 27] == 300.0
    assert out.loc["2026-01-02", 27] == 0
    assert not out.isna().any().any()


def test_aggregate_period_rules():
    agg = aggregate_period(apply_identities(_frame()))
    assert agg[1] == 600 and agg[3] == 660
    assert agg[23] == 101                      # stock: last value
    assert agg[17] == 20 / 6                   # ratios from summed components
    assert agg[21] == 8 / 16 * 100
    assert agg[29] == 60                       # AOV averaged over days with orders (28 > 0)
    assert agg[26] == 0
