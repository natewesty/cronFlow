import inspect

from forecast import backtest, engine, monthly
from forecast.models import club


def test_signatures_are_fixed():
    assert list(inspect.signature(club.detect_releases).parameters) == ["club", "params"]
    assert list(inspect.signature(club.default_schedule).parameters) == ["releases", "origin", "years"]
    assert list(inspect.signature(engine.forecast_daily).parameters) == ["hist", "origin", "dates", "ctx"]
    assert list(inspect.signature(monthly.monthly_forecast).parameters) == ["hist", "origin", "ctx", "daily_point"]
    assert list(inspect.signature(backtest.run_backtest).parameters) == ["hist", "releases", "params", "origin", "champions", "snapshots"]
    assert list(inspect.signature(backtest.bucket_for).parameters) == ["horizon", "params"]
