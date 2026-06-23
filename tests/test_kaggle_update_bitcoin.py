"""Offline tests for the data-processing logic.

These exercise the pandas/requests code paths that a dependency bump could
break, without any network calls, Kaggle credentials, or uploads. Network is
monkeypatched; only pure local computation runs.
"""

from datetime import datetime, timezone

import pandas as pd

from kaggle_bitcoin import kaggle_update_bitcoin as m

COLUMNS = ["Timestamp", "Open", "High", "Low", "Close", "Volume"]


def _write_csv(path, rows):
    pd.DataFrame(rows, columns=COLUMNS).to_csv(path, index=False)


def test_check_missing_data_detects_gap(tmp_path):
    csv = tmp_path / "data.csv"
    old_ts = 1_500_000_000  # well in the past
    _write_csv(csv, [[old_ts, 1, 2, 0, 1, 10], [old_ts + 60, 1, 2, 0, 1, 10]])

    last, current = m.check_missing_data(str(csv))

    assert last == old_ts + 60
    assert current is not None and current > last


def test_check_missing_data_up_to_date(tmp_path):
    csv = tmp_path / "data.csv"
    future_ts = int(datetime.now(timezone.utc).timestamp()) + 10_000
    _write_csv(csv, [[future_ts, 1, 2, 0, 1, 10]])

    last, current = m.check_missing_data(str(csv))

    assert last is None and current is None


def test_fetch_bitstamp_data_parses_ohlc(monkeypatch):
    class FakeResp:
        def raise_for_status(self):
            pass

        def json(self):
            return {"data": {"ohlc": [{"timestamp": "1", "open": "1"}]}}

    monkeypatch.setattr(m.requests, "get", lambda *a, **k: FakeResp())

    assert m.fetch_bitstamp_data("btcusd", 0, 60) == [{"timestamp": "1", "open": "1"}]


def test_fetch_bitstamp_data_handles_request_error(monkeypatch):
    def boom(*a, **k):
        raise m.requests.exceptions.RequestException("network down")

    monkeypatch.setattr(m.requests, "get", boom)

    assert m.fetch_bitstamp_data("btcusd", 0, 60) == []


def test_fetch_and_append_dedupes_and_sorts(tmp_path, monkeypatch):
    existing = tmp_path / "data.csv"
    out = tmp_path / "out.csv"
    _write_csv(existing, [[100, 1, 1, 1, 1, 1], [160, 1, 1, 1, 1, 1]])

    # New data overlaps an existing timestamp (160) and extends past it (220),
    # delivered out of order to exercise the sort.
    new_ohlc = [
        {"timestamp": 220, "open": 2, "high": 2, "low": 2, "close": 2, "volume": 2},
        {"timestamp": 160, "open": 9, "high": 9, "low": 9, "close": 9, "volume": 9},
    ]
    monkeypatch.setattr(m, "fetch_bitstamp_data", lambda *a, **k: new_ohlc)
    monkeypatch.setattr(m.time, "sleep", lambda *_: None)

    m.fetch_and_append_missing_data("btcusd", 100, 220, str(existing), str(out))

    res = pd.read_csv(out)
    # Deduplicated on Timestamp and sorted ascending.
    assert list(res["Timestamp"]) == [100, 160, 220]
    # keep="first" means the existing row for 160 wins (Close stays 1, not 9).
    assert res.loc[res["Timestamp"] == 160, "Close"].iloc[0] == 1
