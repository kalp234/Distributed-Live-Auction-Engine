"""Unit tests for pure logic (no running stack needed)."""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from auction import faults
from auction.bidding import _validate
from auction.coordinator import gid
from auction.errors import AppError
from auction.topology import Member, ShardTopology


def item(**kw):
    base = {"status": "open", "ends_at": datetime.now(timezone.utc) + timedelta(minutes=5), "seller_id": 1,
            "current_price": Decimal("50"), "start_price": Decimal("10"), "highest_bidder": None}
    return {**base, **kw}


@pytest.mark.parametrize("it, user, amount, code", [
    (None, 3, "20", "not_found"),
    (item(status="sold"), 3, "60", "auction_closed"),
    (item(ends_at=datetime.now(timezone.utc) - timedelta(seconds=1)), 3, "60", "auction_closed"),
    (item(), 1, "60", "own_item"),
    (item(highest_bidder=4), 3, "50", "bid_too_low"),        # must be strictly higher
    (item(current_price=Decimal("10")), 3, "9.99", "bid_too_low"),  # below start price
])
def test_validate_rejects(it, user, amount, code):
    with pytest.raises(AppError) as e:
        _validate(it, user, Decimal(amount))
    assert e.value.code == code


def test_validate_accepts_first_bid_at_start_price_and_higher_bid():
    _validate(item(current_price=Decimal("10")), 3, Decimal("10"))   # first bid may equal start price
    _validate(item(highest_bidder=4), 3, Decimal("50.01"))


def test_gid_format_is_parseable():
    t = uuid.uuid4()
    g = gid(t, "B")
    prefix, txn, shard = g.split(":")
    assert (prefix, uuid.UUID(txn), shard) == ("auc", t, "B")


def test_faults_are_consumed():
    faults.clear()
    faults.arm("vote_no_b", count=2)
    assert faults.take("vote_no_b") and faults.take("vote_no_b")
    assert faults.take("vote_no_b") is None
    with pytest.raises(ValueError):
        faults.arm("no_such_fault")


def test_read_candidates_exclude_lagging_and_non_streaming_replicas():
    s = ShardTopology("A", "auction", [], "u", "p", max_lag_bytes=1000)
    s.members = {
        "pg-a1": Member("pg-a1", "pg-a1", 5432, "leader", "running", None, 1),
        "pg-a2": Member("pg-a2", "pg-a2", 5432, "sync_standby", "streaming", 0, 1),
        "pg-a3": Member("pg-a3", "pg-a3", 5432, "replica", "streaming", 0, 1),
        "pg-a4": Member("pg-a4", "pg-a4", 5432, "replica", "starting", 0, 1),
    }
    s.replication = {"pg-a3": {"replay_lag_bytes": 5000}, "pg-a2": {"replay_lag_bytes": 10}}
    assert [m.name for m in s.read_candidates()] == ["pg-a2"]
    s.replication["pg-a3"]["replay_lag_bytes"] = 999
    assert sorted(m.name for m in s.read_candidates()) == ["pg-a2", "pg-a3"]
    assert s.members["pg-a1"].is_leader and not s.members["pg-a4"].is_usable_replica
