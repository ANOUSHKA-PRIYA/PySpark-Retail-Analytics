"""Unit tests for business transformations and data-quality rules."""

from datetime import datetime
from decimal import Decimal

from retail_analytics.schemas import ORDERS_SCHEMA
from retail_analytics.transforms import select_latest_orders, split_valid_orders


def test_select_latest_orders_keeps_newest_update(spark):
    orders = spark.createDataFrame(
        [
            (
                "O1",
                "C1",
                "P1",
                datetime(2026, 9, 1, 10, 0),
                1,
                Decimal("0.0000"),
                datetime(2026, 9, 1, 10, 5),
            ),
            (
                "O1",
                "C1",
                "P1",
                datetime(2026, 9, 1, 10, 0),
                1,
                Decimal("0.1000"),
                datetime(2026, 9, 1, 10, 10),
            ),
        ],
        ORDERS_SCHEMA,
    )

    latest = select_latest_orders(orders).first()

    assert latest.order_id == "O1"
    assert latest.discount_pct == Decimal("0.1000")


def test_split_valid_orders_collects_all_rejection_reasons(spark):
    orders = spark.createDataFrame(
        [
            (
                "O1",
                "C1",
                "P1",
                datetime(2026, 9, 1, 10, 0),
                2,
                Decimal("0.0500"),
                datetime(2026, 9, 1, 10, 5),
            ),
            (
                "O2",
                "C2",
                "P2",
                datetime(2026, 9, 1, 11, 0),
                0,
                Decimal("1.2500"),
                datetime(2026, 9, 1, 11, 5),
            ),
        ],
        ORDERS_SCHEMA,
    )

    valid, rejected = split_valid_orders(orders)
    rejected_row = rejected.first()

    assert valid.count() == 1
    assert rejected.count() == 1
    assert "quantity must be positive" in rejected_row.rejection_reason
    assert "discount_pct must be between 0 and 1" in rejected_row.rejection_reason
