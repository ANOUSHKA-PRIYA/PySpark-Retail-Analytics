"""Small end-to-end test using the tracked demo dataset."""

import json
from pathlib import Path

from retail_analytics.job import run_pipeline

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_pipeline_writes_expected_outputs(spark, tmp_path):
    output_dir = tmp_path / "output"

    metrics = run_pipeline(spark, PROJECT_ROOT / "data" / "raw", output_dir)

    assert metrics == {
        "input_orders": 10,
        "deduplicated_orders": 9,
        "invalid_orders": 2,
        "missing_dimension_orders": 2,
        "rejected_orders": 4,
        "curated_orders": 5,
        "daily_report_rows": 4,
        "category_report_rows": 5,
    }
    assert (output_dir / "curated" / "sales").is_dir()
    assert (output_dir / "reports" / "daily_sales").is_dir()
    assert (output_dir / "reports" / "category_sales").is_dir()
    assert (output_dir / "rejected" / "orders").is_dir()
    assert json.loads((output_dir / "run_summary.json").read_text()) == metrics
