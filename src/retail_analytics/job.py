"""Command-line entry point for the retail analytics pipeline."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from retail_analytics.schemas import CUSTOMERS_SCHEMA, ORDERS_SCHEMA, PRODUCTS_SCHEMA
from retail_analytics.transforms import (
    build_category_sales,
    build_daily_sales,
    enrich_orders,
    normalize_orders,
    select_latest_orders,
    split_valid_orders,
)


def read_csv(spark: SparkSession, path: Path, schema) -> DataFrame:
    """Read a header-based CSV using an explicit schema."""
    return spark.read.option("header", True).schema(schema).csv(str(path))


def run_pipeline(
    spark: SparkSession,
    input_dir: Path,
    output_dir: Path,
) -> dict[str, int]:
    """Run the complete pipeline and return observable row-count metrics."""
    orders = normalize_orders(read_csv(spark, input_dir / "orders.csv", ORDERS_SCHEMA))
    customers = read_csv(spark, input_dir / "customers.csv", CUSTOMERS_SCHEMA)
    products = read_csv(spark, input_dir / "products.csv", PRODUCTS_SCHEMA)

    input_order_count = orders.count()
    latest_orders = select_latest_orders(orders)
    deduplicated_order_count = latest_orders.count()
    valid_orders, invalid_orders = split_valid_orders(latest_orders)
    sales, missing_dimension_orders = enrich_orders(valid_orders, customers, products)

    daily_sales = build_daily_sales(sales)
    category_sales = build_category_sales(sales)
    rejected_orders = invalid_orders.unionByName(missing_dimension_orders)

    sales.write.mode("overwrite").partitionBy("order_date").parquet(
        str(output_dir / "curated" / "sales")
    )
    daily_sales.coalesce(1).write.mode("overwrite").option("header", True).csv(
        str(output_dir / "reports" / "daily_sales")
    )
    category_sales.coalesce(1).write.mode("overwrite").option("header", True).csv(
        str(output_dir / "reports" / "category_sales")
    )
    rejected_orders.write.mode("overwrite").json(str(output_dir / "rejected" / "orders"))

    metrics = {
        "input_orders": input_order_count,
        "deduplicated_orders": deduplicated_order_count,
        "invalid_orders": invalid_orders.count(),
        "missing_dimension_orders": missing_dimension_orders.count(),
        "rejected_orders": rejected_orders.count(),
        "curated_orders": sales.count(),
        "daily_report_rows": daily_sales.count(),
        "category_report_rows": category_sales.count(),
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "run_summary.json").write_text(
        json.dumps(metrics, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return metrics


def create_spark_session() -> SparkSession:
    """Create a local Spark session with deterministic demo settings."""
    python_executable = os.getenv("PYSPARK_PYTHON", sys.executable)
    return (
        SparkSession.builder.appName("pyspark-retail-analytics")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.pyspark.python", python_executable)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build curated retail sales data and daily analytics reports."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("data/raw"),
        help="Directory containing orders.csv, customers.csv, and products.csv.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output"),
        help="Directory where generated datasets will be written.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    try:
        metrics = run_pipeline(spark, args.input_dir, args.output_dir)
        print(json.dumps(metrics, indent=2, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
