"""Reusable transformations for the retail analytics job."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

MONEY = DecimalType(14, 2)


def normalize_orders(orders: DataFrame) -> DataFrame:
    """Trim business keys before deduplication and validation."""
    return (
        orders.withColumn("order_id", F.trim("order_id"))
        .withColumn("customer_id", F.trim("customer_id"))
        .withColumn("product_id", F.trim("product_id"))
    )


def select_latest_orders(orders: DataFrame) -> DataFrame:
    """Keep the most recently updated record for each business order ID."""
    latest_first = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc_nulls_last())
    return (
        orders.withColumn("_row_number", F.row_number().over(latest_first))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )


def split_valid_orders(orders: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split records into valid and rejected data-quality streams."""
    reason_columns = [
        F.when(
            F.col("order_id").isNull() | (F.length("order_id") == 0),
            F.lit("missing order_id"),
        ),
        F.when(
            F.col("customer_id").isNull() | (F.length("customer_id") == 0),
            F.lit("missing customer_id"),
        ),
        F.when(
            F.col("product_id").isNull() | (F.length("product_id") == 0),
            F.lit("missing product_id"),
        ),
        F.when(F.col("order_ts").isNull(), F.lit("invalid order_ts")),
        F.when(
            F.col("quantity").isNull() | (F.col("quantity") <= 0),
            F.lit("quantity must be positive"),
        ),
        F.when(
            F.col("discount_pct").isNull()
            | ~F.col("discount_pct").between(F.lit(0), F.lit(1)),
            F.lit("discount_pct must be between 0 and 1"),
        ),
        F.when(F.col("updated_at").isNull(), F.lit("invalid updated_at")),
    ]

    checked = orders.withColumn(
        "rejection_reason",
        F.concat_ws("; ", *reason_columns),
    )
    valid = checked.filter(F.length("rejection_reason") == 0).drop("rejection_reason")
    rejected = checked.filter(F.length("rejection_reason") > 0)
    return valid, rejected


def enrich_orders(
    orders: DataFrame,
    customers: DataFrame,
    products: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Join dimensions, quarantine missing keys, and calculate sales amounts."""
    customer_dimension = customers.select(
        "customer_id",
        "customer_name",
        "segment",
        "state",
        F.lit(True).alias("_customer_found"),
    )
    product_dimension = products.select(
        "product_id",
        "product_name",
        "category",
        "unit_price",
        F.lit(True).alias("_product_found"),
    )

    joined = orders.join(customer_dimension, "customer_id", "left").join(
        product_dimension,
        "product_id",
        "left",
    )
    joined = joined.withColumn(
        "rejection_reason",
        F.concat_ws(
            "; ",
            F.when(F.col("_customer_found").isNull(), F.lit("unknown customer_id")),
            F.when(F.col("_product_found").isNull(), F.lit("unknown product_id")),
        ),
    )

    orphaned = joined.filter(F.length("rejection_reason") > 0).select(
        *orders.columns,
        "rejection_reason",
    )
    enriched = joined.filter(F.length("rejection_reason") == 0)

    gross_amount = F.bround(F.col("quantity") * F.col("unit_price"), 2).cast(MONEY)
    discount_amount = F.bround(gross_amount * F.col("discount_pct"), 2).cast(MONEY)

    sales = (
        enriched.withColumn("order_date", F.to_date("order_ts"))
        .withColumn("gross_amount", gross_amount)
        .withColumn("discount_amount", discount_amount)
        .withColumn(
            "net_amount",
            F.bround(F.col("gross_amount") - F.col("discount_amount"), 2).cast(MONEY),
        )
        .select(
            "order_id",
            "order_ts",
            "order_date",
            "customer_id",
            "customer_name",
            "segment",
            "state",
            "product_id",
            "product_name",
            "category",
            "quantity",
            "unit_price",
            "discount_pct",
            "gross_amount",
            "discount_amount",
            "net_amount",
            "updated_at",
        )
    )
    return sales, orphaned


def build_daily_sales(sales: DataFrame) -> DataFrame:
    """Create one row of sales KPIs per order date."""
    return (
        sales.groupBy("order_date")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("quantity").alias("units_sold"),
            F.bround(F.sum("net_amount"), 2).cast(MONEY).alias("revenue"),
        )
        .withColumn(
            "average_order_value",
            F.bround(F.col("revenue") / F.col("total_orders"), 2).cast(MONEY),
        )
        .orderBy("order_date")
    )


def build_category_sales(sales: DataFrame) -> DataFrame:
    """Aggregate category revenue and rank categories within each day."""
    category_totals = sales.groupBy("order_date", "category").agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("quantity").alias("units_sold"),
        F.bround(F.sum("net_amount"), 2).cast(MONEY).alias("revenue"),
    )
    daily_revenue_rank = Window.partitionBy("order_date").orderBy(
        F.col("revenue").desc(),
        F.col("category"),
    )
    return category_totals.withColumn(
        "daily_revenue_rank",
        F.dense_rank().over(daily_revenue_rank),
    ).orderBy("order_date", "daily_revenue_rank")
