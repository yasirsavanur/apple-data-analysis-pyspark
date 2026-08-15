"""Pure PySpark business transformations for Apple purchase analysis."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window, functions as F


PURCHASE_WINDOW = Window.partitionBy("customer_id").orderBy(
    F.col("transaction_date"), F.col("transaction_id")
)


def with_next_purchase(transactions: DataFrame) -> DataFrame:
    return (
        transactions.withColumn("next_product", F.lead("product_name").over(PURCHASE_WINDOW))
        .withColumn("next_purchase_date", F.lead("transaction_date").over(PURCHASE_WINDOW))
        .withColumn("next_transaction_id", F.lead("transaction_id").over(PURCHASE_WINDOW))
    )


def iphone_then_airpods(transactions: DataFrame, customers: DataFrame) -> DataFrame:
    """Customers whose immediate next purchase after an iPhone was AirPods."""

    qualifying = (
        with_next_purchase(transactions)
        .filter(
            (F.col("product_name") == "iPhone")
            & (F.col("next_product") == "AirPods")
        )
        .select(
            "customer_id",
            F.col("transaction_id").alias("iphone_transaction_id"),
            F.col("next_transaction_id").alias("airpods_transaction_id"),
            F.col("transaction_date").alias("iphone_purchase_date"),
            F.col("next_purchase_date").alias("airpods_purchase_date"),
            F.datediff("next_purchase_date", "transaction_date").alias("days_between"),
        )
    )
    return (
        customers.join(F.broadcast(qualifying), "customer_id", "inner")
        .select(
            "customer_id",
            "customer_name",
            "location",
            "iphone_transaction_id",
            "airpods_transaction_id",
            "iphone_purchase_date",
            "airpods_purchase_date",
            "days_between",
        )
        .orderBy("customer_id")
    )


def only_iphone_and_airpods(transactions: DataFrame, customers: DataFrame) -> DataFrame:
    """Customers whose complete product set equals iPhone plus AirPods."""

    allowed_products = F.array_sort(F.array(F.lit("iPhone"), F.lit("AirPods")))
    product_sets = transactions.groupBy("customer_id").agg(
        F.array_sort(F.collect_set("product_name")).alias("products")
    )
    qualifying = product_sets.filter(
        (F.size("products") == 2) & (F.col("products") == allowed_products)
    )
    return (
        customers.join(F.broadcast(qualifying), "customer_id", "inner")
        .select("customer_id", "customer_name", "location", "products")
        .orderBy("customer_id")
    )


def purchases_after_first(transactions: DataFrame, customers: DataFrame) -> DataFrame:
    """Return each customer's first purchase and ordered later purchases."""

    ranked = transactions.withColumn(
        "purchase_rank", F.row_number().over(PURCHASE_WINDOW)
    )
    first = ranked.filter(F.col("purchase_rank") == 1).select(
        "customer_id",
        F.col("product_name").alias("first_product"),
        F.col("transaction_date").alias("first_purchase_date"),
    )
    later = (
        ranked.filter(F.col("purchase_rank") > 1)
        .groupBy("customer_id")
        .agg(
            F.sort_array(
                F.collect_list(
                    F.struct("purchase_rank", "transaction_date", "product_name")
                )
            ).alias("subsequent_purchase_records")
        )
        .withColumn(
            "subsequent_products",
            F.expr("transform(subsequent_purchase_records, x -> x.product_name)"),
        )
        .drop("subsequent_purchase_records")
    )
    return (
        customers.join(first, "customer_id", "inner")
        .join(later, "customer_id", "left")
        .withColumn(
            "subsequent_products",
            F.coalesce("subsequent_products", F.expr("array()")),
        )
        .select(
            "customer_id",
            "customer_name",
            "first_product",
            "first_purchase_date",
            "subsequent_products",
        )
        .orderBy("customer_id")
    )


def average_iphone_to_airpods_delay(iphone_airpods: DataFrame) -> DataFrame:
    return iphone_airpods.agg(
        F.round(F.avg("days_between"), 2).alias("average_days_iphone_to_airpods"),
        F.count("customer_id").alias("qualifying_sequences"),
    )


def top_products_by_revenue(transactions: DataFrame, products: DataFrame) -> DataFrame:
    """Rank the three highest revenue products using the prepared product dimension."""

    sales = (
        transactions.join(F.broadcast(products), "product_name", "inner")
        .groupBy("product_id", "product_name", "source_product_name", "category", "price")
        .agg(
            F.count("transaction_id").alias("units_sold"),
            F.round(F.sum("price"), 2).alias("revenue"),
        )
    )
    rank_window = Window.orderBy(F.desc("revenue"), F.desc("units_sold"), "product_name")
    return (
        sales.withColumn("revenue_rank", F.dense_rank().over(rank_window))
        .filter(F.col("revenue_rank") <= 3)
        .orderBy("revenue_rank", "product_name")
    )
