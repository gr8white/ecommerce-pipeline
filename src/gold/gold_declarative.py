import dlt
from pyspark.sql.functions import date_trunc, col, sum as spark_sum, approx_count_distinct

@dlt.table(name="monthly_revenue", comment="Monthly revenue by product category")
def monthly_revenue():
    orders = dlt.read("ecommerce.silver.orders")
    items  = dlt.read("ecommerce.silver.order_items")
    products = dlt.read("ecommerce.silver.products")

    return (
        orders
        .filter(orders.order_status == "delivered")
        .join(items, "order_id")
        .join(products, "product_id")
        .groupBy(
            date_trunc("month", orders.order_purchase_timestamp).alias("revenue_month"),
            products.product_category_name_english.alias("category")
        )
        .agg(
            spark_sum(items.price + items.freight_value).alias("total_revenue"),
            approx_count_distinct(orders.order_id).alias("total_orders")
        )
    )

@dlt.table(name="live_orders", comment="Active orders in processing or shipped status")
def live_orders():
    return (
        spark.readStream.table("ecommerce.silver.orders")
        .filter(col("order_status").isin("processing", "shipped"))
        .select(
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_estimated_delivery_date"
        )
    )