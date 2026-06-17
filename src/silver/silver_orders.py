from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, to_timestamp, desc, col, assert_true

timestamp_schema = "yyyy-MM-dd HH:mm:ss"

window = Window.partitionBy("order_id").orderBy(desc("order_purchase_timestamp"))

orders_bronze = spark.read.table("ecommerce.bronze.orders")
orders_clean = (orders_bronze
    .withColumn("order_purchase_timestamp",
        to_timestamp(col("order_purchase_timestamp"), timestamp_schema))
    .withColumn("order_approved_at",
        to_timestamp(col("order_approved_at"), timestamp_schema))
    .withColumn("order_delivered_carrier_date",
        to_timestamp(col("order_delivered_carrier_date"), timestamp_schema))
    .withColumn("order_delivered_customer_date",
        to_timestamp(col("order_delivered_customer_date"), timestamp_schema))
    .withColumn("order_estimated_delivery_date",
        to_timestamp(col("order_estimated_delivery_date"), timestamp_schema))
    .withColumn("rn", row_number().over(window))
    .filter(col("rn") == 1)
    .drop("rn")
    .withColumn("_dq_order_id",
        assert_true(col("order_id").isNotNull(), "silver.orders: order_id cannot be null"))
    .drop("_dq_order_id")
    .withColumn("_dq_order_status",
    assert_true(
        col("order_status").isin("delivered", "shipped", "canceled", "invoiced",
                                  "processing", "approved", "unavailable", "created"),
        "silver.orders: unrecognized order_status value"
    ))
    .drop("_dq_order_status")
)

orders_clean.write.mode("overwrite").option("mergeSchema", True).saveAsTable("ecommerce.silver.orders")