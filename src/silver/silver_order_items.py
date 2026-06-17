from pyspark.sql.functions import col, to_timestamp, assert_true

timestamp_schema = "yyyy-MM-dd HH:mm:ss"

order_items_bronze = spark.read.table("ecommerce.bronze.order_items")

order_items_clean = (order_items_bronze
    .withColumn("price", col("price").cast("double"))
    .withColumn("freight_value", col("freight_value").cast("double"))
    .withColumn("shipping_limit_date",
        to_timestamp(col("shipping_limit_date"), timestamp_schema))
    .withColumn("_dq_seller_id",
        assert_true(col("seller_id").isNotNull(), "silver.order_items: seller_id cannot be null"))
    .drop("_dq_seller_id")
    .withColumn("_dq_price",
        assert_true(col("price") >= 0, "silver.order_items: price must be non-negative"))
    .drop("_dq_price")
)

order_items_clean.write.mode("overwrite").option("mergeSchema", True).saveAsTable("ecommerce.silver.order_items")