from pyspark.sql.functions import col, current_timestamp

# path = "/Volumes/ecommerce/bronze/raw_files/olist_orders_dataset.csv"
# orders_df = (spark.read
#              .option("header", True)
#              .option("inferSchema", True)
#              .option("quote", '"')
#              .csv(path)
#              .withColumn("_source_file", col("_metadata.file_path"))
#              .withColumn("_ingested_at", current_timestamp())
#              )
# orders_df.show()

# orders_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("ecommerce.bronze.olist_orders")
# .mode("append") for appending new data to the target table

VOLUME = "/Volumes/ecommerce/bronze/raw_files"

files = [
    ("olist_customers_dataset.csv",            "customers"),
    ("olist_geolocation_dataset.csv",          "geolocation"),
    ("olist_order_items_dataset.csv",          "order_items"),
    ("olist_order_payments_dataset.csv",       "order_payments"),
    ("olist_order_reviews_dataset.csv",        "order_reviews"),
    ("olist_orders_dataset.csv",               "orders"),
    ("olist_products_dataset.csv",             "products"),
    ("olist_sellers_dataset.csv",              "sellers"),
    ("product_category_name_translation.csv",  "product_category_translations"),
]

for filename, table in files:
    df = (spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "true")   # reviews file has newlines in comment text
        .option("encoding", "UTF-8")   # Portuguese characters in reviews
        .csv(f"{VOLUME}/{filename}")
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_source_modified_at", col("_metadata.file_modification_time"))
        .withColumn("_ingested_at", current_timestamp())
    )

    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"ecommerce.bronze.{table}")
    print(f"{table}: {df.count()} rows")