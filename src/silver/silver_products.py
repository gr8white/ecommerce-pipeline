from pyspark.sql.functions import col, cast, broadcast, assert_true

products_bronze = spark.read.table("ecommerce.bronze.products")
product_category_translations = spark.read.table("ecommerce.bronze.product_category_translations")

products_join = products_bronze.join(broadcast(product_category_translations), "product_category_name", "left")

products_clean = (products_join
    .select(
        col("product_id"),
        col("product_category_name"),
        col("product_category_name_english"),
        col("product_name_lenght").cast("integer"),
        col("product_description_lenght").cast("integer"),
        col("product_photos_qty").cast("integer"),
        col("product_weight_g").cast("double"),
        col("product_length_cm").cast("double"),
        col("product_height_cm").cast("double"),
        col("product_width_cm").cast("double"),
        col("products._source_file"),
        col("products._ingested_at")
    )
    .withColumn("_dq_product_weight_g",
        assert_true(
             col("product_weight_g").isNull() | (col("product_weight_g") > 0),
            "silver.products: product_weight_g must be non-negative"
        )
    )
    .drop("_dq_product_weight_g")
)

products_clean.write.mode("overwrite").option("mergeSchema", True).saveAsTable("ecommerce.silver.products")