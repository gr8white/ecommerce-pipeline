customers_bronze = spark.read.table("ecommerce.bronze.customers")

customers_bronze.write.mode("overwrite").option("mergeSchema", True).saveAsTable("ecommerce.silver.customers")