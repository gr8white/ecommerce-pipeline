sellers_bronze = spark.read.table("ecommerce.bronze.sellers")

sellers_bronze.write.mode("overwrite").option("mergeSchema", True).saveAsTable("ecommerce.silver.sellers")