# from pyspark.sql import functions as F

# df = spark.read.table("ecommerce.bronze.olist_orders")
# display(spark.sql("select * from ecommerce.bronze.olist_orders"))
# display(df.show(5))
# null_counts = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns])
# display(null_counts)

df = spark.table("ecommerce.bronze.olist_orders")
df.printSchema()
display(df.limit(10))
# DBTITLE 1,Let
