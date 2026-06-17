MINIMUM_COUNTS = {
    "ecommerce.bronze.customers": 99441,       # replace with your actual count
    "ecommerce.bronze.order_items": 112650,
    "ecommerce.bronze.geolocation": 1000163,
    "ecommerce.bronze.order_payments": 103886,
    "ecommerce.bronze.order_reviews": 99224,
    "ecommerce.bronze.orders": 99441,
    "ecommerce.bronze.products": 32951,
    "ecommerce.bronze.sellers": 3095,
    "ecommerce.bronze.product_category_translations": 71,
}

for table, minimum in MINIMUM_COUNTS.items():
    actual = spark.read.table(table).count()
    if actual < minimum:
        raise ValueError(f"Data quality failure: {table} has {actual} rows, expected >= {minimum}")

print("All bronze table counts validated.")