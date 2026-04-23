from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_date, month, year, round as spark_round

def transform(df):
    """
    Transform: Clean and enrich the raw sales DataFrame.
    Steps:
      1. Drop rows with nulls in critical columns
      2. Filter only completed orders
      3. Cast columns to correct types
      4. Add revenue, month, year columns
      5. Remove duplicate orders
    """
    print(f"[TRANSFORM] Starting with {df.count()} rows")

    # Step 1 — drop nulls in critical columns
    df = df.dropna(subset=["order_id", "status", "price", "quantity"])
    print(f"[TRANSFORM] After null removal: {df.count()} rows")

    # Step 2 — keep only completed orders
    df = df.filter(col("status") == "completed")
    print(f"[TRANSFORM] After status filter: {df.count()} rows")

    # Step 3 — fix data types
    df = df.withColumn("price",    col("price").cast("double"))
    df = df.withColumn("quantity", col("quantity").cast("integer"))
    df = df.withColumn("date",     to_date(col("date"), "yyyy-MM-dd"))

    # Step 4 — add new columns
    df = df.withColumn("revenue", spark_round(col("quantity") * col("price"), 2))
    df = df.withColumn("month",   month(col("date")))
    df = df.withColumn("year",    year(col("date")))

    # Step 5 — remove duplicate orders
    before = df.count()
    df = df.dropDuplicates(["order_id"])
    print(f"[TRANSFORM] Removed {before - df.count()} duplicates")
    print(f"[TRANSFORM] Final clean rows: {df.count()}")

    return df