from pyspark.sql import SparkSession

def extract(spark, path):
    """
    Extract: Read raw CSV file into a PySpark DataFrame.
    Returns the raw DataFrame with schema inferred automatically.
    """
    print(f"[EXTRACT] Reading from {path}")

    df = spark.read.csv(
        path,
        header=True,
        inferSchema=True
    )

    print(f"[EXTRACT] Rows loaded: {df.count()}")
    print(f"[EXTRACT] Columns: {df.columns}")
    df.printSchema()

    return df