from pyspark.sql import SparkSession
from src.extract   import extract
from src.transform import transform
from src.load      import load
import time

def run_pipeline():
    print("=" * 45)
    print("  Sales ETL Pipeline — Starting")
    print("=" * 45)
    start = time.time()

    # Create Spark session
    spark = SparkSession.builder \
        .appName("SalesETLPipeline") \
        .master("local[*]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")  # hide verbose Spark logs

    # Run ETL steps
    raw_df   = extract(spark,  "data/raw/sales.csv")
    clean_df = transform(raw_df)
    load(clean_df, "data/processed/sales_clean")

    elapsed = round(time.time() - start, 2)
    print("=" * 45)
    print(f"  Pipeline complete in {elapsed}s")
    print("=" * 45)
    spark.stop()

if __name__ == "__main__":
    run_pipeline()