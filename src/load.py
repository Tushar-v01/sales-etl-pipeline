def load(df, output_path):
    """
    Load: Save the cleaned DataFrame as Parquet,
    partitioned by year and month for fast querying.
    """
    print(f"[LOAD] Saving to {output_path}")

    df.write \
      .mode("overwrite") \
      .partitionBy("year", "month") \
      .parquet(output_path)

    print(f"[LOAD] Saved successfully as Parquet")
    print(f"[LOAD] Partitioned by year and month")