# Sales ETL Pipeline

An end-to-end ETL pipeline built with Python and PySpark that processes
100,000+ rows of raw sales data into clean, analytics-ready Parquet files.

## Architecture

Raw CSV → PySpark Extract → Transform → Clean Parquet → SQL Insights

## Tech Stack

- Python 3.8+
- Apache Spark / PySpark
- Pandas (data generation)
- SQL (analytical queries)
- Parquet (output format)

## Project Structure

    src/generate_data.py   — generates 100K synthetic sales rows
    src/extract.py         — reads raw CSV into Spark DataFrame
    src/transform.py       — cleans, filters, enriches data
    src/load.py            — saves as partitioned Parquet
    pipeline.py            — runs the full ETL pipeline
    sql/insights.sql       — analytical SQL queries

## How to Run

    pip install -r requirements.txt
    python src/generate_data.py
    python pipeline.py

## Key Features

- Null removal and duplicate detection
- Revenue calculation and date partitioning
- Window functions for monthly trend analysis
- Modular ETL design with step-by-step logging