import pandas as pd
import os
from google.cloud import bigquery


def validate_data(df):
    checks = {
        "no_null_order_ids": df["order_id"].isnull().sum() == 0,
        "no_negative_sales": (df["sales"] >= 0).all(),
        "valid_dates": (df["order_date"] <= df["ship_date"]).all(),
        "row_count_above_minimum": len(df) > 1000,
        "discount_percentage_within_range": df["discount"].between(0, 1).all()
    }

    failed = [check for check, passed in checks.items() if not passed]

    if failed:
        raise ValueError(f"Data quality checks failed: {failed}")

    print(f"All {len(checks)} data quality checks passed")
    return True


def load_superstore_to_bronze():
    client = bigquery.Client()
    project_id = os.environ.get("GCP_PROJECT_ID", client.project)

    df = pd.read_csv("/opt/airflow/data/superstore.csv", encoding="latin-1")

    # standardize column names
    df.columns = (
        df.columns
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )

    # parse dates
    df["order_date"] = pd.to_datetime(df["order_date"], format="%m/%d/%Y")
    df["ship_date"]  = pd.to_datetime(df["ship_date"],  format="%m/%d/%Y")

    # explicitly cast every column to match the BigQuery schema exactly
    # this is what prevents the ArrowTypeError during parquet conversion
    df["row_id"]        = df["row_id"].astype("int64")
    df["order_id"]      = df["order_id"].astype("str")
    df["ship_mode"]     = df["ship_mode"].astype("str")
    df["customer_id"]   = df["customer_id"].astype("str")
    df["customer_name"] = df["customer_name"].astype("str")
    df["segment"]       = df["segment"].astype("str")
    df["country"]       = df["country"].astype("str")
    df["city"]          = df["city"].astype("str")
    df["state"]         = df["state"].astype("str")
    df["postal_code"]   = df["postal_code"].astype("str")  # int64 → str, fixes the error
    df["region"]        = df["region"].astype("str")
    df["product_id"]    = df["product_id"].astype("str")
    df["category"]      = df["category"].astype("str")
    df["sub_category"]  = df["sub_category"].astype("str")
    df["product_name"]  = df["product_name"].astype("str")
    df["sales"]         = df["sales"].astype("float64").round(2)
    df["quantity"]      = df["quantity"].astype("int64")
    df["discount"]      = df["discount"].astype("float64").round(4)
    df["profit"]        = df["profit"].astype("float64").round(2)

    validate_data(df)

    table_id = f"{project_id}.bronze.staging_raw_data"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("row_id",        "INTEGER"),
            bigquery.SchemaField("order_id",       "STRING"),
            bigquery.SchemaField("order_date",     "DATE"),
            bigquery.SchemaField("ship_date",      "DATE"),
            bigquery.SchemaField("ship_mode",      "STRING"),
            bigquery.SchemaField("customer_id",    "STRING"),
            bigquery.SchemaField("customer_name",  "STRING"),
            bigquery.SchemaField("segment",        "STRING"),
            bigquery.SchemaField("country",        "STRING"),
            bigquery.SchemaField("city",           "STRING"),
            bigquery.SchemaField("state",          "STRING"),
            bigquery.SchemaField("postal_code",    "STRING"),
            bigquery.SchemaField("region",         "STRING"),
            bigquery.SchemaField("product_id",     "STRING"),
            bigquery.SchemaField("category",       "STRING"),
            bigquery.SchemaField("sub_category",   "STRING"),
            bigquery.SchemaField("product_name",   "STRING"),
            bigquery.SchemaField("sales",          "FLOAT64"),
            bigquery.SchemaField("quantity",       "INTEGER"),
            bigquery.SchemaField("discount",       "FLOAT64"),
            bigquery.SchemaField("profit",         "FLOAT64"),
        ]
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    print(f"Loaded {len(df)} rows into {table_id}")


if __name__ == "__main__":
    load_superstore_to_bronze()