import pandas as pd
import os
from google.cloud import bigquery


def read_from_bronze(client, project_id):
    query = f"""
        select *
        from `{project_id}.bronze.staging_raw_data`
    """
    df = client.query(query).to_dataframe()
    print(f"Read {len(df)} rows from bronze.staging_raw_data")
    return df


def clean_dataframe(df):

    # 1. type casting
    df["order_date"]  = pd.to_datetime(df["order_date"]).dt.date
    df["ship_date"]   = pd.to_datetime(df["ship_date"]).dt.date
    df["row_id"]      = df["row_id"].astype("int64")
    df["quantity"]    = df["quantity"].astype("int64")
    df["sales"]       = df["sales"].astype("float64")
    df["discount"]    = df["discount"].astype("float64")
    df["profit"]      = df["profit"].astype("float64")
    df["postal_code"] = df["postal_code"].astype("str")

    # 2. text standardization
    string_columns = [
        "order_id", "ship_mode", "customer_id", "customer_name",
        "segment", "city", "state", "region", "country",
        "product_id", "product_name", "category", "sub_category"
    ]
    for col in string_columns:
        df[col] = df[col].astype("str").str.strip()

    title_case_columns = [
        "ship_mode", "customer_name", "segment",
        "city", "state", "product_name", "category", "sub_category"
    ]
    for col in title_case_columns:
        df[col] = df[col].str.title()

    df["region"]  = df["region"].str.upper()
    df["country"] = df["country"].str.upper()

    # 3. rounding
    df["sales"]    = df["sales"].round(2)
    df["discount"] = df["discount"].round(4)
    df["profit"]   = df["profit"].round(2)

    # 4. derived columns
    df["days_to_ship"] = (
        pd.to_datetime(df["ship_date"]) - pd.to_datetime(df["order_date"])
    ).dt.days

    df["profit_margin_pct"] = df.apply(
        lambda row: round(row["profit"] / row["sales"] * 100, 2)
        if row["sales"] != 0 else None,
        axis=1
    )

    df["discounted_sales"] = (
        df["sales"] * (1 - df["discount"])
    ).round(2)

    df["is_loss_making"] = df["profit"] < 0

    def categorize_shipping(days):
        if days == 0:
            return "Same Day"
        elif days <= 2:
            return "Fast"
        elif days <= 5:
            return "Standard"
        else:
            return "Slow"

    df["shipping_speed"]   = df["days_to_ship"].apply(categorize_shipping)
    df["order_year"]       = pd.to_datetime(df["order_date"]).dt.year
    df["order_month"]      = pd.to_datetime(df["order_date"]).dt.month
    df["order_year_month"] = pd.to_datetime(
        df["order_date"]
    ).dt.strftime("%Y-%m")

    # 5. data quality filter
    before = len(df)
    df = df[df["order_id"].notna()].copy()
    after = len(df)
    if before != after:
        print(f"Removed {before - after} rows with null order_id")

    df["loaded_at"] = pd.Timestamp.now()

    return df


def validate_cleaned(df):
    checks = {
        "no_null_order_ids":      df["order_id"].isnull().sum() == 0,
        "no_negative_sales":      (df["sales"] >= 0).all(),
        "no_negative_quantity":   (df["quantity"] > 0).all(),
        "valid_dates":            (
            pd.to_datetime(df["ship_date"])
            >= pd.to_datetime(df["order_date"])
        ).all(),
        "valid_discount_range":   df["discount"].between(0, 1).all(),
        "days_to_ship_positive":  (df["days_to_ship"] >= 0).all(),
        "row_count_above_minimum": len(df) > 1000,
    }

    failed = [check for check, passed in checks.items() if not passed]

    if failed:
        raise ValueError(f"Cleaned data validation failed: {failed}")

    print(f"All {len(checks)} data quality checks passed on cleaned data")
    return True


def load_to_silver(df, client, project_id):
    table_id = f"{project_id}.silver.orders_cleaned"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("row_id",            "INTEGER"),
            bigquery.SchemaField("order_id",          "STRING"),
            bigquery.SchemaField("order_date",        "DATE"),
            bigquery.SchemaField("ship_date",         "DATE"),
            bigquery.SchemaField("ship_mode",         "STRING"),
            bigquery.SchemaField("customer_id",       "STRING"),
            bigquery.SchemaField("customer_name",     "STRING"),
            bigquery.SchemaField("segment",           "STRING"),
            bigquery.SchemaField("country",           "STRING"),
            bigquery.SchemaField("city",              "STRING"),
            bigquery.SchemaField("state",             "STRING"),
            bigquery.SchemaField("postal_code",       "STRING"),
            bigquery.SchemaField("region",            "STRING"),
            bigquery.SchemaField("product_id",        "STRING"),
            bigquery.SchemaField("category",          "STRING"),
            bigquery.SchemaField("sub_category",      "STRING"),
            bigquery.SchemaField("product_name",      "STRING"),
            bigquery.SchemaField("quantity",          "INTEGER"),
            bigquery.SchemaField("sales",             "FLOAT64"),
            bigquery.SchemaField("discount",          "FLOAT64"),
            bigquery.SchemaField("profit",            "FLOAT64"),
            bigquery.SchemaField("days_to_ship",      "INTEGER"),
            bigquery.SchemaField("profit_margin_pct", "FLOAT64"),
            bigquery.SchemaField("discounted_sales",  "FLOAT64"),
            bigquery.SchemaField("is_loss_making",    "BOOL"),
            bigquery.SchemaField("shipping_speed",    "STRING"),
            bigquery.SchemaField("order_year",        "INTEGER"),
            bigquery.SchemaField("order_month",       "INTEGER"),
            bigquery.SchemaField("order_year_month",  "STRING"),
            bigquery.SchemaField("loaded_at",         "TIMESTAMP"),
        ]
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"Loaded {len(df)} cleaned rows into {table_id}")


def clean_to_silver():
    client = bigquery.Client()
    project_id = os.environ.get("GCP_PROJECT_ID", client.project)

    df = read_from_bronze(client, project_id)
    df = clean_dataframe(df)
    validate_cleaned(df)
    load_to_silver(df, client, project_id)


if __name__ == "__main__":
    clean_to_silver()