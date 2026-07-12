import pandas as pd
import os
from google.cloud import bigquery


def validate_raw(df):
    checks = {
        "file_has_rows": len(df) > 0,
        "order_id_column_exists": "order_id" in df.columns,
        "minimum_row_count": len(df) > 1000,
    }

    failed = [check for check, passed in checks.items() if not passed]

    if failed:
        raise ValueError(f"Raw file validation failed: {failed}")

    print(f"All {len(checks)} raw file checks passed")
    return True


def load_raw_to_bronze():
    client = bigquery.Client()
    project_id = os.environ.get("GCP_PROJECT_ID", client.project)

    df = pd.read_csv("/opt/airflow/data/superstore.csv", encoding="latin-1")

    df.columns = (
        df.columns
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )

    validate_raw(df)

    table_id = f"{project_id}.bronze.staging_raw_data"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    print(f"Loaded {len(df)} rows into {table_id}")
    print(f"Columns: {list(df.columns)}")


if __name__ == "__main__":
    load_raw_to_bronze()