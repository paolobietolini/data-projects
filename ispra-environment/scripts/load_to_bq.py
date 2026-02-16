import subprocess
import json
from pathlib import Path

import duckdb
from google.cloud import bigquery
from google.oauth2 import service_account

ROOT = Path(__file__).parent.parent
DBT_DIR = ROOT / "dbt"
DUCKDB_PATH = ROOT / "ispra.duckdb"
SECRETS_PATH = ROOT.parent / "secrets" / "secret_zcmp-final.json"


def load_credentials():
    with open(SECRETS_PATH) as f:
        return json.load(f)


def run_dbt():
    subprocess.run(["uv", "run", "dbt", "build"], cwd=str(DBT_DIR), check=True)


def get_bq_client(credentials_info):
    project_id = credentials_info["project_id"]
    credentials = service_account.Credentials.from_service_account_info(credentials_info)
    return bigquery.Client(project=project_id, credentials=credentials)


def ensure_dataset(client, dataset_name):
    dataset_id = f"{client.project}.{dataset_name}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "EU"
    client.create_dataset(dataset, exists_ok=True)
    return dataset_id


def get_fact_tables(con):
    tables = con.sql("SHOW TABLES").fetchall()
    return [name for (name,) in tables if name.startswith("fct_")]


def load_tables_to_bq(con, client, dataset_id, tables):
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    for table in tables:
        print(f"Loading {table}...")
        df = con.sql(f"SELECT * FROM {table}").fetchdf()
        job = client.load_table_from_dataframe(
            df, f"{dataset_id}.{table}", job_config=job_config
        )
        job.result()
        print(f"  {len(df)} rows loaded.")


def main():
    creds = load_credentials()
    run_dbt()
    client = get_bq_client(creds)
    dataset_id = ensure_dataset(client, "ispra")

    with duckdb.connect(DUCKDB_PATH) as con:
        tables = get_fact_tables(con)
        load_tables_to_bq(con, client, dataset_id, tables)


if __name__ == "__main__":
    main()