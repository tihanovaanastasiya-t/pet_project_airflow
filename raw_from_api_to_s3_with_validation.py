import logging
import pandas as pd
import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import great_expectations as ge

# DAG config
OWNER = "a.tihanova"
DAG_ID = "raw_from_api_to_s3_with_validation"

LAYER = "raw"
SOURCE = "earthquake"

# S3
ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 3, 8, tz="Europe/Moscow"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}


# даты
def get_dates(**context):
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")
    return start_date, end_date


# твоя функция (НЕ МЕНЯЕМ)
def get_and_transfer_api_data_to_s3(**context):
    start_date, end_date = get_dates(**context)
    logging.info(f"💻 Start load for dates: {start_date}/{end_date}")

    con = duckdb.connect()

    con.sql(f"""
        SET TIMEZONE='UTC';
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
        SET s3_use_ssl = FALSE;

        COPY (
            SELECT *
            FROM read_csv_auto(
                'https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&starttime={start_date}&endtime={end_date}'
            )
        )
        TO 's3://prod/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet';
    """)

    con.close()
    logging.info(f"✅ Download success: {start_date}")


# Validation
def validate_earthquake_data(**context):
    start_date, _ = get_dates(**context)
    logging.info(f"🔍 Validate data: {start_date}")

    con = duckdb.connect()

    # подключаемся к S3
    con.sql(f"""
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_url_style='path';
        SET s3_endpoint='minio:9000';
        SET s3_access_key_id='{ACCESS_KEY}';
        SET s3_secret_access_key='{SECRET_KEY}';
        SET s3_use_ssl=FALSE;
    """)

    # читаем parquet
    df = con.execute(f"""
        SELECT *
        FROM 's3://prod/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet'
    """).fetchdf()

    # простое приведение типов
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df["mag"] = pd.to_numeric(df["mag"], errors="coerce")

    ge_df = ge.from_pandas(df)

    # -----------------------
    # базовые проверки
    # -----------------------
    for col in ["time", "latitude", "longitude", "mag"]:
        if not ge_df.expect_column_values_to_not_be_null(col)["success"]:
            raise ValueError(f"{col} has NULL values")

    # уникальность id
    if "id" in df.columns:
        if not ge_df.expect_column_values_to_be_unique("id")["success"]:
            raise ValueError("Duplicate id found")

    # -----------------------
    # диапазоны значений
    # -----------------------
    ge_df.expect_column_values_to_be_between("latitude", -90, 90)
    ge_df.expect_column_values_to_be_between("longitude", -180, 180)
    ge_df.expect_column_values_to_be_between("mag", 0, 10)

    # -----------------------
    # простая логика
    # -----------------------
    if (df["mag"] > 12).any():
        raise ValueError("Invalid magnitude values")

    # дата не в будущем
    if df["time"].max() > pd.Timestamp.utcnow():
        raise ValueError("Future dates detected")

    # -----------------------
    # объем данных
    # -----------------------
    if len(df) < 10:
        raise ValueError("Too few records")

    # -----------------------
    # простой outlier
    # -----------------------
    if df["mag"].mean() > 8:
        raise ValueError("Strange average magnitude")

    logging.info("✅ Validation passed")
    con.close()


with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 5 * * *",
    default_args=args,
    tags=["s3", "raw", "validation"],
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    load_task = PythonOperator(
        task_id="get_and_transfer_api_data_to_s3",
        python_callable=get_and_transfer_api_data_to_s3,
    )

    validate_task = PythonOperator(
        task_id="validate_earthquake_data",
        python_callable=validate_earthquake_data,
    )

    end = EmptyOperator(task_id="end")

    start >> load_task >> validate_task >> end