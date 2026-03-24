import logging
import pandas as pd
import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
import great_expectations as ge

# DAG configuration
OWNER = "a.tihanova"
DAG_ID = "raw_from_s3_to_pg_with_validation"

LAYER = "raw"
SOURCE = "earthquake"
SCHEMA = "ods"
TARGET_TABLE = "fct_earthquake"

ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")
PASSWORD = Variable.get("pg_password")

LONG_DESCRIPTION = "Load earthquake data from S3 to Postgres with validation"
SHORT_DESCRIPTION = "Earthquake ETL with data quality checks"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 3, 8, tz="Europe/Moscow"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}

def get_dates(**context) -> tuple[str, str]:
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")
    return start_date, end_date

def get_and_transfer_raw_data_to_ods_pg(**context):
    start_date, end_date = get_dates(**context)
    logging.info(f"💻 Start load for dates: {start_date}/{end_date}")
    con = duckdb.connect()

    con.sql(
        f"""
        SET TIMEZONE='UTC';
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
        SET s3_use_ssl = FALSE;

        CREATE SECRET dwh_postgres (
            TYPE postgres,
            HOST 'postgres_dwh',
            PORT 5432,
            DATABASE postgres,
            USER 'postgres',
            PASSWORD '{PASSWORD}'
        );

        ATTACH '' AS dwh_postgres_db (TYPE postgres, SECRET dwh_postgres);

        INSERT INTO dwh_postgres_db.{SCHEMA}.{TARGET_TABLE}
        (
            time,
            latitude,
            longitude,
            depth,
            mag,
            mag_type,
            nst,
            gap,
            dmin,
            rms,
            net,
            id,
            updated,
            place,
            type,
            horizontal_error,
            depth_error,
            mag_error,
            mag_nst,
            status,
            location_source,
            mag_source
        )
        SELECT
            time,
            latitude,
            longitude,
            depth,
            mag,
            magType AS mag_type,
            nst,
            gap,
            dmin,
            rms,
            net,
            id,
            updated,
            place,
            type,
            horizontalError AS horizontal_error,
            depthError AS depth_error,
            magError AS mag_error,
            magNst AS mag_nst,
            status,
            locationSource AS location_source,
            magSource AS mag_source
        FROM 's3://prod/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet';
        """,
    )

    con.close()
    logging.info(f"✅ Download for date success: {start_date}")

# Data validation function
def validate_earthquake_data(**context):
    start_date, _ = get_dates(**context)
    logging.info(f"🔍 Validating data for {start_date}")

    con = duckdb.connect()

    # connect to S3
    con.sql(f"""
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_url_style='path';
        SET s3_endpoint='minio:9000';
        SET s3_access_key_id='{ACCESS_KEY}';
        SET s3_secret_access_key='{SECRET_KEY}';
        SET s3_use_ssl=FALSE;
    """)

    # read parquet into pandas
    df = con.execute(
        f"SELECT * FROM 's3://prod/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet'"
    ).fetchdf()

    # convert columns
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df["mag"] = pd.to_numeric(df["mag"], errors="coerce")
    df["depth"] = pd.to_numeric(df["depth"], errors="coerce")

    ge_df = ge.from_pandas(df)

    # 1. Null checks. Ensure critical columns have no nulls
    for col in ["time", "latitude", "longitude", "mag", "depth"]:
        if not ge_df.expect_column_values_to_not_be_null(col)["success"]:
            raise ValueError(f"{col} contains NULL values!")

    # 2. Unique IDs. Id should be unique for each earthquake
    if "id" in df.columns:
        if not ge_df.expect_column_values_to_be_unique("id")["success"]:
            raise ValueError("Duplicate IDs found!")

    # 3. Value ranges
    # Latitude [-90, 90], Longitude [-180, 180]
    ge_df.expect_column_values_to_be_between("latitude", -90, 90)
    ge_df.expect_column_values_to_be_between("longitude", -180, 180)
    # Magnitude 0-10
    ge_df.expect_column_values_to_be_between("mag", 0, 10)
    # Depth realistic range (-10 km for weird measurements, max ~700 km)
    ge_df.expect_column_values_to_be_between("depth", -10, 700)

    # 4. Business logic checks
    # Depth cannot be extremely negative
    if (df["depth"] < -10).any():
        raise ValueError("Depth values too negative!")
    # Depth cannot exceed maximum geophysical depth
    if (df["depth"] > 700).any():
        raise ValueError("Depth values too high!")

    # Magnitude cannot exceed 10
    if (df["mag"] > 10).any():
        raise ValueError("Invalid magnitude values!")

    # Time cannot be in the future
    if df["time"].max() > pd.Timestamp.utcnow():
        raise ValueError("Future timestamps detected!")

    # 5. Volume checks
    # Ensure there is enough data
    if len(df) < 10:
        raise ValueError("Too few earthquake records!")

    # 6. Simple outlier check
    if df["mag"].mean() > 8:
        raise ValueError("Unusually high average magnitude!")

    # 7. Freshness check
    # Check if the data for the day is present
    # If the dataset has no rows, it may indicate upstream failure or missing data
    if len(df) == 0:
        raise ValueError(f"No data found for {start_date}! Freshness check failed.")

    # Optional: check if the latest timestamp is within expected window
    # For example, no data older than 2 days for recent day
    max_time = df["time"].max()
    if (pd.Timestamp.utcnow() - max_time).days > 20:
        raise ValueError(f"Data is not fresh! Latest record is older than 20 days: {max_time}")

    logging.info("✅ Data validation passed")
    con.close()

# DAG definition
with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 5 * * *",
    default_args=args,
    tags=["s3", "ods", "pg", "validation"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    sensor_on_raw_layer = ExternalTaskSensor(
        task_id="sensor_on_raw_layer",
        external_dag_id="raw_from_api_to_s3",
        allowed_states=["success"],
        mode="reschedule",
        timeout=360000,
        poke_interval=60,
    )

    load_task = PythonOperator(
        task_id="get_and_transfer_raw_data_to_ods_pg",
        python_callable=get_and_transfer_raw_data_to_ods_pg,
    )

    validate_task = PythonOperator(
        task_id="validate_earthquake_data",
        python_callable=validate_earthquake_data,
    )

    end = EmptyOperator(task_id="end")

    # DAG dependencies
    start >> sensor_on_raw_layer >> load_task >> validate_task >> end