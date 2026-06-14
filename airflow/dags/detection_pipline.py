# airflow/dags/detection_pipeline.py
"""
Airflow DAG: runs hourly to aggregate detection data into the
detection_summary table (data warehouse pattern).
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import psycopg2
import os

DEFAULT_ARGS = {
    "owner": "vision-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

PG_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "host.docker.internal"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname":   os.getenv("POSTGRES_DB", "detections_db"),
    "user":     os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
}


def aggregate_detections(**context):
    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()

    # Clear the last hour's summary before recomputing (idempotent ETL)
    cur.execute("""
        DELETE FROM detection_summary
        WHERE hour_bucket >= NOW() - INTERVAL '1 hour'
    """)

    # Re-aggregate: count detections per class per hour
    cur.execute("""
        INSERT INTO detection_summary (hour_bucket, class_name, total_count, avg_confidence)
        SELECT
            DATE_TRUNC('hour', de.detected_at)    AS hour_bucket,
            dc.class_name,
            COUNT(*)                               AS total_count,
            ROUND(AVG(de.confidence)::numeric, 4)  AS avg_confidence
        FROM detection_events de
        JOIN dim_class dc ON de.class_id = dc.class_id
        WHERE de.detected_at >= NOW() - INTERVAL '1 hour'
        GROUP BY DATE_TRUNC('hour', de.detected_at), dc.class_name
        ORDER BY hour_bucket, total_count DESC
    """)

    conn.commit()

    cur.execute("SELECT COUNT(*) FROM detection_summary")
    total_rows = cur.fetchone()[0]
    cur.close()
    conn.close()

    print(f"[Airflow] Aggregation complete. detection_summary has {total_rows} rows.")
    return total_rows


def validate_data_quality(**context):
    """Checks the raw data for known quality issues before aggregating."""
    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM detection_events WHERE confidence IS NULL")
    null_conf = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM detection_events WHERE detected_at > NOW()")
    future_ts = cur.fetchone()[0]

    cur.close()
    conn.close()

    if null_conf > 0:
        raise ValueError(f"Data quality FAIL: {null_conf} rows with null confidence")
    if future_ts > 0:
        raise ValueError(f"Data quality FAIL: {future_ts} rows with future timestamps")

    print("[Airflow] Data quality checks passed.")


with DAG(
    dag_id="detection_pipeline",
    default_args=DEFAULT_ARGS,
    description="Hourly aggregation of vision detection events",
    schedule="0 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["vision", "mlops", "data-warehouse"],
) as dag:

    validate = PythonOperator(
        task_id="validate_data_quality",
        python_callable=validate_data_quality,
    )

    aggregate = PythonOperator(
        task_id="aggregate_detections",
        python_callable=aggregate_detections,
    )

    log_summary = BashOperator(
        task_id="log_summary",
        bash_command='echo "Pipeline complete at $(date)"',
    )

    validate >> aggregate >> log_summary