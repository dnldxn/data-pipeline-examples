"""
DAG to push the delta files to S3
"""

from airflow.decorators import dag
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator

from datetime import date, datetime

@dag(
    start_date=datetime(2021, 1, 1),
    tags=['snowflake'],
    schedule_interval=None,
    catchup=False,
)
def pipeline(load_dt: date = date.today()):

    LocalFilesystemToS3Operator(
        task_id="upload_to_s3",
        filename="/data/stage/{{ params.load_dt }}_orders.csv",
        dest_key="stage/{{ params.load_dt }}/orders.csv",
        dest_bucket="data-pipeline-practice-snowflake",
        aws_conn_id="aws_s3",
    )


pipeline_dag = pipeline()
