"""
DAG to incrementally push a delta file to Snowflake.  First the delta file will be pushed to S3,
partitioned by the specified load_dt Airflow parameter.  Then it will be staged in Snowflake,
before being transformed/normalized into the data model.
"""

from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from datetime import date, datetime


@dag(
    start_date=datetime(2021, 1, 1),
    tags=['snowflake'],
    schedule_interval=None,
    catchup=False,
    default_args={
        'aws_conn_id': 'aws_s3',
        'snowflake_conn_id': 'snowflake',
        'autocommit': False
    },
)
def pipeline(load_dt: date = date.today()):

    start = DummyOperator(
        task_id="start"
    )

    upload_to_s3 = LocalFilesystemToS3Operator(
        task_id="upload_to_s3",
        filename="/data/stage/{{ params.load_dt }}_orders.csv",
        dest_key="stage/{{ params.load_dt }}/orders.csv",
        dest_bucket="data-pipeline-practice-snowflake",
        replace=True,
    )

    begin_transaction = SnowflakeOperator(
        task_id='begin_transaction',
        sql='BEGIN TRANSACTION;',
    )

    load_incremental_data = SnowflakeOperator(
        task_id='load_incremental_data',
        sql='./sql/01-import-raw-csv.sql',
    )

    commit = SnowflakeOperator(
        task_id='commit',
        sql='COMMIT;',
    )

    start >> upload_to_s3 >> begin_transaction >> load_incremental_data >> commit


dag = pipeline()
