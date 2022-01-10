"""
DAG to reset the initial data environment.  First any temporary delta files will be removed.  Then
the main Amazon purchases will be split into n delta files ready to be loaded incrementally into
Snowflake.

Each delta file will be stored as a separate CSV file with a random date assigned.  These files
will be used to test delta loading into the Snowflake data warehouse.
"""

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.s3_delete_objects import S3DeleteObjectsOperator

from datetime import date, datetime, timedelta


@task()
def split_file(n: int, input_file: str, output_dir: str):
    """Task to split a CSV file into n pieces"""

    import random

    # Generate list of n random dates in the past 30 days
    start_date = date.today() - timedelta(days=30)
    random_number_list = random.sample(range(30), n)
    random_dates = [start_date + timedelta(days=days) for days in random_number_list]
    random_dates.sort()

    # Collect all lines in the input CSV file, without loading whole file into memory
    lines = []
    header_line = ""
    with open(input_file, 'r') as csv_file:
        header_line = csv_file.readline()
        for line in csv_file:
            lines.append(line)

    # Determine the size of each output file
    input_file_rows = len(lines)
    size_of_each_output_file = int(input_file_rows / n)  # truncate decimal result (aka round down)
    remainder = input_file_rows % n

    # Write each group of lines to separate files
    for i, dt in enumerate(random_dates):

        start_position = i * size_of_each_output_file + remainder  # shift the start position since the first file will be a little bigger
        if i == 0:
            # Make the first file a little bigger due to uneven division
            start_position = 0
        
        end_position = start_position + size_of_each_output_file

        filename = f"{output_dir}/stage/{str(dt)}_orders.csv"
        with open(filename, 'w') as output_csv_file:
            output_csv_file.write(header_line)
            output_csv_file.writelines(lines[start_position:end_position])


@dag(
    start_date=datetime(2021, 1, 1),
    tags=['snowflake'],
    schedule_interval=None,
    catchup=False,
)
def init_data(n: int = 5, input_file: str = '/data/Retail.OrderHistory.1.csv', output_dir: str = '/data'):

    """
    DAG to reset the environment and prepare the delta files

    :param n: Number of delta files to create from the Amazon purchase history
    :param
    """

    remove_delta_files = BashOperator(
        task_id="remove_delta_files",
        bash_command="rm -rf {{ params.output_dir }}/stage",
    )

    make_stage_dir = BashOperator(
        task_id="make_stage_dir",
        bash_command="mkdir -p {{ params.output_dir }}/stage",
    )
    
    empty_s3_bucket = S3DeleteObjectsOperator(
        task_id="empty_s3_bucket",
        bucket="data-pipeline-practice-snowflake",
        prefix="stage",
        aws_conn_id="aws_s3",
    )

    [remove_delta_files, empty_s3_bucket] >> make_stage_dir >> split_file(n, input_file, output_dir)


dag = init_data()
