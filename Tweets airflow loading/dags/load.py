from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.transfers.local_to_snowflake import LocalFileToSnowflakeOperator
from datetime import datetime

def read_csv_and_upload_to_snowflake():
    # Read local CSV file
    # Assuming you have a CSV file named "data.csv" in the same directory as this script
    with open("Tweets sentiment analysis/tweets.csv", "r") as file:
        data = file.read()

    # Upload data to Snowflake
    conn_id = 'snowflake_connexion'  # The ID of your Snowflake connection in Astro
    table_name = 'tweets'  # The target table in Snowflake

    snowflake_operator = LocalFileToSnowflakeOperator(
        task_id='upload_to_snowflake',
        src_conn_id=None,  # No source connection needed for local file
        dst_conn_id=conn_id,
        table=table_name,
        schema='public',  # Adjust as per your Snowflake setup
        file_path="data.csv",  # Assuming the file is in the same directory
        file_format='csv',  # Adjust as per your file format
        stage='your_stage_name',  # The stage in your Snowflake account
        copy_options=['REMOVEQUOTES = TRUE'],  # Adjust as per your needs
        autocommit=True,  # Adjust as per your needs
        params={"data": data}
    )

    return snowflake_operator.execute(context=None)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 30),  # Adjust the start date
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'csv_to_snowflake',
    default_args=default_args,
    description='Move data from local CSV to Snowflake',
    schedule_interval=None,  # Set the schedule interval as per your requirements
    catchup=False
)

upload_task = PythonOperator(
    task_id='upload_csv_to_snowflake',
    python_callable=read_csv_and_upload_to_snowflake,
    provide_context=True,
    dag=dag,
)
