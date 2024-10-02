from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

def print_date(**kwargs):
    from datetime import datetime
    print(f"Current date and time: {datetime.now()}")

def create_file(**kwargs):
    with open('/tmp/sample_file.txt', 'w') as f:
        f.write('This is a sample file created by Airflow.')

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='sample_dag',
    default_args=default_args,
    description='A simple sample DAG',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    print_task = PythonOperator(
        task_id='print_date',
        python_callable=print_date,
        provide_context=True,
    )

    file_task = PythonOperator(
        task_id='create_file',
        python_callable=create_file,
        provide_context=True,
    )

    # Set task dependencies
    print_task >> file_task

