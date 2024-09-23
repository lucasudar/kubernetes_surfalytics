import sys
sys.path.append('../scripts')

from all_discord_dump import run_discord_dump
from all_stripe_dump import run_stripe_dump

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 22),
    'retries': 0
}

# Define the DAG
with DAG('all_data_dump', default_args=default_args, schedule_interval=None, catchup=False) as dag:

    discord_data_dump = PythonOperator(
        task_id='discord_data_dump',
        python_callable=run_discord_dump
    )

    stripe_data_dump = PythonOperator(
        task_id='stripe_data_dump',
        python_callable=run_stripe_dump
    )

    # Set task dependencies
    discord_data_dump >> stripe_data_dump