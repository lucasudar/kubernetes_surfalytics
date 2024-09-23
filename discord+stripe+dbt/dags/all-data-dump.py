from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess
from airflow.models import Variable

# Define the path to the scripts
DISCORD_SCRIPT = './scripts/all-discord-dump.py'
STRIPE_SCRIPT = './scripts/all-stripe-dump.py'

# Function to execute discord-script.py
def run_discord_script():
    result = subprocess.run(['python3', DISCORD_SCRIPT], check=True, capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Discord script failed: {result.stderr}")

# Function to execute stripe-script.py
def run_stripe_script():
    result = subprocess.run(['python3', STRIPE_SCRIPT], check=True, capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Stripe script failed: {result.stderr}")

# Define default arguments
default_args = {
    'start_date': datetime(2024, 9, 23),  # Update with your desired start date
    'catchup': False
}

# Define the DAG
with DAG(
    'initial_data_load_dag',
    default_args=default_args,
    schedule_interval=None,  # This ensures it runs only once unless triggered manually
    description='DAG for initial data load from Discord and Stripe',
    tags=['initial-load']
) as dag:

    # Task to load Discord data
    discord_data_dump = PythonOperator(
        task_id='discord_data_dump',
        python_callable=run_discord_script,
        env_vars={
            'DISCORD_BOT_TOKEN': Variable.get('DISCORD_BOT_TOKEN'),
            'GUILD_ID': Variable.get('GUILD_ID'),
            'DB_URI': Variable.get('DB_URI'),
            'DB_SCHEMA': Variable.get('DB_SCHEMA')
        },
        get_logs=True,
    )

    # Task to load Stripe data
    stripe_data_dump = PythonOperator(
        task_id='stripe_data_dump',
        python_callable=run_stripe_script,
        env_vars={
            'STRIPE_SECRET_KEY': Variable.get('STRIPE_SECRET_KEY'),
            'DB_URI': Variable.get('DB_URI'),
            'DB_SCHEMA': Variable.get('DB_SCHEMA')
        },
        get_logs=True,
    )

    # Define the execution order (both can run independently)
    discord_data_dump >> stripe_data_dump