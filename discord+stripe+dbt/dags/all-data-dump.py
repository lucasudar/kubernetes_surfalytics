from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os
from airflow.models import Variable

# Define the path to the scripts
DISCORD_SCRIPT = './scripts/all-discord-dump.py'
STRIPE_SCRIPT = './scripts/all-stripe-dump.py'

# Define your function to call the Python scripts
def run_discord_script():
    # Set environment variables (removed commas to avoid tuple creation)
    os.environ['DISCORD_BOT_TOKEN'] = Variable.get('DISCORD_BOT_TOKEN')
    os.environ['GUILD_ID'] = Variable.get('GUILD_ID')
    os.environ['DB_URI'] = Variable.get('DB_URI')
    os.environ['DB_SCHEMA'] = Variable.get('DB_SCHEMA')
    
    # Import and run your script here
    import subprocess
    result = subprocess.run(
        ['python3', DISCORD_SCRIPT], 
        check=True, 
        capture_output=True, 
        text=True
    )
    print(result.stdout)  # Log standard output
    print(result.stderr)  # Log errors

def run_stripe_script():
    # Set environment variables (removed commas to avoid tuple creation)
    os.environ['STRIPE_SECRET_KEY'] = Variable.get('STRIPE_SECRET_KEY')
    os.environ['DB_URI'] = Variable.get('DB_URI')
    os.environ['DB_SCHEMA'] = Variable.get('DB_SCHEMA')
    
    # Import and run your script here
    import subprocess
    subprocess.run(['python3', STRIPE_SCRIPT], check=True)

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
        python_callable=run_discord_script
    )

    stripe_data_dump = PythonOperator(
        task_id='stripe_data_dump',
        python_callable=run_stripe_script
    )

    # Set task dependencies
    discord_data_dump >> stripe_data_dump