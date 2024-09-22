from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG('api_extractor',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    discord_task = KubernetesPodOperator(
        task_id='run_discord_script',
        name='discord-script',
        namespace='airflow',
        image='nikitastarkov/stripe_discord:latest',
        cmds=["python", "./scripts/discord-script.py"],
        env_vars={
            'DISCORD_BOT_TOKEN': Variable.get('DISCORD_BOT_TOKEN'),
            'GUILD_ID': Variable.get('GUILD_ID'),
        },
        is_delete_operator_pod=True,
        get_logs=True,
    )

    stripe_task = KubernetesPodOperator(
        task_id='run_stripe_script',
        name='stripe-script',
        namespace='airflow',
        image='nikitastarkov/stripe_discord:latest',
        cmds=["python", "./scripts/stripe-script.py"],
        env_vars={
            'STRIPE_SECRET_KEY': Variable.get('STRIPE_SECRET_KEY'),
        },
        is_delete_operator_pod=True,
        get_logs=True,
    )

    discord_task >> stripe_task
