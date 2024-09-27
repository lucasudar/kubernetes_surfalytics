from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG('get_last_2_days',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    discord_task = KubernetesPodOperator(
        task_id='get_last_2_days_discord',
        name='get_last_2_days_discord',
        namespace='airflow',
        image='nikitastarkov/discord_stripe_dbt:0.3',
        cmds=["python", "./scripts/get_last_2_days_discord.py"],
        env_vars={
            'DISCORD_BOT_TOKEN': Variable.get('DISCORD_BOT_TOKEN'),
            'GUILD_ID': Variable.get('GUILD_ID'),
            'DB_URI': Variable.get('DB_URI'),
            'DB_SCHEMA': Variable.get('DB_SCHEMA')
        },
        is_delete_operator_pod=True,
        get_logs=True,
    )

    stripe_task = KubernetesPodOperator(
        task_id='get_last_2_days_stripe',
        name='get_last_2_days_stripe',
        namespace='airflow',
        image='nikitastarkov/discord_stripe_dbt:0.3',
        cmds=["python", "./scripts/get_last_2_days_stripe.py"],
        env_vars={
            'STRIPE_SECRET_KEY': Variable.get('STRIPE_SECRET_KEY'),
            'DB_URI': Variable.get('DB_URI'),
            'DB_SCHEMA': Variable.get('DB_SCHEMA')
        },
        is_delete_operator_pod=True,
        get_logs=True,
    )

    discord_task >> stripe_task