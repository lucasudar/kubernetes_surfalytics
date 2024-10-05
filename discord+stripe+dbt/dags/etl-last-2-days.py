from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.models import Variable
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'retries': 0,
}

with DAG(
        dag_id='etl_last_2_days',
        default_args=default_args,
        start_date=datetime(2024, 10, 1),
        schedule_interval=None,
        catchup=False) as dag:

    etl_task = KubernetesPodOperator(
        task_id='etl_task',
        name='etl_task',
        namespace='airflow',
        image='nikitastarkov/discord_stripe_dbt:0.31',
        cmds=["python", "./scripts/etl_last_2_days.py"],
        env_vars={
            'DB_URI': Variable.get('DB_URI'),
        },
        is_delete_operator_pod=True,
        get_logs=True,
    )

    trigger_dbt_task = TriggerDagRunOperator(
        task_id='trigger_dbt_task',
        trigger_dag_id='dbt',
    )

    etl_task >> trigger_dbt_task
