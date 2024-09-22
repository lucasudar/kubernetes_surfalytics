from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from airflow.utils.dates import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dbt_dag',
    default_args=default_args,
    description='A simple dbt DAG using KubernetesPodOperator',
    schedule_interval='@daily',  # Adjust the schedule as needed
    start_date=days_ago(1),
    catchup=False,
) as dag:

    dbt_run = KubernetesPodOperator(
        task_id='dbt_run',
        name='dbt-run',
        namespace='airflow',
        image='nikitastarkov/dbt-surfalytics:latest',
        cmds=["dbt"],
        arguments=["run", "--profiles-dir", "./profiles", "--project-dir", "/usr/app/dbt/"],
        is_delete_operator_pod=True,
    )

    dbt_test = KubernetesPodOperator(
        task_id='dbt_test',
        name='dbt-test',
        namespace='airflow',
        image='nikitastarkov/dbt-surfalytics:latest',
        cmds=["dbt"],
        arguments=["test", "--profiles-dir", "./profiles", "--project-dir", "/usr/app/dbt/"],
        is_delete_operator_pod=True,
    )

    dbt_run >> dbt_test
