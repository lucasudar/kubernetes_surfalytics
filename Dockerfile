FROM apache/airflow

USER root

COPY --chown=airflow:root ./dags/ ${AIRFLOW_HOME}/dags/

USER airflow
