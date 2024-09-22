# Extended version stripe+discord

добавить скрипрты (stripe and discord) и новый даг в контейнер **nikitastarkov/airflow-dags**
пересобрать докер контейнер с новыми данными и конечно же изменить докер файл

```docker
FROM apache/airflow

USER root

COPY --chown=airflow:root ./dags/ ${AIRFLOW_HOME}/dags/

COPY --chown=airflow:root ./scripts/ ${AIRFLOW_HOME}/dags/scripts/

USER airflow
```