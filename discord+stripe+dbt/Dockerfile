FROM apache/airflow

USER root

RUN apt-get update && \
    apt-get install -y \
    python3-dev \
    libpq-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Переключаемся на пользователя airflow для установки Python пакетов
USER airflow

RUN pip install --no-cache-dir \
    discord.py \
    pandas \
    aiohttp \
    sqlalchemy \
    stripe

# Возвращаемся к пользователю root для копирования файлов
USER root

COPY --chown=airflow:root ./dags/ ${AIRFLOW_HOME}/dags/
COPY --chown=airflow:root ./scripts/ ${AIRFLOW_HOME}/scripts/

# Возвращаемся к пользователю airflow для запуска Airflow
USER airflow
