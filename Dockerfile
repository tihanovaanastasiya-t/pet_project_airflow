FROM apache/airflow:2.10.5-python3.12

USER root

# Системные зависимости
RUN apt-get update && apt-get install -y \
    python3-distutils \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Переключаемся на airflow перед установкой Python пакетов
USER airflow

# Устанавливаем Great Expectations под airflow
RUN pip install great_expectations==0.18.22