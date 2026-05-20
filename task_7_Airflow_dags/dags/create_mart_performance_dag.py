# dags/create_mart_performance_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Добавляем путь к папке scripts
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'scripts'))

# Импортируем функцию из нового скрипта
from build_mart_performance import create_mart_performance

default_args = {
    'owner': 'schepetova',  # укажите своё имя
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'create_analytics_mart_performance',  # новое имя DAG
    default_args=default_args,
    description='Создание и обновление витрины dmr.analytics_student_performance',
    schedule_interval='0 3 * * *',   # каждый день в 3:00 (можно изменить)
    catchup=False,
    tags=['mart', 'student_performance', 'advanced'],
) as dag:

    create_mart_task = PythonOperator(
        task_id='create_performance_mart',
        python_callable=create_mart_performance,
    )

    create_mart_task