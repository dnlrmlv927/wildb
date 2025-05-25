import sys
sys.path.append('/home/danilssau6364/airflow')

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from modules.connection import get_clickhouse_client
from modules.extract import WBETLProcessor

def load_wb_stocks(**context) -> None:
    """Задача Airflow для обработки данных"""
    execution_date = context['execution_date']
    date_str = execution_date.strftime('%Y-%m-%d')

    processor = WBETLProcessor(get_clickhouse_client())
    inserted_rows = processor.process_products(
        nmIds=[293363765, 293378862, 295739458, 263038976, 287293767,
               267672429, 265116995, 268759087, 284439349, 274048350],
        date_str=date_str
    )

    context['ti'].xcom_push(key='inserted_rows', value=inserted_rows)


# Конфигурация DAG asda
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
}

dag = DAG(
    'wb_stocks_monitoring',
    default_args=default_args,
    description='Ежедневный мониторинг остатков WB',
    schedule_interval='0 23 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['production', 'wb', 'analytics'],
)

load_task = PythonOperator(
    task_id='load_wb_stocks_data',
    python_callable=load_wb_stocks,
    provide_context=True,
    dag=dag,
)

# Дополнительные задачи могут быть добавлены здесь
# example_task >> load_task