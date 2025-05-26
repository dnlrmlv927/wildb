import sys
sys.path.append('/home/danilssau6364/airflow')

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from modules.connection import get_clickhouse_client


def calculate_sales(**context) -> None:
    client = get_clickhouse_client()

    query = """
    INSERT INTO warehouse_balances.sales_by_day
    SELECT
        date,
        nmId,
        SUM(lagStocks - stocks) AS orders
    FROM (
        SELECT
            date,
            nmId,
            warehouse_id,
            stocks,
            any(stocks) OVER (
                PARTITION BY nmId, warehouse_id
                ORDER BY date
                ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
            ) AS lagStocks
        FROM warehouse_balances.wb_stocks_main
    )
    WHERE lagStocks IS NOT NULL AND lagStocks >= stocks
    GROUP BY date, nmId
    HAVING SUM(lagStocks - stocks) > 0
    """

    client.execute(query)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'wb_calculate_sales',
    default_args=default_args,
    description='Расчет продаж WB после загрузки остатков',
    schedule_interval='0 1 * * *',  # 01:00 ночи каждый день
    catchup=False,
    max_active_runs=1,
    tags=['wb', 'sales', 'dependent'],
)

wait_for_stocks_load = ExternalTaskSensor(
    task_id='wait_for_stocks_load',
    external_dag_id='wb_stocks_monitoring',
    external_task_id='load_wb_stocks_data',
    execution_delta=timedelta(hours=4),
    timeout=3600,  # ждать максимум 1 час
    mode='poke',
    dag=dag,
)

calculate_sales_task = PythonOperator(
    task_id='calculate_daily_sales',
    python_callable=calculate_sales,
    provide_context=True,
    dag=dag,
)

wait_for_stocks_load >> calculate_sales_task  # зависимость
