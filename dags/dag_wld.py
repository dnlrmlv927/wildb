import sys
sys.path.append('/home/danilssau6364/airflow')

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from modules.connection import get_clickhouse_client
from modules.extract import WBETLProcessor

def load_wb_stocks(**context):
    data_date = context['data_interval_end'].date()
    date_str = data_date.strftime('%Y-%m-%d')

    processor = WBETLProcessor(get_clickhouse_client())
    inserted_rows = processor.process_products(
        nmIds=[293363765, 293378862, 295739458, 263038976, 287293767,
               267672429, 265116995, 268759087, 284439349, 274048350],
        date_str=date_str
    )
    context['ti'].xcom_push(key='inserted_rows', value=inserted_rows)


def is_buffer_empty(**context):
    client = get_clickhouse_client()
    query = """
        SELECT total_bytes 
        FROM system.tables 
        WHERE database = 'warehouse_balances' AND name = 'wb_stocks_buffer'
    """
    result = client.query(query)

    # Проверяем наличие результатов и что total_bytes равен 0
    if result.result_set and len(result.result_set) > 0:
        total_bytes = result.result_set[0][0]  # Получаем значение total_bytes
        context['ti'].xcom_push(key='buffer_size_bytes', value=total_bytes)
        return total_bytes == 0


def calculate_sales(**context):
    client = get_clickhouse_client()
    execution_date = context['logical_date'].date()
    date_str = execution_date.strftime('%Y-%m-%d')

    query = f"""
    INSERT INTO warehouse_balances.sales_by_day
    WITH 
    date_range AS (
        SELECT 
            toDate('{date_str}') AS current_date,
            toDate('{date_str}') - 1 AS previous_date
    ),
    all_products AS (
        SELECT DISTINCT nmId
        FROM warehouse_balances.wb_stocks_main
        WHERE date = (SELECT previous_date FROM date_range)
    ),
    yesterday_warehouses AS (
        SELECT 
            nmId, 
            warehouse_id,
            sum(stocks) AS total_stocks
        FROM warehouse_balances.wb_stocks_main
        WHERE date = (SELECT previous_date FROM date_range)
        GROUP BY nmId, warehouse_id
    ),
    today_data AS (
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
        WHERE date = (SELECT current_date FROM date_range) OR date = (SELECT previous_date FROM date_range)
    ),
    regular_sales AS (
        SELECT
            date,
            nmId,
            toInt64(SUM(lagStocks - stocks)) AS orders
        FROM today_data
        WHERE lagStocks IS NOT NULL AND lagStocks >= stocks
        GROUP BY date, nmId
        HAVING SUM(lagStocks - stocks) > 0
    ),
    missing_warehouse_sales AS (
        SELECT
            (SELECT current_date FROM date_range) AS date,
            y.nmId,
            toInt64(SUM(y.total_stocks)) AS orders
        FROM yesterday_warehouses y
        LEFT ANTI JOIN today_data t ON y.nmId = t.nmId AND y.warehouse_id = t.warehouse_id
        GROUP BY y.nmId
        HAVING SUM(y.total_stocks) > 0
    ),
    all_sales AS (
        SELECT date, nmId, orders FROM regular_sales
        UNION ALL
        SELECT date, nmId, orders FROM missing_warehouse_sales
    ),
    new_data_to_insert AS (
        SELECT 
            (SELECT current_date FROM date_range) AS date,
            p.nmId,
            COALESCE(s.orders, 0) AS orders
        FROM all_products p
        LEFT JOIN all_sales s ON p.nmId = s.nmId
    )
    SELECT nd.date, nd.nmId, nd.orders
    FROM new_data_to_insert nd
    LEFT ANTI JOIN warehouse_balances.sales_by_day sb 
        ON nd.date = sb.date AND nd.nmId = sb.nmId
    WHERE nd.date IS NOT NULL;
    """

    client.query(query)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 25),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
}

dag = DAG(
    'wb_data_pipeline',
    default_args=default_args,
    description='DAG для загрузки остатков и расчёта продаж WB',
    schedule_interval='0 20 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['production', 'wb', 'analytics'],
)

load_task = PythonOperator(
    task_id='load_wb_stocks_data',
    python_callable=load_wb_stocks,
    dag=dag,
)

buffer_sensor = PythonSensor(
    task_id='check_buffer_empty',
    python_callable=is_buffer_empty,
    poke_interval=30,
    timeout=3600,
    mode='poke',
    dag=dag,
)

calculate_sales_task = PythonOperator(
    task_id='calculate_daily_sales',
    python_callable=calculate_sales,
    dag=dag,
)

load_task >> buffer_sensor >> calculate_sales_task
