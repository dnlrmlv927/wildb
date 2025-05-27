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
    -- Вставляем только новые данные, которых еще нет в целевой таблице
INSERT INTO warehouse_balances.sales_by_day
WITH 
-- Определяем последнюю дату в данных
date_range AS (
    SELECT 
        max(date) AS current_date,
        current_date - 1 AS previous_date
    FROM warehouse_balances.wb_stocks_main
),

-- Все товары, которые были в предыдущий день (для полного покрытия)
all_products AS (
    SELECT DISTINCT nmId
    FROM warehouse_balances.wb_stocks_main
    WHERE date = (SELECT previous_date FROM date_range)
),

-- Получаем все уникальные пары nmId и warehouse_id за предыдущий день
yesterday_warehouses AS (
    SELECT 
        nmId, 
        warehouse_id,
        sum(stocks) AS total_stocks
    FROM warehouse_balances.wb_stocks_main
    WHERE date = (SELECT previous_date FROM date_range)
    GROUP BY nmId, warehouse_id
),

-- Получаем данные за текущий день (последнюю дату)
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
    WHERE date = (SELECT current_date FROM date_range)
),

-- Рассчитываем продажи по складам, которые есть в обоих отчетах
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

-- Рассчитываем "продажи" по складам, которые были вчера, но отсутствуют сегодня
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

-- Объединяем все продажи
all_sales AS (
    SELECT date, nmId, orders FROM regular_sales
    UNION ALL
    SELECT date, nmId, orders FROM missing_warehouse_sales
),

-- Получаем итоговые данные для вставки
new_data_to_insert AS (
    SELECT 
        (SELECT current_date FROM date_range) AS date,
        p.nmId,
        COALESCE(s.orders, 0) AS orders
    FROM all_products p
    LEFT JOIN all_sales s ON p.nmId = s.nmId
)

-- Вставляем только те записи, которых еще нет таблице
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
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'wb_calculate_sales',
    default_args=default_args,
    description='Расчет продаж WB после загрузки остатков',
    schedule_interval='0 1 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['wb', 'sales', 'dependent'],
)

wait_for_stocks_load = ExternalTaskSensor(
    task_id='wait_for_stocks_load',
    external_dag_id='wb_stocks_monitoring',
    external_task_id='load_wb_stocks_data',
    execution_delta=timedelta(hours=4),
    timeout=3600,
    mode='poke',
    dag=dag,
)

calculate_sales_task = PythonOperator(
    task_id='calculate_daily_sales',
    python_callable=calculate_sales,
    provide_context=True,
    dag=dag,
)

wait_for_stocks_load >> calculate_sales_task
