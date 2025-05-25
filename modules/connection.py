from clickhouse_driver import Client
from airflow.models import Variable

def get_clickhouse_client():
    """Возвращает подключение к ClickHouse используя переменные Airflow"""
    return Client(
        host=Variable.get("CLICKHOUSE_HOST"),
        user=Variable.get("CLICKHOUSE_USER", default_var='default'),
        password=Variable.get("CLICKHOUSE_PASSWORD", default_var=''),
        database=Variable.get("CLICKHOUSE_DB", default_var='default'),
        port=int(Variable.get("CLICKHOUSE_PORT", default_var='8443'))
    )

