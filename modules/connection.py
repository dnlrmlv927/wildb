from clickhouse_connect import get_client
from airflow.models import Variable

def get_clickhouse_client():
    """Возвращает подключение к ClickHouse через HTTP, используя переменные Airflow"""
    return get_client(
        host=Variable.get("CLICKHOUSE_HOST"),
        username=Variable.get("CLICKHOUSE_USER", default_var='default'),
        password=Variable.get("CLICKHOUSE_PASSWORD", default_var=''),
        database=Variable.get("CLICKHOUSE_DB", default_var='default'),
        port=int(Variable.get("CLICKHOUSE_PORT", default_var='8443')),
        secure=True  # Включает HTTPS
    )
