import requests
import time
from datetime import datetim
from clickhouse_connect import get_client
from typing import List, Optional, Dict, Any


class WBETLProcessor:
    def __init__(self, clickhouse_client):
        self.ch_client = clickhouse_client

    def fetch_product_stocks(self, nmId: int) -> Optional[Dict[str, Any]]:
        """Получение данных об остатках товара с Wildberries"""
        url = f'https://card.wb.ru/cards/v2/detail?appType=1&curr=rub&dest=-1257786&spp=99&nm={nmId}'

        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            product = response.json()['data']['products'][0]

            return {
                'nmId': nmId,
                'stocks': sum(
                    stock['qty']
                    for size in product['sizes']
                    for stock in size['stocks']
                )
            }
        except Exception as e:
            print(f"Error fetching product {nmId}: {str(e)}")
            return None

    def process_products(
            self,
            nmIds: List[int],
            date_str: str,
            delay: float = 0.3
    ) -> int:
        """Основной ETL-процесс"""
        data_to_insert = []

        # Преобразуем строку в datetime.date
        date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()

        for nmId in nmIds:
            product = self.fetch_product_stocks(nmId)
            if product:
                data_to_insert.append((date_obj, product['nmId'], product['stocks']))
            time.sleep(delay)

        if data_to_insert:
            self.ch_client.insert(
                table='wb_stocks_buffer',
                data=data_to_insert,
                column_names=['date', 'nmId', 'stocks']
            )

        return len(data_to_insert)
