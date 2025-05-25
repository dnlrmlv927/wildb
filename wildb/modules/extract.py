import requests
import time
from clickhouse_driver import Client
from typing import List, Optional, Dict, Any


class WBETLProcessor:
    def __init__(self, clickhouse_client: Client):
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

        for nmId in nmIds:
            product = self.fetch_product_stocks(nmId)
            if product:
                data_to_insert.append((date_str, product['nmId'], product['stocks']))
            time.sleep(delay)

        if data_to_insert:
            self.ch_client.execute(
                "INSERT INTO wb_stocks_buffer (date, nmId, stocks) VALUES",
                data_to_insert
            )

        return len(data_to_insert)