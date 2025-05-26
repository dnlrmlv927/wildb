import requests
import time
from datetime import datetime
from clickhouse_connect import get_client
from typing import List, Optional, Dict, Any, Tuple


class WBETLProcessor:
    def __init__(self, clickhouse_client):
        self.ch_client = clickhouse_client

    def fetch_product_stocks(self, nmId: int) -> Optional[Dict[str, Any]]:
        """Получение данных об остатках товара с Wildberries с обработкой отсутствующих значений"""
        url = f'https://card.wb.ru/cards/v2/detail?appType=1&curr=rub&dest=-1257786&spp=99&nm={nmId}'

        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            product = response.json()['data']['products'][0]

            warehouse_stocks = []
            total_stocks = product.get('totalQuantity', 0)  # Берем общее количество, если есть

            for size in product['sizes']:
                for stock in size['stocks']:
                    qty = stock.get('qty')
                    if qty is None:
                        qty = 0

                    warehouse_stocks.append({
                        'wh': stock['wh'],  # ID склада
                        'qty': qty  # Количество на складе (0 если нет данных)
                    })

            return {
                'nmId': nmId,
                'stocks': total_stocks,  # Общее количество товара
                'warehouse_stocks': warehouse_stocks
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
        """Основной ETL-процесс с обработкой отсутствующих данных"""
        buffer_data = []

        date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()

        for nmId in nmIds:
            product = self.fetch_product_stocks(nmId)
            if product:
                # Если нет данных по складам, но есть totalQuantity
                if not product['warehouse_stocks'] and product['stocks'] > 0:
                    # Добавляем запись с warehouse_id = 0 (или другим значением для "неизвестного склада")
                    buffer_data.append((
                        date_obj,
                        product['nmId'],
                        0,  # Специальное значение для "неизвестного склада"
                        product['stocks']
                    ))
                else:
                    for stock in product['warehouse_stocks']:
                        buffer_data.append((
                            date_obj,
                            product['nmId'],
                            stock['wh'],
                            stock['qty']
                        ))

            time.sleep(delay)

        if buffer_data:
            self.ch_client.insert(
                table='wb_stocks_buffer',
                data=buffer_data,
                column_names=['date', 'nmId', 'warehouse_id', 'stocks']
            )

        return len(buffer_data)