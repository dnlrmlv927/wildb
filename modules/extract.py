def fetch_product_stocks(self, nmId: int) -> Optional[Dict[str, Any]]:
    """Получение данных об остатках товара с Wildberries с обработкой отсутствующих значений"""
    url = f'https://card.wb.ru/cards/v2/detail?appType=1&curr=rub&dest=-1257786&spp=99&nm={nmId}'

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        product = response.json()['data']['products'][0]

        total_stocks = product.get('totalQuantity', 0)
        warehouse_stocks = []

        sizes = product.get('sizes', [])
        for size in sizes:
            stocks = size.get('stocks', [])
            for stock in stocks:
                qty = stock.get('qty', 0)
                wh = stock.get('wh', 0)  # на случай, если поля нет
                warehouse_stocks.append({'wh': wh, 'qty': qty})

        # Если нет данных по складам вообще — добавим одну запись с нулями
        if not warehouse_stocks:
            warehouse_stocks.append({'wh': 0, 'qty': 0})

        return {
            'nmId': nmId,
            'stocks': total_stocks,
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
