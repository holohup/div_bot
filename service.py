import asyncio
import datetime
from decimal import Decimal
from typing import Literal

import pandas as pd
from tinkoff.invest.schemas import RealExchange

from exceptions import ValidationError
from settings import (DEFAULT_DISCOUNT_RATE, FUTURES_KEEP_COLUMNS,
                      STOCKS_KEEP_COLUMNS)
from settings import STORAGE
from t_api import (fetch_futures, fetch_stocks, get_last_prices,
                   get_orderbook_price, is_trading_now)

FORCE_LAST_PRICE = False
DISCOUNT_RATE = DEFAULT_DISCOUNT_RATE

DATA_FETCHERS = {
    'futures': fetch_futures,
    'stocks': fetch_stocks,
}


class THandler:
    def __init__(self, storage) -> None:
        self._storage = storage

    async def get_data(self, dt: Literal['futures', 'stocks']) -> pd.DataFrame:
        data_storage = self._storage(dt)
        await self.update_data(dt, data_storage)
        return data_storage.retrieve_df()

    async def update_data(self, dt: Literal['futures', 'stocks'], data):
        if not data.is_updated():
            df = await DATA_FETCHERS[dt]()
            df = df[df['real_exchange'] == RealExchange.REAL_EXCHANGE_MOEX]
            if dt == 'futures':
                df = self._apply_futures_filters(df)
                df = df[FUTURES_KEEP_COLUMNS]
                df['basic_asset_size'] = df['basic_asset_size'].apply(
                    lambda a: a['units']
                )
            elif dt == 'stocks':
                df = df[STOCKS_KEEP_COLUMNS]
            data.store_df(df)

    def _apply_futures_filters(self, df: pd.DataFrame) -> pd.DataFrame:
        now = datetime.datetime.now().date()
        df['expiration_date'] = pd.to_datetime(df['expiration_date']).dt.date
        df = df[df['expiration_date'] > now + datetime.timedelta(days=3)]
        df = df[df['asset_type'] == 'TYPE_SECURITY']
        return df


class DividendCounter:
    def __init__(self, storage, ticker: str = '') -> None:
        self._ticker = ticker.upper()
        self._stocks_db = self._futures_db = self._stock = self._futures = None
        self._handler = THandler(storage)

    async def count(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        await self._load_data()
        await self._fill_missing_numbers()
        self._count_dividends()
        return self._futures, self._stock

    def _count_dividends(self) -> None:
        stock_price: Decimal = self._stock.iloc[0]['price']
        self._futures['dividend'] = self._futures.apply(
            self.count_dividend, axis=1, args=(stock_price,)
        )
        self._futures[
            'div_percent'
        ] = 100 * self._futures['dividend'] / float(stock_price)

    @staticmethod
    def count_dividend(row: pd.Series, stock_price: Decimal) -> float:
        daily_discount_rate = Decimal(DISCOUNT_RATE) / Decimal('365') / 100
        present_value = row['price'] / (1 + daily_discount_rate) ** row['days']
        dividend = stock_price - (present_value / row['basic_asset_size'])
        return float(dividend)
        # daily_discount_rate = (
        #     Decimal('1') + (Decimal(DISCOUNT_RATE) / 100)
        # ) ** (Decimal('1') / Decimal('365')) - 1
        # present_value = row['price'] / (
        # 1 + daily_discount_rate
        # ) ** row['days']
        # dividend = stock_price - (present_value / row['basic_asset_size'])
        # return float(dividend)

    async def _fill_missing_numbers(self) -> None:
        self._futures['days'] = (pd.to_datetime(self._futures[
            'expiration_date'
        ]).dt.date - datetime.datetime.now().date()).apply(lambda d: d.days)
        self._stock['price'] = await self._get_stock_buy_price()
        self._futures['price'] = await self._get_futures_sell_prices()

    async def _get_futures_sell_prices(self) -> list[Decimal]:
        if FORCE_LAST_PRICE or not await is_trading_now(self._futures.iloc[0]):
            return await get_last_prices(self._futures['uid'])
        tasks = [get_orderbook_price(
            row['uid'], sell=True
        ) for _, row in self._futures.iterrows()]
        results = await asyncio.gather(*tasks)
        return results

    async def _get_stock_buy_price(self) -> list[Decimal]:
        if FORCE_LAST_PRICE or not await is_trading_now(self._futures.iloc[0]):
            return await get_last_prices(self._stock['uid'])
        return await get_orderbook_price(
            self._stock.iloc[0]['uid'], sell=False
        )

    async def _load_data(self) -> None:
        await self._update_from_db()
        self._stock = self._stocks_db.loc[self._stocks_db[
            'ticker'
        ] == self._ticker].copy()
        if self._stock.empty:
            raise ValidationError(f'Тикер {self._ticker} не найден в базе')
        self._futures = self._futures_db[self._futures_db[
            'basic_asset'
        ] == self._ticker].sort_values(
            by='expiration_date', ascending=True
        ).copy()
        if self._futures is None or self._futures.empty:
            raise ValidationError(f'Для тикера {self._ticker} нет фьючерсов')

    async def _update_from_db(self) -> None:
        self._stocks_db = await self._handler.get_data('stocks')
        self._futures_db = await self._handler.get_data('futures')

    async def list_available_tickers(self) -> str:
        await self._update_from_db()
        f_tickers = set(self._futures_db['basic_asset'])
        res = sorted([t for t in self._stocks_db['ticker'] if t in f_tickers])
        return ', '.join(res)


async def main():
    c = DividendCounter(STORAGE, 'sber')
    print(await c.count())
    # print(await DividendCounter(storage=STORAGE).list_available_tickers())


if __name__ == '__main__':
    asyncio.run(main())
