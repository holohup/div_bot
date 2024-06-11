import datetime
from decimal import Decimal
from typing import Literal
from exceptions import ValidationError
from t_api import (fetch_futures, fetch_stocks,
                   is_trading_now, get_last_prices, get_orderbook_price)
import asyncio
from storage import Storage, FileStorage
import pandas as pd
from tinkoff.invest.schemas import RealExchange
from settings import FUTURES_KEEP_COLUMNS, STOCKS_KEEP_COLUMNS

FORCE_LAST_PRICE = False

DATA_FETCHERS = {
    'futures': fetch_futures,
    'stocks': fetch_stocks,
}


class THandler:
    def __init__(self, storage: Storage) -> None:
        self._storage = storage

    async def get_data(
        self, dt: Literal['futures', 'stocks']
    ) -> pd.DataFrame:
        data = self._storage(dt)
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
        return data.retrieve_df()

    def _apply_futures_filters(self, df: pd.DataFrame) -> pd.DataFrame:
        now = datetime.datetime.now().date()
        df['expiration_date'] = pd.to_datetime(df['expiration_date']).dt.date
        df = df[df['expiration_date'] > now + datetime.timedelta(days=3)]
        df = df[df['asset_type'] == 'TYPE_SECURITY']
        return df


class DividendCounter:
    def __init__(self, ticker: str) -> None:
        self._ticker = ticker.upper()
        self._stocks_db = self._futures_db = self._stock = self._futures = None
        self._handler = THandler(FileStorage)

    async def count(self):
        await self._load_data()
        await self._fill_missing_numbers()
        return self._futures, self._stock

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
        self._stocks_db = await self._handler.get_data('stocks')
        self._futures_db = await self._handler.get_data('futures')
        self._stock = self._stocks_db.loc[self._stocks_db[
            'ticker'
        ] == self._ticker]
        if self._stock.empty:
            raise ValidationError(f'Тикер {self._ticker} не найден в базе')
        self._futures = self._futures_db[self._futures_db[
            'basic_asset'
        ] == self._ticker].sort_values(by='expiration_date', ascending=True)
        if self._futures is None or self._futures.empty:
            raise ValidationError(f'Для тикера {self._ticker} нет фьючерсов')

    def list_available_tickers(self):
        f_tickers = set(self._futures_db['basic_asset'])
        res = sorted([t for t in self._stocks_db['ticker'] if t in f_tickers])
        return ', '.join(res)


async def main():
    c = DividendCounter('gazp')
    print(await c.count())
    # print(c.list_available_tickers())


if __name__ == '__main__':
    asyncio.run(main())
