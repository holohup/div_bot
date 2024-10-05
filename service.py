import asyncio
import datetime
from decimal import Decimal
from typing import Literal

import pandas as pd
from tinkoff.invest.schemas import RealExchange, MoneyValue, Quotation
from tinkoff.invest.utils import quotation_to_decimal

from exceptions import ValidationError
from settings import DEFAULT_DISCOUNT_RATE, FUTURES_KEEP_COLUMNS, STOCKS_KEEP_COLUMNS
from settings import STORAGE
from t_api import (
    fetch_futures,
    fetch_stocks,
    get_last_prices,
    get_orderbook_price,
    is_trading_now,
    get_index_futures,
)

FORCE_LAST_PRICE = True
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
            df = df[
                (df['real_exchange'] == RealExchange.REAL_EXCHANGE_MOEX)
                | (df['ticker'] == 'IMOEX')
                | (df['ticker'] == 'RTSI')
            ]
            if dt == 'futures':
                df = self._apply_futures_filters(df)
                df = df[FUTURES_KEEP_COLUMNS]
                df['basic_asset_size'] = df['basic_asset_size'].apply(lambda a: a['units'])
                df['initial_margin_on_buy'] = df['initial_margin_on_buy'].apply(
                    lambda a: quotation_to_decimal(MoneyValue(**a))
                )
                df['initial_margin_on_sell'] = df['initial_margin_on_sell'].apply(
                    lambda a: quotation_to_decimal(MoneyValue(**a))
                )
            elif dt == 'stocks':
                df = df[STOCKS_KEEP_COLUMNS]
            data.store_df(df)

    def _apply_futures_filters(self, df: pd.DataFrame) -> pd.DataFrame:
        now = datetime.datetime.now().date()
        # df.to_csv('test_futures.csv', index=False)
        df['expiration_date'] = pd.to_datetime(df['expiration_date']).dt.date
        df = df[df['expiration_date'] > now + datetime.timedelta(days=3)]
        df = df[
            (df['asset_type'] == 'TYPE_SECURITY')
            | (
                (df['asset_type'] == 'TYPE_INDEX')
                & df['name'].str.contains('мини', na=False)
            )
        ]
        return df


class IndexCounter:
    async def run(self):
        index_futures = await get_index_futures()
        index_futures['current_prices'] = await get_last_prices(index_futures['uid'])
        return index_futures


class DividendCounter:
    def __init__(self, storage, ticker: str = '') -> None:
        self._ticker = ticker.upper()
        self._stocks_db = self._futures_db = pd.DataFrame()
        self._stock = self._futures = pd.DataFrame()
        self._handler = THandler(storage)

    async def count(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        await self._load_data()
        await self._fill_missing_numbers()
        self._count_dividends()
        return self._futures, self._stock

    async def count_all(self):
        await self._update_from_db()
        self._futures = (
            self._futures_db[self._futures_db['basic_asset'] == self._ticker]
            .sort_values(by='expiration_date', ascending=True)
            .copy()
        )
        stock_tickers = set(self._futures_db['basic_asset'])
        stocks_with_futures = self._stocks_db[
            self._stocks_db['ticker'].isin(stock_tickers)
        ]
        combined_uids = pd.concat([stocks_with_futures, self._futures_db]).reset_index(
            drop=True
        )
        combined_uids['price'] = await get_last_prices(combined_uids['uid'])
        stocks_with_prices = (
            combined_uids[: len(stocks_with_futures)][['ticker', 'price']]
            .reset_index(drop=True)
            .sort_values(by='ticker')
        )
        futures_with_prices = combined_uids[len(stocks_with_futures) :]
        futures_with_prices = futures_with_prices[
            futures_with_prices['price'] > 0
        ].reset_index(drop=True)
        res = []
        for stock in stocks_with_prices.itertuples():
            futures = (
                futures_with_prices[futures_with_prices['basic_asset'] == stock.ticker]
                .sort_values(by='expiration_date', ascending=True)
                .copy()
            )
            if len(futures) < 1:
                continue
            futures['days'] = (
                pd.to_datetime(futures['expiration_date']).dt.date
                - datetime.datetime.now().date()
            ).apply(lambda d: d.days)
            futures['dividend'] = futures.apply(
                self.count_dividend, axis=1, args=(stock.price,)
            )
            for future in futures.itertuples():
                res.append(
                    {
                        'тикер': stock.ticker,
                        'цена': round(float(stock.price), 2),
                        'тикер фьюча': future.ticker,
                        'экспира': future.expiration_date,
                        # 'future_price': round(float(future.price), 2),
                        'дней': future.days,
                        'дивиденд': round(float(future.dividend), 2),
                    }
                )
            result = pd.DataFrame(res)
        filename = 'result.xlsx'
        with pd.ExcelWriter(filename) as writer:
            result.to_excel(writer, sheet_name='Подробно', index=False)
        return filename

    def _count_dividends(self) -> None:
        stock_price: Decimal = self._stock.iloc[0]['price']
        self._futures['dividend'] = self._futures.apply(
            self.count_dividend, axis=1, args=(stock_price,)
        )
        self._futures['div_percent'] = (
            100 * self._futures['dividend'] / float(stock_price)
        )
        fair_prices = self._futures.apply(
            self.count_fair_spread_price, axis=1, args=(stock_price,)
        )
        self._futures['sell_margin'] = self._futures.apply(
            self._sell_spread_margin, axis=1, args=(stock_price,)
        )
        self._futures['buy_margin'] = self._futures.apply(
            self._buy_spread_margin, axis=1, args=(stock_price,)
        )
        self._futures = pd.concat([self._futures, fair_prices], axis=1)

    @staticmethod
    def _sell_spread_margin(row: pd.Series, stock_price: Decimal) -> int:
        return int(
            (stock_price * Decimal(row['basic_asset_size']))
            # - row['price']
            + Decimal(row['initial_margin_on_sell'])
        )

    @staticmethod
    def _buy_spread_margin(row: pd.Series, stock_price: Decimal) -> int:
        return int(stock_price * Decimal(row['basic_asset_size']))


    @staticmethod
    def count_dividend(row: pd.Series, stock_price: Decimal) -> float:
        daily_discount_rate = Decimal(DISCOUNT_RATE) / Decimal('365') / 100
        present_value = row['price'] / (1 + daily_discount_rate) ** row['days']
        dividend = stock_price - (present_value / Decimal(row['basic_asset_size']))
        return float(dividend / Decimal('0.87'))

    @staticmethod
    def count_fair_spread_price(row: pd.Series, stock_price: Decimal) -> pd.Series:
        today_fut_price = stock_price * row['basic_asset_size']
        daily_discount_rate = Decimal(DISCOUNT_RATE) / Decimal('365') / 100
        fair_future_price = today_fut_price * (1 + daily_discount_rate) ** row['days']
        fair_spread_price = fair_future_price - today_fut_price
        current_spread_price = row['price'] - today_fut_price
        return pd.Series(
            {'current': float(current_spread_price), 'fair': float(fair_spread_price)}
        )

    async def _fill_missing_numbers(self) -> None:
        self._futures['days'] = (
            pd.to_datetime(self._futures['expiration_date']).dt.date
            - datetime.datetime.now().date()
        ).apply(lambda d: d.days)
        self._stock['price'] = await self._get_stock_buy_price()
        self._futures['price'] = await self._get_futures_sell_prices()

    async def _get_futures_sell_prices(self) -> list[Decimal]:
        if FORCE_LAST_PRICE or not await is_trading_now(self._futures.iloc[0]):
            return await get_last_prices(self._futures['uid'])
        tasks = [
            get_orderbook_price(row['uid'], sell=True)
            for _, row in self._futures.iterrows()
        ]
        results = await asyncio.gather(*tasks)
        return results

    async def _get_stock_buy_price(self) -> Decimal:
        if FORCE_LAST_PRICE or not await is_trading_now(self._futures.iloc[0]):
            return await get_last_prices(self._stock['uid'])
        return await get_orderbook_price(self._stock.iloc[0]['uid'], sell=False)

    async def _load_data(self) -> None:
        await self._update_from_db()
        self._stock = self._stocks_db.loc[
            self._stocks_db['ticker'] == self._ticker
        ].copy()
        if self._stock.empty:
            raise ValidationError(f'Тикер {self._ticker} не найден в базе')
        self._futures = (
            self._futures_db[self._futures_db['basic_asset'] == self._ticker]
            .sort_values(by='expiration_date', ascending=True)
            .copy()
        )
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
    print(await c.count_all())


if __name__ == '__main__':
    asyncio.run(main())
