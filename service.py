import datetime
from typing import Literal
from t_api import fetch_futures, fetch_stocks
import asyncio
from storage import Storage, FileStorage
import pandas as pd
from tinkoff.invest.schemas import RealExchange
from settings import FUTURES_KEEP_COLUMNS, STOCKS_KEEP_COLUMNS


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
        if data.is_updated():
            df = await DATA_FETCHERS[dt]()
            df = df[df['real_exchange'] == RealExchange.REAL_EXCHANGE_MOEX]
            if dt == 'futures':
                df = self._apply_futures_filters(df)
                df = df[FUTURES_KEEP_COLUMNS]
            elif dt == 'stocks':
                df = df[STOCKS_KEEP_COLUMNS]
            data.store_df(df)

        return data.retrieve_df()

    def _apply_futures_filters(self, df: pd.DataFrame) -> pd.DataFrame:
        now = datetime.datetime.now().date()
        df['expiration_date'] = pd.to_datetime(df['expiration_date']).dt.date
        df = df[df['expiration_date'] > now]
        df = df[df['asset_type'] == 'TYPE_SECURITY']
        df['basic_asset_size'] = df['basic_asset_size'].apply(
            lambda a: a['units']
        )
        return df


async def main():
    h = THandler(FileStorage)
    print(await h.get_data('futures'))
    print(await h.get_data('stocks'))


if __name__ == '__main__':
    asyncio.run(main())
