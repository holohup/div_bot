from settings import TG_ADMIN_IDS, STORAGE, USER_FIELDS, DEFAULT_DISCOUNT_RATE
import pandas as pd
import asyncio
from service import THandler


async def init_stocks_and_futures_db() -> None:
    for s in 'stocks', 'futures':
        data_storage = STORAGE(s)
        if not data_storage.exists():
            db_handler = THandler(STORAGE)
            await db_handler.update_data(s, data_storage)


async def init_users_db() -> None:
    users_storage = STORAGE('users')
    if not users_storage.exists():
        admin_row = [TG_ADMIN_IDS, True, True, DEFAULT_DISCOUNT_RATE, False]
        df = pd.DataFrame(data=[admin_row], columns=USER_FIELDS)
        users_storage.store_df(df)


async def main():
    await init_stocks_and_futures_db()
    await init_users_db()


if __name__ == '__main__':
    asyncio.run(main())
