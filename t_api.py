from settings import TCS_RO_TOKEN, RETRY_SETTINGS
import pandas as pd
from tinkoff.invest.retrying.aio.client import AsyncRetryingClient


async def update_futures():
    async with AsyncRetryingClient(
        TCS_RO_TOKEN, settings=RETRY_SETTINGS
    ) as client:
        response = await client.instruments.futures()
        return pd.DataFrame(response.instruments)


async def update_stocks():
    async with AsyncRetryingClient(
        TCS_RO_TOKEN, settings=RETRY_SETTINGS
    ) as client:
        response = await client.instruments.shares()
        return pd.DataFrame(response.instruments)
