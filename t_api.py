from settings import TCS_RO_TOKEN, RETRY_SETTINGS
import pandas as pd
from tinkoff.invest.retrying.aio.client import AsyncRetryingClient


async def fetch_futures() -> pd.DataFrame:
    async with AsyncRetryingClient(
        TCS_RO_TOKEN, settings=RETRY_SETTINGS
    ) as client:
        response = await client.instruments.futures()
        return pd.DataFrame(response.instruments)


async def fetch_stocks() -> pd.DataFrame:
    async with AsyncRetryingClient(
        TCS_RO_TOKEN, settings=RETRY_SETTINGS
    ) as client:
        response = await client.instruments.shares()
        return pd.DataFrame(response.instruments)
