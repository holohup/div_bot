from decimal import Decimal

import pandas as pd
from tinkoff.invest.retrying.aio.client import AsyncRetryingClient
from tinkoff.invest.schemas import InstrumentIdType as IdType
from tinkoff.invest.schemas import SecurityTradingStatus as TStatus
from tinkoff.invest.utils import quotation_to_decimal

from settings import ORDERBOOK_DEPTH, RETRY_SETTINGS, TCS_RO_TOKEN


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


async def is_trading_now(future: pd.Series):
    async with AsyncRetryingClient(
        TCS_RO_TOKEN, settings=RETRY_SETTINGS
    ) as client:
        response = await client.instruments.future_by(
            id_type=IdType.INSTRUMENT_ID_TYPE_UID, id=future['uid']
        )
    return (
        response.instrument.trading_status
        == TStatus.SECURITY_TRADING_STATUS_NORMAL_TRADING
    )


async def get_last_prices(uids: pd.Series) -> list[Decimal]:
    async with AsyncRetryingClient(
        TCS_RO_TOKEN, settings=RETRY_SETTINGS
    ) as client:
        response = await client.market_data.get_last_prices(
            instrument_id=uids.to_list()
        )
        result = [quotation_to_decimal(p.price) for p in response.last_prices]
        return result


async def get_orderbook_price(uid: str, sell: bool) -> Decimal:
    async with AsyncRetryingClient(
        TCS_RO_TOKEN, settings=RETRY_SETTINGS
    ) as client:
        ob = await client.market_data.get_order_book(
            instrument_id=uid, depth=ORDERBOOK_DEPTH
        )
    result = ob.bids[0] if sell else ob.asks[0]
    return quotation_to_decimal(result.price)
