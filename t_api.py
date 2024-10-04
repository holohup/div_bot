from decimal import Decimal

import pandas as pd
from tinkoff.invest.retrying.aio.client import AsyncRetryingClient
from tinkoff.invest.schemas import InstrumentIdType as IdType, IndicativesRequest
from tinkoff.invest.schemas import SecurityTradingStatus as TStatus, GetLastPricesResponse, LastPriceType
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
        response_shares = await client.instruments.shares()
        response_indicatives = await client.instruments.indicatives(request=IndicativesRequest())
    shares_df = pd.DataFrame(response_shares.instruments)
    indicatives_df = pd.DataFrame(response_indicatives.instruments)
    result = pd.concat([shares_df, indicatives_df], ignore_index=True)
    return result


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
        response: GetLastPricesResponse = await client.market_data.get_last_prices(
            instrument_id=uids.to_list(), last_price_type=LastPriceType.LAST_PRICE_EXCHANGE
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


async def get_index_futures():
    async with AsyncRetryingClient(TCS_RO_TOKEN, settings=RETRY_SETTINGS) as client:
        response = await client.instruments.indicatives(request=IndicativesRequest())
    all_indexes = pd.DataFrame(response.instruments)
    return all_indexes[(all_indexes['ticker'] == 'IMOEX') | (all_indexes['ticker'] == 'RTSI')]
