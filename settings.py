import os

from dotenv import load_dotenv
from tinkoff.invest.retrying.settings import RetryClientSettings
from storage import FileStorage

# tinkoff settings
RETRY_SETTINGS = RetryClientSettings(use_retry=True, max_retry_attempt=100)
STORAGE = FileStorage
load_dotenv()
TCS_RO_TOKEN = os.getenv('TCS_RO_TOKEN', '000')
TCS_ACCOUNT_ID = os.getenv('TG_ACCOUNT_ID', '000')
TG_BOT_TOKEN = os.getenv(
    'TG_BOT_TOKEN', '123456789:AABBCCDDEEFFaabbccddeeff-1234567890'
)
TG_ADMIN_IDS = int(os.getenv('TG_ADMIN_IDS', '000'))

FUTURES_KEEP_COLUMNS = [
    'ticker',
    'basic_asset',
    'basic_asset_size',
    'expiration_date',
    'uid',
    'initial_margin_on_sell',
    'initial_margin_on_buy'
]
STOCKS_KEEP_COLUMNS = [
    'ticker',
    'name',
    'uid',
]
ORDERBOOK_DEPTH = 1
DEFAULT_DISCOUNT_RATE = 19
USER_FIELDS = [
    'id', 'is_admin', 'approved', 'discount_rate', 'force_last_price'
]
DEFAULT_USER_SETTINGS = [False, False, DEFAULT_DISCOUNT_RATE, True]
