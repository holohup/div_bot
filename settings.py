import os

from dotenv import load_dotenv
from tinkoff.invest.retrying.settings import RetryClientSettings

# tinkoff settings
RETRY_SETTINGS = RetryClientSettings(use_retry=True, max_retry_attempt=100)

load_dotenv()
TCS_RO_TOKEN = os.getenv('TCS_RO_TOKEN', '000')
TCS_RW_TOKEN = os.getenv('TCS_RW_TOKEN', '000')
TCS_ACCOUNT_ID = os.getenv('TG_ACCOUNT_ID', '000')
TELEGRAM_TOKEN = os.getenv(
    'TG_TOKEN', '123456789:AABBCCDDEEFFaabbccddeeff-1234567890'
)
TELEGRAM_CHAT_ID = int(os.getenv('TG_CHAT_ID', '000'))

DB_UPDATE_TIMEOUT_HOURS = 24

FUTURES_KEEP_COLUMNS = [
    'ticker',
    'basic_asset',
    'basic_asset_size',
    'expiration_date',
    'uid',
]
STOCKS_KEEP_COLUMNS = [
    'ticker',
    'name',
    'uid',
]
