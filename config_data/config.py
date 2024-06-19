from dataclasses import dataclass
from environs import Env

from tinkoff.invest.retrying.settings import RetryClientSettings


DEFAULT_DISCOUNT_RATE = 16


@dataclass
class TgBot:
    token: str
    admin_ids: list[int]


@dataclass
class DatabaseConfig:
    database: str
    db_host: str
    db_user: str
    db_password: str


@dataclass
class TCSConfig:
    retry_settings: RetryClientSettings
    token: str


@dataclass
class DefaultSettings:
    discount_rate: int
    user_fields: list[str]
    user_settings: list[bool, bool, int, bool]


@dataclass
class ParseSettings:
    futures_columns: list[str]
    stock_columns: list[str]
    orderbook_depth: int


@dataclass
class Config:
    tg_bot: TgBot
    db: DatabaseConfig
    tcs: TCSConfig
    parse: ParseSettings
    default: DefaultSettings


def load_config(path: str | None = None) -> Config:

    env: Env = Env()
    env.read_env(path)

    return Config(
        tg_bot=TgBot(
            token=env('TG_BOT_TOKEN'),
            admin_ids=list(map(int, env.list('TG_ADMIN_IDS')))
        ),
        db=DatabaseConfig('', '', '', ''),
        # db=DatabaseConfig(
        #     database=env('DATABASE'),
        #     db_host=env('DB_HOST'),
        #     db_user=env('DB_USER'),
        #     db_password=env('DB_PASSWORD')
        # )
        tcs=TCSConfig(
            retry_settings=RetryClientSettings(
                use_retry=True, max_retry_attempt=100
            ),
            token=env('TCS_RO_TOKEN'),
        ),
        parse=ParseSettings(
            futures_columns=['ticker',
                             'basic_asset',
                             'basic_asset_size',
                             'expiration_date',
                             'uid'],
            stock_columns=['ticker',
                           'name',
                           'uid',],
            orderbook_depth=1
        ),
        default=DefaultSettings(
            discount_rate=DEFAULT_DISCOUNT_RATE,
            user_fields=[
                'id',
                'is_admin',
                'approved',
                'discount_rate',
                'force_last_price'
            ],
            user_settings=[False, False, DEFAULT_DISCOUNT_RATE, True]
        )
    )
