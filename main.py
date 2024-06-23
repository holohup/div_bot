from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message
from settings import STORAGE
from users import IsAdmin, UserHandler, IsApproved
from service import DividendCounter
from settings import TG_BOT_TOKEN, TG_ADMIN_IDS
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
bot = Bot(TG_BOT_TOKEN)
dp = Dispatcher()
user_handler = UserHandler(STORAGE)


def parse_command(cmd: str) -> str:
    return cmd.split()[-1]


@dp.message(IsAdmin() and Command(commands='approve'))
async def approve_user(message: Message):
    id_ = int(parse_command(message.text))
    await message.answer(text=user_handler.approve_user(id_))


@dp.message(Command(commands='start'))
async def welcome_new_user(message: Message):
    id_ = message.from_user.id
    name = message.from_user.full_name
    user_handler.register_user(id_)
    await message.answer('Добро пожаловать в мир дивибота!')
    await bot.send_message(
        chat_id=TG_ADMIN_IDS,
        text=f'У нас новый пользователь {name} id следующим сообщением'
    )
    await bot.send_message(
        chat_id=TG_ADMIN_IDS,
        text=f'{id_}'
    )


@dp.message(Command(commands='list'))
async def process_ticker_list(message: Message):
    await message.answer('Список тикеров, для которых есть хотя бы один фьюч:')
    await message.answer(
        await DividendCounter(STORAGE).list_available_tickers()
    )


def format_dataframe_as_markdown(df):
    from tabulate import tabulate
    # Using tabulate library to create Markdown table
    table = tabulate(df, headers='keys', tablefmt='pipe')
    return f"```\n{table}\n```"


@dp.message(IsApproved())
async def process_other_answers(message: Message):
    try:
        futures, stock = await DividendCounter(STORAGE, message.text).count()
        short_columns = {
            'expiration_date': 'expires',
            'div_percent': 'div%',
            'dividend': 'div'
        }
        futures = futures.rename(columns=short_columns)
        futures['div'] = futures['div'].round(2)
        futures['div%'] = futures['div%'].round(2)
        futures['expires'] = pd.to_datetime(futures['expires'])
        futures['expires'] = futures['expires'].dt.strftime('%d.%m.%y')
        df_string = futures[
            ['ticker', 'expires', 'days', 'div', 'div%']
        ].to_string(index=False)
        await message.reply(
            f"Futures for {stock.iloc[0].ticker}:\n<pre>{df_string}</pre>",
            parse_mode='HTML'
        )

    except Exception as e:
        await message.answer(str(e))


if __name__ == '__main__':
    dp.run_polling(bot)
