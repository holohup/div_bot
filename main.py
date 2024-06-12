from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message
from settings import STORAGE
from users import IsAdmin
from service import DividendCounter
from settings import TG_BOT_TOKEN

bot = Bot(TG_BOT_TOKEN)
dp = Dispatcher()


# @dp.message(IsAdmin())
# async def answer_if_admins_update(message: Message):
#     await message.answer(text='Вы админ')


@dp.message(Command(commands='list'))
async def process_ticker_list(message: Message):
    await message.answer('Список тикеров, для которых есть хотя бы один фьюч:')
    await message.answer(
        await DividendCounter(STORAGE).list_available_tickers()
    )


@dp.message()
async def process_other_answers(message: Message):
    try:
        futures, stock = await DividendCounter(STORAGE, message.text).count()
        df_string = futures[
            ['ticker', 'expiration_date', 'days', 'dividend', 'div_percent']
        ].to_string(index=False)
        await message.reply(f"<pre>{df_string}</pre>", parse_mode='HTML')
    except Exception as e:
        await message.answer(str(e))


if __name__ == '__main__':
    dp.run_polling(bot)
