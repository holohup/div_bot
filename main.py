from aiogram import Bot, Dispatcher
# from aiogram.filters import Command
from aiogram.types import Message

from service import DividendCounter
from settings import TG_BOT_TOKEN

bot = Bot(TG_BOT_TOKEN)
dp = Dispatcher()


@dp.message()
async def process_other_answers(message: Message):
    futures, stock = await DividendCounter(message.text).count()
    df_string = futures[
        ['ticker', 'expiration_date', 'days', 'dividend', 'div_percent']
    ].to_string(index=False)
    await message.reply(f"<pre>{df_string}</pre>", parse_mode='HTML')

if __name__ == '__main__':
    dp.run_polling(bot)
