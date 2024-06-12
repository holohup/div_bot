from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message
from settings import STORAGE
from users import IsAdmin, UserHandler, IsApproved
from service import DividendCounter
from settings import TG_BOT_TOKEN, TG_ADMIN_ID

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
        chat_id=TG_ADMIN_ID,
        text=f'У нас новый пользователь {name}, id={id_}'
    )


@dp.message(Command(commands='list'))
async def process_ticker_list(message: Message):
    await message.answer('Список тикеров, для которых есть хотя бы один фьюч:')
    await message.answer(
        await DividendCounter(STORAGE).list_available_tickers()
    )


@dp.message(IsApproved())
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
