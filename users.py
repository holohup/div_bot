from settings import STORAGE, DEFAULT_USER_SETTINGS
from aiogram.filters import BaseFilter
from aiogram.types import Message
import pandas as pd


class UserHandler:
    _instance = None

    def __new__(cls, storage, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self, storage) -> None:
        self._storage = storage('users')
        self._users: pd.DataFrame = self._storage.retrieve_df().set_index('id')
        self._users.index.name = 'id'

    def register_user(self, id_) -> str:
        if self.is_registered(id_):
            return
        self._users.loc[id_] = DEFAULT_USER_SETTINGS
        self._save_users_snapshot()

    def approve_user(self, id_) -> str:
        if not self.is_registered(id_):
            return f'User {id_} is not registered!'
        self._users.loc[id_, 'approved'] = not self._users.loc[id_, 'approved']
        self._save_users_snapshot()
        new_status = 'APPROVED' if bool(
            self._users.loc[id_, 'approved']
        ) is True else 'NOT APPROVED'
        return f'User {id_} new status: {new_status}'

    def _save_users_snapshot(self):
        self._storage.store_df(self._users.reset_index())

    def change_discount_rate(self, id_, new_rate):
        pass

    def toggle_force_last_price(self):
        pass

    def is_admin(self, id_):
        return self.is_registered(id_) and bool(
            self._users.loc[id_]['is_admin']
        ) is True

    def is_approved(self, id_):
        return self.is_registered(id_) and bool(
            self._users.loc[id_]['approved']
        ) is True

    def is_registered(self, id_):
        return id_ in self._users.index


class IsAdmin(BaseFilter):
    async def __call__(self, message: Message) -> bool:
        handler = UserHandler(STORAGE)
        return handler.is_admin(int(message.from_user.id))


class IsApproved(BaseFilter):
    async def __call__(self, message: Message) -> bool:
        handler = UserHandler(STORAGE)
        return handler.is_approved(int(message.from_user.id))


if __name__ == '__main__':
    handler = UserHandler(STORAGE)
