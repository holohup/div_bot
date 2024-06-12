from settings import STORAGE, TG_ADMIN_ID
from aiogram.filters import BaseFilter
from aiogram.types import Message
import pandas as pd
from typing import NamedTuple

class User:
    id: int
    is_admin: bool
    active: bool
    discount_reate: float
    force_last_price: bool


class IsAdmin(BaseFilter):
    async def __call__(self, message: Message) -> bool:
        return message.from_user.id == TG_ADMIN_ID


class UserHandler:
    _instance = None

    def __new__(cls, storage, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self, storage) -> None:
        self._storage = storage
        self._users = self._storage.load

    def register_user(self):
        pass

    def get_user(self):
        pass

    def _commit_changes(self):
        pass

    def activate_user(self):
        pass

    def change_discount_rate(self):
        pass

    def toggle_force_last_price(self):
        pass


if __name__ == '__main__':
    handler = UserHandler(STORAGE)
    
