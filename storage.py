import datetime
import os
from abc import ABC, abstractmethod

import pandas as pd

from settings import DB_UPDATE_TIMEOUT_HOURS


class Storage(ABC):
    @abstractmethod
    def __init__(self) -> None:
        pass

    @abstractmethod
    def store_df(self, df: pd.DataFrame) -> None:
        pass

    @abstractmethod
    def retrieve_df(self) -> pd.DataFrame:
        pass

    @abstractmethod
    def is_updated(self) -> bool:
        pass


class FileStorage(Storage):
    def __init__(self, name: str) -> None:
        self._filename = name + '.csv'

    def store_df(self, df: pd.DataFrame) -> None:
        df.to_csv(self._filename, index=False)

    def retrieve_df(self) -> pd.DataFrame:
        return pd.read_csv(self._filename)

    def is_updated(self) -> bool:
        if not os.path.isfile(self._filename):
            return False
        current_time = datetime.datetime.now()
        creation_time = os.path.getctime(self._filename)
        creation_date = datetime.datetime.fromtimestamp(creation_time)
        time_difference = current_time - creation_date
        return time_difference < datetime.timedelta(
            hours=DB_UPDATE_TIMEOUT_HOURS
        )
