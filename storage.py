import datetime
import os
from abc import ABC, abstractmethod

import pandas as pd


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

    @abstractmethod
    def exists(self) -> bool:
        pass


class FileStorage(Storage):
    def __init__(self, name: str, db_timeout_hours: int = 24) -> None:
        self._filename = name + '.csv'
        self._db_update_timeout_hours = db_timeout_hours

    def store_df(self, df: pd.DataFrame) -> None:
        df.to_csv(self._filename, index=False)

    def retrieve_df(self) -> pd.DataFrame:
        return pd.read_csv(self._filename)

    def is_updated(self) -> bool:
        if not self.exists():
            return False
        current_time = datetime.datetime.now()
        creation_time = os.path.getctime(self._filename)
        creation_date = datetime.datetime.fromtimestamp(creation_time)
        time_difference = current_time - creation_date
        return time_difference < datetime.timedelta(
            hours=self._db_update_timeout_hours
        )

    def exists(self) -> bool:
        return os.path.isfile(self._filename)
