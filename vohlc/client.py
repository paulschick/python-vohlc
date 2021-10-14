import sqlite3 as sql
import pandas as pd
from .base.collect_data import create_exchange, run_ohlcvs_collection
from .base.transform_ohlcv import batch_df
from typing import List, Dict
from enum import Enum


class Persistence(Enum):
    NONE = 0
    CSV = 1
    SQLITE = 2


class Transform:
    def __init__(self, dataframe: pd.DataFrame, batch_size: int):
        self.df = dataframe
        self.batch = batch_size

    def transform_data(self) -> pd.DataFrame:
        return batch_df(self.df, self.batch)


class WriteCsv:
    def __init__(self, filepath: str, dataframe: pd.DataFrame):
        self.filepath = filepath
        self.df = dataframe

    def write(self):
        self.df.to_csv(self.filepath, index=False)


class WriteSql:
    def __init__(self, db_filepath, tablename, data: pd.DataFrame):
        self.db_file = db_filepath
        self.table = tablename
        self.df = data

    def write(self):
        con = sql.connect(self.db_file)
        self.df.to_sql(name=self.table, con=con, index=False)
        con.close()


class Collect:
    def __init__(self, exchange, symbol, timeframe, from_iso, batch_size):
        self.exchange = exchange
        self.symbol = symbol
        self.timeframe = timeframe
        self.from_iso = from_iso
        self.batch_size = batch_size

    def collect_data(self) -> Transform:
        df = run_ohlcvs_collection(self.exchange, self.symbol, self.timeframe, self.from_iso)
        return Transform(df, self.batch_size)


class RunBasic:
    def __init__(self, exchangeid: str, batch_size: int, rate_limit=600):
        self.exchange = create_exchange(exchangeid, rate_limit)
        self.batch_size = batch_size

    def collect(self, symbol, timeframe, from_iso) -> Collect:
        return Collect(self.exchange, symbol, timeframe, from_iso, self.batch_size)




# Example Usage
# df = Configuration('coinbasepro').configure_client('AVAX/USDT', '1h',
#                                                    '2021-05-01 00:00:00').collect_data().transform_data(1000000)
