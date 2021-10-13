import os
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
    def __init__(self, dataframe: pd.DataFrame, persistence_config: Dict):
        self.df = dataframe
        self.pers = persistence_config

    def transform_data(self, batch_size: int) -> pd.DataFrame:
        df = batch_df(self.df, batch_size)
        if self.pers['type'].value is Persistence.CSV.value:
            if self.pers['vohlc_db_path'] is not None:
                df.to_csv(path_or_buf=self.pers['vohlc_db_path'], index=False)
            elif self.pers['type'].value is Persistence.SQLITE.value:
                if self.pers['vohlc_db_path'] is not None:
                    con = sql.connect(self.pers['vohlc_db_path'])
                    name = self.pers['sqlite_table2']
                    df.to_sql(name=name, con=con, index=False)
                    con.close()
        return df


class Collect:
    def __init__(self, configuration: List, persistance_config: Dict):
        self.config = configuration
        self.pers = persistance_config

    def collect_data(self) -> Transform:
        df = run_ohlcvs_collection(*self.config)
        if self.pers['type'].value is Persistence.CSV.value:
            if self.pers['initial_data'] is not None:
                df.to_csv(path_or_buf=self.pers['initial_data'], index=False)
        elif self.pers['type'].value is Persistence.SQLITE.value:
            if self.pers['vohlc_db_path'] is not None:
                con = sql.connect(self.pers['vohlc_db_path'])
                name = self.pers['sqlite_table1']
                df.to_sql(name=name, con=con, index=False)
                con.close()
        return Transform(df, self.pers)


class Configuration:
    def __init__(self, exchangeid: str, rate_limit=600, initial_data_filepath=None, vohlc_path=None,
                 persistence_type=Persistence.NONE, initial_db_table=None, vohlc_table=None):
        self.exchange = create_exchange(exchangeid, rate_limit)
        self.initial_filepath = initial_data_filepath
        self.vohlc_path = vohlc_path
        self._configuration = []
        self._persistence = dict()
        self._ptype = persistence_type
        self._table1 = initial_db_table
        self._table2 = vohlc_table

    def configure_client(self, symbol, timeframe, from_iso) -> Collect:
        """Goes directly into run_ohlcvs_collection"""
        self._configuration = [self.exchange, symbol, timeframe, from_iso]
        self._persistence['type'] = self._ptype
        self._persistence['initial_data'] = self.initial_filepath
        self._persistence['vohlc_db_path'] = self.vohlc_path
        self._persistence['sqlite_table1'] = self._table1
        self._persistence['sqlite_table2'] = self._table2
        return Collect(self._configuration, self._persistence)


# Example Usage
# df = Configuration('coinbasepro').configure_client('AVAX/USDT', '1h',
#                                                    '2021-05-01 00:00:00').collect_data().transform_data(1000000)
