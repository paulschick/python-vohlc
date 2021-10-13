import os
import sqlite3 as sql
import pandas as pd
from .base.collect_data import create_exchange, run_ohlcvs_collection
from typing import List, Dict
from enum import Enum


class Persistence(Enum):
    NONE = 0
    CSV = 1
    SQLITE = 2


class Run:
    def __init__(self, configuration: List, persistance_config: Dict):
        self.config = configuration
        self.pers = persistance_config

    def collect_data(self):
        df = run_ohlcvs_collection(*self.config)
        if self.pers['type'].value is Persistence.CSV.value:
            if self.pers['initial_data'] is not None:
                df.to_csv(path_or_buf=self.pers['initial_data'], index=False)
        elif self.pers['type'].value is Persistence.SQLITE.value:
            if self.pers['vohlc_db_path'] is not None:
                con = sql.connect(self.pers['vohlc_db_path'])
                name = self.pers['sqlite_table1']
                df.to_sql(name=name, con=con, index=False)


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

    def configure_client(self, symbol, timeframe, from_iso) -> Run:
        """Goes directly into run_ohlcvs_collection"""
        self._configuration = [self.exchange, symbol, timeframe, from_iso]
        self._persistence['type'] = self._ptype
        self._persistence['initial_data'] = self.initial_filepath
        self._persistence['vohlc_db_path'] = self.vohlc_path
        self._persistence['sqlite_table1'] = self._table1
        self._persistence['sqlite_table2'] = self._table2
        return Run(self._configuration, self._persistence)
