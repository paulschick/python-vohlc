import ccxt
import pandas as pd
from datetime import datetime, timezone
from typing import List


def create_exchange(exchangeid, rate_limit=600):
    return getattr(ccxt, exchangeid)({
        'enableRateLimit': True,
        'rateLimit': rate_limit
    })


def start_time() -> int:
    today = datetime.utcnow().date()
    start_dt = datetime(today.year, today.month, today.day, tzinfo=timezone.utc)
    return int(start_dt.timestamp() * 1000)


def ohlcv_col_names() -> List[str]:
    return ['ts', 'open', 'high', 'low', 'close', 'volume']


def create_df(ohlcvs) -> pd.DataFrame:
    return pd.DataFrame(
        columns=ohlcv_col_names(),
        data=ohlcvs
    )


def update_df(dataframe: pd.DataFrame, ohlcvs) -> pd.DataFrame:
    df_2 = create_df(ohlcvs)
    df_ = pd.concat([dataframe, df_2])
    return df_.copy()


def ohlcvs_recursive(exchange, symbol, timeframe, end_timestamp, from_timestamp, dataframe=None) -> pd.DataFrame:
    if from_timestamp < end_timestamp:
        if dataframe is None:
            ohlcvs = exchange.fetch_ohlcv(symbol, timeframe, from_timestamp)
            df = create_df(ohlcvs)
            try:
                from_timestamp_ = ohlcvs[-1][0]
            except IndexError:
                return df
            return ohlcvs_recursive(exchange, symbol, timeframe, end_timestamp, from_timestamp_, df)
        else:
            ohlcvs = exchange.fetch_ohlcv(symbol, timeframe, from_timestamp)
            df = update_df(dataframe, ohlcvs)
            try:
                from_timestamp_ = ohlcvs[-1][0]
            except IndexError:
                return df
            return ohlcvs_recursive(exchange, symbol, timeframe, end_timestamp, from_timestamp_, df)
    else:
        return dataframe


def run_ohlcvs_collection(exchange, symbol, timeframe, from_iso) -> pd.DataFrame:
    from_timestamp = exchange.parse8601(from_iso)
    end_timestamp = start_time()
    return ohlcvs_recursive(exchange=exchange, symbol=symbol, timeframe=timeframe, end_timestamp=end_timestamp,
                            from_timestamp=from_timestamp)
