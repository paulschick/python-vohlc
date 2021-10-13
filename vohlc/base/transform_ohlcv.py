import pandas as pd
from typing import List


def vohlc_cols() -> List[str]:
    return ['open', 'high', 'low', 'close']


def parse_ohlc(df: pd.DataFrame) -> pd.DataFrame:
    op = float(df.loc[0, 'open'])
    last_idx = df.index[-1]
    cl = float(df.loc[last_idx, 'close'])
    max_ = df.high.max()
    min_ = df.low.min()
    return pd.DataFrame(
        columns=vohlc_cols(),
        data=[[op, max_, min_, cl]]
    )


def batch_df(df: pd.DataFrame, batch_size: int):
    df['volume'] = df['volume'].astype(float)
    df['total'] = df['volume'].cumsum()
    df['batch'] = (df['total'] / batch_size).astype(int)
    grouped_df = df.groupby('batch')
    ohlc_dfs = []
    for d in grouped_df.groups.keys():
        _df = grouped_df.get_group(d)
        _df.reset_index(inplace=True, drop=True)
        ohlc_df = parse_ohlc(_df)
        ohlc_dfs.append(ohlc_df)
    master_df = pd.concat(ohlc_dfs)
    master_df.reset_index(inplace=True, drop=True)
    return master_df
