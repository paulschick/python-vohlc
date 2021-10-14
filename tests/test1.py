"""
1. Change the name from Configuration. Since that's the only import, make it meaningful. It's in
a cile called client, so something like that.
2. Change the persistence configuration to be more intuitive. I don't like having to pass everything
in this way, especially with the chaining, that just gets too big.
3. Test the persistence functionality, other than that it works great.
Definitely a program that will take some time to run depending on timeframe and start date.
"""
import os
from vohlc.client import RunBasic, RunCsv, RunSqlite


df = RunBasic('coinbasepro', 1000).collect('BTC/USD', '1d', '2021-01-01 00:00:00') \
    .collect_data().transform_data()

db_path = os.path.join(os.getcwd(), 'data', 'testdata.db')
csv_path = os.path.join(os.getcwd(), 'data', 'testdata.csv')

print(db_path)
print(csv_path)

print(df)
sym = 'BTC/USD'
tf = '1d'
iso = '2021-01-01 00:00:00'

csv_df = RunCsv('coinbasepro', 1000).collect(sym, tf, iso, csv_path)
sqlite_df = RunSqlite('coinbasepro', 1000).collect(sym, tf, iso, db_path, 'btc_usd_data')
