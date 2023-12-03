from datetime import datetime as dt
from datetime import timedelta as td
import json
import re
import os

import numpy as np
import pandas as pd

from binance.client import Client
import psycopg2

VALID_INTERVALS = ['1m', '3m', '5m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '1w', '1m']
DB_FORM_CHANGE_LEVEL = VALID_INTERVALS.index('1d')  # intraday form -> daily form


class PriceUpdater:
    def __init__(self, interval='1d', symbols=['BTCUSDT'], future=False, init_start_date='20180101'):
        if interval not in VALID_INTERVALS:
            raise ValueError(f"valid intervals are {VALID_INTERVALS}.")
        self.interval = interval
        self.is_daily_form = True if VALID_INTERVALS.index(interval) >= DB_FORM_CHANGE_LEVEL else False

        config_path = os.path.join(os.getcwd(), "config.json")
        if os.path.exists(config_path):
            with open('config.json', 'r') as json_file:
                _config = json.load(json_file)
            if 'db' in _config:
                db_info = _config['db']
                required_keys = ["host", "port", "user", "password", "dbname"]
                if all(key in db_info for key in required_keys):
                    self.conn = psycopg2.connect(**db_info)
                else:
                    raise ValueError("db_info must contain host, port, user, password, and dbname.")
            else:
                raise ValueError("config.json must contain 'db' key.")
        else:
            raise FileNotFoundError("config.json must exist in the path. ")

        self.client = Client("", "")
        self.symbols = symbols
        self.future = future
        self.init_start_date = re.sub(r'[^0-9]', '', init_start_date)

        table_name = f' binance_chart_{self.interval}'
        self.table_name = table_name + '_future' if self.future else table_name

        daily_start_field = 'dateint INT, symbol VARCHAR'
        intraday_start_field = 'symbol VARCHAR, cddt TIMESTAMP, dateint INT, hhmmint INT'
        start_field = daily_start_field if self.is_daily_form else intraday_start_field
        primary_key = 'dateint, symbol' if self.is_daily_form else 'symbol, cddt'

        with self.conn.cursor() as curs:
            sql = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                {start_field},
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                vol FLOAT,
                trd_val FLOAT,
                trd_num BIGINT,
                taker_buy_vol FLOAT,
                taker_buy_trd_val FLOAT,
                updated_at TIMESTAMP,
                PRIMARY KEY ({primary_key}))
            ;
            """
            curs.execute(sql)
        self.conn.commit()

    def get_spot_symbols(self):
        spot_symbol_dict = self.client.get_all_tickers()
        spot_symbols = list(map(lambda x: x['symbol'], spot_symbol_dict))
        usdt_spot_symbols = [spot_symbol for spot_symbol in spot_symbols if spot_symbol.endswith('USDT')]
        return usdt_spot_symbols

    def get_future_symbols(self):
        future_symbol_dict = self.client.futures_symbol_ticker()
        future_symbols = list(map(lambda x: x['symbol'], future_symbol_dict))
        usdt_future_symbols = [future_symbol for future_symbol in future_symbols if 'USDT' in future_symbol]
        return usdt_future_symbols

    def get_data(self, symbol, interval, start_date, end_date, future):
        start_date = int(dt.strptime(start_date, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
        end_date = int(dt.strptime(end_date, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)

        if future:
            kline = self.client.futures_historical_klines(symbol=symbol, interval=interval, start_str=start_date,
                                                          end_str=end_date)
        else:
            kline = self.client.get_historical_klines(symbol=symbol, interval=interval, start_str=start_date,
                                                      end_str=end_date)

        init_col = ['cddt', 'open', 'high', 'low', 'close', 'vol', 'closetime', 'trd_val',
                    'trd_num', 'taker_buy_vol', 'taker_buy_trd_val', 'ignore']
        kline_df = pd.DataFrame(columns=init_col, data=kline)
        str2float_col = ['open', 'high', 'low', 'close', 'vol', 'trd_val', 'taker_buy_vol', 'taker_buy_trd_val']
        str2int_col = ['trd_num', 'ignore']
        int2time_col = ['cddt', 'closetime']

        kline_df[str2float_col] = kline_df[str2float_col].astype(np.float64)
        kline_df[str2int_col] = kline_df[str2int_col].astype(np.int64)
        kline_df[int2time_col] = kline_df[int2time_col].apply(lambda s: pd.to_datetime(s, unit='ms') + td(hours=9))
        return kline_df

    def get_start_date(self, symbol):
        with self.conn.cursor() as curs:
            sql = f"""
            SELECT MAX(dateint) FROM {self.table_name}
            WHERE symbol = '{symbol}'
            ;
            """
            curs.execute(sql)
            rs = curs.fetchone()

            start_date = str(rs[0]) if rs[0] is not None else self.init_start_date
            start_date = f'{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}'
            if self.is_daily_form:
                start_date = pd.to_datetime(start_date) + td(days=1)
                start_date = start_date.strftime('%Y-%m-%d')
        return start_date + ' 09:00:00'
