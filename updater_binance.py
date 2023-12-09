import json
import os
import re
from datetime import datetime as dt
from datetime import timedelta as td

import numpy as np
import pandas as pd
import psycopg2
from binance.client import Client

VALID_INTERVALS = ['1m', '3m', '5m', '30m', '1h', '2h', '4h', '6h', '8h', '1d', '1w', '1M']
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
                opentime BIGINT,
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

        init_col = ['opentime', 'open', 'high', 'low', 'close', 'vol', 'closetime', 'trd_val',
                    'trd_num', 'taker_buy_vol', 'taker_buy_trd_val', 'ignore']
        kline_df = pd.DataFrame(columns=init_col, data=kline)
        str2float_col = ['open', 'high', 'low', 'close', 'vol', 'trd_val', 'taker_buy_vol', 'taker_buy_trd_val']
        str2int_col = ['trd_num', 'ignore']

        kline_df[str2float_col] = kline_df[str2float_col].astype(np.float64)
        kline_df[str2int_col] = kline_df[str2int_col].astype(np.int64)
        kline_df['cddt'] = pd.to_datetime(kline_df['opentime'], unit='ms') + td(hours=9)
        return kline_df

    def get_start_time(self, symbol):
        with self.conn.cursor() as curs:
            sql = f"""
            SELECT MAX(opentime) FROM {self.table_name}
            WHERE symbol = '{symbol}'
            ;
            """
            curs.execute(sql)
            rs = curs.fetchone()
            if rs[0] is not None:
                start_time = rs[0] + 1000
                start_time = pd.to_datetime(start_time, unit='ms') + td(hours=9)
                start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
                return start_time
            else:
                start_time = self.init_start_date
                start_time = f'{start_time[:4]}-{start_time[4:6]}-{start_time[6:]} 00:00:00'
                return start_time

    def update_price_data(self):
        symbol_cnt = len(self.symbols)
        end_date = dt.now().strftime('%Y-%m-%d %H:%M:%S')

        daily_start_col = ['dateint', 'symbol']
        intraday_start_col = ['symbol', 'cddt', 'dateint', 'hhmmint']
        start_col = daily_start_col if self.is_daily_form else intraday_start_col
        remain_col = ['opentime', 'open', 'high', 'low', 'close', 'vol', 'trd_val', 'trd_num', 'taker_buy_vol', 'taker_buy_trd_val']
        col = start_col + remain_col

        print(f'Total Symbol Count: {symbol_cnt}')
        print(self.symbols)

        for cnt, symbol in enumerate(self.symbols, start=1):
            print(f'[ {cnt} / {symbol_cnt} ] {symbol} {self.interval} Price Info Download...', end='\r')
            start_date = self.get_start_time(symbol)
            kline_df = self.get_data(symbol, self.interval, start_date, end_date, self.future).iloc[:-1,:]

            if len(kline_df) == 0:
                print(f'{symbol} Have no data.')
                continue

            kline_df.drop(columns=['closetime', 'ignore'], inplace=True)
            kline_df['symbol'] = symbol
            kline_df['dateint'] = kline_df['cddt'].dt.strftime('%Y%m%d').astype(int)
            kline_df['hhmmint'] = kline_df['cddt'].dt.strftime('%H%M').astype(int)
            kline_df['cddt'] = kline_df['cddt'].astype(str)
            kline_df = kline_df[col]
            print(f'[ {cnt} / {symbol_cnt} ] {symbol} {self.interval} Price Info DB Update...', end='\r')
            with self.conn.cursor() as curs:
                t_now = dt.now()
                for _, row in kline_df.iterrows():
                    update_values = str(row.to_list())[1:-1] + f", '{t_now}'"
                    sql = f"""
                    INSERT INTO {self.table_name}
                    VALUES ({update_values})
                    ;
                    """
                    curs.execute(sql)
                self.conn.commit()

                print(f'[ {cnt} / {symbol_cnt} ] {symbol} {self.interval} Price Info DB Update...OK')


if __name__ == '__main__':
    pu = PriceUpdater()
    target_intervals = VALID_INTERVALS
    # target_spot_symbols = pu.get_get_spot_tickers()
    # target_future_symbols = pu.get_get_future__tickers()
    target_spot_symbols = ['BTCUSDT', 'ETHUSDT', 'XRPUSDT']
    target_future_symbols = ['BTCUSDT', 'ETHUSDT', 'XRPUSDT']

    for interval in target_intervals:
        print(f'--- interval {interval} Update ---')

        print(f'Spot:')
        pus = PriceUpdater(interval=interval, symbols=target_spot_symbols, future=False)
        pus.update_price_data()

        print(f'Future:')
        puf = PriceUpdater(interval=interval, symbols=target_future_symbols, future=True)
        puf.update_price_data()
