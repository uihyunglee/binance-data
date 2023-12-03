from datetime import datetime as dt
from datetime import timedelta as td
import json
import re
import os
import sys

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
        if not os.path.exists(config_path):
            raise FileNotFoundError("config.json must exist in the path. ")
        with open('config.json', 'r') as json_file:
            _config = json.load(json_file)
        if 'db' not in _config:
            raise ValueError("config.json must contain 'db' key.")
        db_info = _config['db']
        required_keys = ["host", "port", "user", "password", "dbname"]
        if not all(key in db_info for key in required_keys):
            raise ValueError("db_info must contain host, port, user, password, and dbname.")
        self.conn = psycopg2.connect(**db_info)

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
        all_symbol_dict = self.client.get_all_tickers()
        all_symbols = list(map(lambda x: x['symbol'], all_symbol_dict))
        usdt_symbols = [symbol for symbol in all_symbols if symbol.endswith('USDT')]
        return usdt_symbols

    def get_future_symbols(self):
        future_symbol_dict = self.client.futures_symbol_ticker()
        future_symbols = list(map(lambda x: x['symbol'], future_symbol_dict))
        usdt_future_symbols = [symbol for symbol in future_symbols if 'USDT' in symbol]
        return usdt_future_symbols
