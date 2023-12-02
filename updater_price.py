from datetime import datetime as dt
from datetime import timedelta as td
import json

import numpy as np
import pandas as pd

from binance.client import Client

import psycopg2


class PriceUpdater:
    def __init__(self, interval='1d', symbols=['BTCUSDT']):
        self.client = Client("", "")
        self.interval = interval
        self.symbols = symbols

    def get_spot_tickers(self):
        all_ticker_dict = self.client.get_all_tickers()
        all_tickers = list(map(lambda x: x['symbol'], all_ticker_dict))
        usdt_tickers = [ticker for ticker in all_tickers if ticker.endswith('USDT')]
        return usdt_tickers
