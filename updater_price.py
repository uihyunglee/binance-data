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
