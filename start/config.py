# start/config.py
import os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
SUPPORTED_EXCHANGES = ['binance', 'bybit', 'kucoin', 'gateio', 'okx']
DEFAULT_EXCHANGE = 'bybit'
DEFAULT_PAIR = 'MNT/USDT'
TIMEFRAME = '1m'
BATCH_SAVE_SIZE = 10
DATA_PATH = os.path.join(BASE_DIR, 'data', 'candles')
LOGS_PATH = os.path.join(BASE_DIR, 'logs')
USE_CCXT = True