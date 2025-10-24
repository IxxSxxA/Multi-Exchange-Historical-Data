# start/config.py
import os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
SUPPORTED_EXCHANGES = ['binance', 'bybit', 'kucoin', 'gateio', 'okx']
DEFAULT_EXCHANGE = 'bybit'
DEFAULT_PAIR = 'MNT/USDT'
DEFAULT_MARKET_TYPE = 'spot'  # 'spot' o 'perpetual'
TIMEFRAME = '1m'
BATCH_SAVE_SIZE = 10
DATA_PATH = os.path.join(BASE_DIR, 'data')
LOGS_PATH = os.path.join(BASE_DIR, 'logs')
USE_CCXT = True

# Configurazione per dati perpetual
FETCH_FUNDING = True
FETCH_OPEN_INTEREST = True