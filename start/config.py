# start/config.py
import os

# Paths configuration
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DATA_PATH = os.path.join(BASE_DIR, 'data')
LOGS_PATH = os.path.join(BASE_DIR, 'logs')

# Exchange configuration
SUPPORTED_EXCHANGES = ['binance', 'bybit', 'kucoin', 'gateio', 'okx']
DEFAULT_EXCHANGE = 'bybit'
DEFAULT_ASSET = 'BTC'  # Solo asset, il pair viene dedotto dopo
# DEFAULT_MARKET_TYPE = 'spot'      # Default market type Non usato per ora

# Data download configuration
TIMEFRAME = '1m'        # For OHLCV candles
BATCH_SAVE_SIZE = 10
DEFAULT_START_DATE = '2000-01-01'  # Per evitare problemi con exchange come Bybit

# Timeframes for different data types
FUNDING_TIMEFRAME = '1h'  # Funding rate typically every 8h but for some new pairs can be less
OI_TIMEFRAME = '4h'       # Open Interest - start with 1h, can be changed

# Additional metrics configuration (for perpetual only)
FETCH_FUNDING = False
FETCH_OPEN_INTEREST = False

# Technical configuration
USE_CCXT = True
# CCXT gestisce automaticamente i rate limits - non serve configurazione

# Directory structure
DATA_DIRECTORIES = {
    'spot': os.path.join(DATA_PATH, 'spot'),
    'perpetual': os.path.join(DATA_PATH, 'perpetual'), 
    #'funding': os.path.join(DATA_PATH, 'funding'),
    #'open_interest': os.path.join(DATA_PATH, 'open_interest'),
    'logs': LOGS_PATH
}