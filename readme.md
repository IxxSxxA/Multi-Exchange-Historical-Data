### MEHD - Multi-Exchange Historical Data

Bot Python per scaricare dati storici crypto da exchange multipli.
Scarica tutto lo storico disponibile (OHLCV, funding rate, open interest) via API pubbliche CCXT e salva in formato Parquet per analisi/ML.

# 🚀 Caratteristiche
Exchange Supportati: Binance, Bybit, KuCoin, Gate.io, OKX e exchanges supportati da ccxt

Dati Scaricati:
Spot: Candele OHLCV 1m
Perpetual: Candele OHLCV 1m + Funding Rate + Open Interest
Formato: Parquet ottimizzato per analisi
Interfaccia: CLI semplice e intuitiva
Gestione File: Append intelligente o sovrascrittura

# 📁 Struttura Progetto

Multi-Exchange-Historical-Data/
├── data/
│   ├── spot/                          # Candele spot (1m)
│   │   ├── binance_spot_BTC-USDT_1m.parquet
│   │   └── bybit_spot_SOL-USDT_1m.parquet
│   ├── perpetual/                     # Candele perpetual (1m)  
│   │   ├── bybit_perpetual_BTC-USDT_1m.parquet
│   │   └── binance_perpetual_ETH-USDT_1m.parquet
│   ├── funding/                       # Funding rate perpetual (8h)
│   │   ├── bybit_perpetual_BTC-USDT_funding.parquet
│   │   └── binance_perpetual_ETH-USDT_funding.parquet
│   └── open_interest/                 # Open interest perpetual (1h)
│       ├── bybit_perpetual_BTC-USDT_oi.parquet
│       └── binance_perpetual_ETH-USDT_oi.parquet
├── logs/
│   └── 2024-01-15.log                 # Log di esecuzione
├── start/
│   ├── mehd.py                        # Script principale
│   └── config.py                      # Configurazione
├── utils/
│   ├── check_raw_parquet.py           # Controllo file Parquet
│   ├── date_utils.py                  # Gestione date/timestamp
│   ├── file_utils.py                  # Operazioni file Parquet
│   ├── market_utils.py                # Rilevamento tipo mercato
│   └── logger.py                      # Sistema di logging
├── .gitignore
└── README.md


# ⚡ Installazione

git clone <repository-url>
cd Multi-Exchange-Historical-Data
python -m venv .venv
source .venv/bin/activate  # Linux/Mac

# .venv\Scripts\activate  # Windows

# Installa dipendenze
pip install ccxt pandas pyarrow tqdm colorama

🎯 Utilizzo e Esempi di Esecuzione:

python start/mehd.py

🚀 Avvio MEHD - Multi-Exchanges Historical Data Downloader

Exchange [bybit]: bybit
Asset [BTC]: SOL

🔍 Cercando coppie disponibili per SOL su bybit...

📊 COPPIE TROVATE:
[1] SOL/USDT (SPOT) - Volume alto
[2] SOL/USDT:USDT (PERPETUAL) - Volume altissimo
[3] SOL/BTC (SPOT) - Volume basso
[4] SOL/USDC (SPOT) - Volume medio  
[5] SOL/USD (PERPETUAL) - Volume alto
[6] Tutte le coppie

Scelta [1-6]: 6

💡 METRICHE AGGIUNTIVE (solo perpetual):
[1] Funding Rate | [2] Open Interest | [3] Tutto
Selezione: 3

💡 MODALITÀ DOWNLOAD:
(1) APPEND - Continua da file esistenti  
(2) OVERWRITE - Ricomincia da zero
Scelta [1-2]: 1

📅 Start date [2000-01-01]: 2023-01-01
📊 Formato Dati
Candele OHLCV (Spot & Perpetual)
python

# Colonne: timestamp_ms, open, high, low, close, volume, trades_count
timestamp_ms      open    high    low     close   volume
1640995200000     100.0   101.0   99.0    100.5   1000
1640995260000     100.5   101.5   100.0   101.0   1500
Funding Rate (Perpetual)
python

# Colonne: timestamp_ms, funding_rate
timestamp_ms      funding_rate
1641024000000     0.0001
1641052800000     0.0002
Open Interest (Perpetual)
python

# Colonne: timestamp_ms, open_interest
timestamp_ms      open_interest
1640995200000     1500000
1640998800000     1510000

# ⚙️ Configurazione
Modifica start/config.py per personalizzare:

python
SUPPORTED_EXCHANGES = ['binance', 'bybit', 'kucoin', 'gateio', 'okx']
DEFAULT_EXCHANGE = 'bybit'
DEFAULT_PAIR = 'BTC/USDT'
TIMEFRAME = '1m'
DATA_PATH = 'data'
LOGS_PATH = 'logs'

# 🔄 Gestione File Esistenti
APPEND: Continua dall'ultimo timestamp disponibile
OVERWRITE: Cancella e ricomincia da zero
Controllo Automatico: Evita duplicati e gap nei timestamp

# 📈 Analisi Dati
I file Parquet possono essere letti facilmente con pandas:

python
import pandas as pd

# Carica candele
df = pd.read_parquet('data/spot/binance_spot_BTC-USDT_1m.parquet')

# Carica funding rate
funding = pd.read_parquet('data/funding/bybit_perpetual_BTC-USDT_funding.parquet')

# Combina per analisi
df['funding_rate'] = df['timestamp_ms'].map(
    funding.set_index('timestamp_ms')['funding_rate']).ffill()

# 🐛 Risoluzione Problemi
Errore connessione exchange: Verifica la connessione internet e che l'exchange sia operativo
Rate limit raggiunto: Il programma gestisce automaticamente i limiti API
File corrotto: Usa OVERWRITE per rigenerare i file problematici

📄 Licenza
MIT License - Sentiti libero di usare e modificare per i tuoi progetti.