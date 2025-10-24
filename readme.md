# README.md

# Multi-Exchanges-Historical-Data (mehd.py)

Bot Python per scaricare dati storici di candele crypto da exchange multipli, al timeframe minimo (1m). Usa CCXT per connettersi agli exchange, come faresti in JS, ma in stile Python per imparare il linguaggio. Scarica tutto lo storico disponibile (OHLC, volume, trades count, ecc.) via API pubbliche e salva in formato Parquet per analisi/ML future (es. TensorFlow, backtest tipo Alpha Arena). Interfaccia CLI semplice: rispondi a poche domande e parte il download. Niente aggregazione o plotting qui – solo dati grezzi e robustezza.

## Scopo

- Pull dati storici completi (dal primo disponibile) per un pair su un exchange.
- Salva in Parquet per analisi/ML efficienti.
- CLI minimal: scegli exchange, pair, start date (default: più vecchio possibile).
- Robustezza: valida exchange/pair, gestisci sovrascrittura/append, logga errori.
- Espandibile: per live data, aggregazione TF, o plotting, bot futuri.

## Struttura del Progetto

Multi-Exchanges-Historical-Data/
├── data/
│   ├── candles/           												// Candele storiche per exchange/pair/TF
│   │   ├── binance_BTC-USDT_1m.parquet           // Colonne: timestamp_ms, open, high, low, close, volume, trades_count, ...
│   │   └── ...            												// Un file  per combo exchange/pair
├── logs/
│   ├── 2025-10-17.log     // Log errori/progress
├── start/
│   ├── mehd.py            // Script principale CLI
│   └── config.py          // Default: exchange supportati, TF='1m', path dati, toggle CCXT 
├── utils/																		// Files di utilità generica
│   ├── date_utils.py													// Se serve -> Gestione timestamp/date (stile moment.js)
│   ├── file_utils.py												  	// Gestione file Parquet, check esistenza -> Tener compatto -> Se serve un domani lo scomponiamo in più files
│   ├── inspect_parquet.py										// Prog indipendente per test file parquet -> Lancia con python /utils/inspect_parquet.py
│   └── logger.py															// Setup logging
├── .gitignore						// File da ignorare per caricamento su github
└── README.md				// Questo file

## Setup

- Installa dipendenze: `pip install ccxt pandas pyarrow tqdm colorama`.
- Configura `config.py`: lista exchange supportati (da CCXT), default pair (es. 'BTC/USDT'), TF='1m', path dati.
- No API key necessarie (solo endpoint pubblici).

## Utilizzo

- **Scaricare dati**: `python start/mehd.py`
- **Ispezionare file Parquet**: `python utils/inspect_parquet.py`
  - Chiede exchange e pair, mostra numero di candele, date iniziali/finali, ultime candele e verifica gap nei dati.

## Flusso di Esecuzione

Lancia python start/mehd.py:

1. Chiede exchange (lista da CCXT/config; default: binance; valida via ping API).
2. Chiede pair (es. BTC/USDT; valida via API; check file in data/candles/ -> opzione sovrascrivi/append/ignora).
3. Chiede start date (default: più vecchio disponibile; check conflitti con dati esistenti). Poi: scarica candele 1m con CCXT, gestisce rate limits, mostra progress (tqdm), logga in logs/. Output: file Parquet in data/candles/ con tutti i campi API (timestamp_ms, OHLC, volume, trades_count, ecc.).

## Formato Dati

- Parquet: ottimizzato per ML/analisi con pandas. Colonne: timestamp_ms, open, high, low, close, volume, trades_count, altri campi API.
- Nomi file: {exchange}_{pair}_1m.parquet (es. bybit_ETH-USDT_1m.parquet).

## Note per Espansione

- ML: carica Parquet in pandas DataFrame per feature engineering (es. normalizza OHLC).
- Limiti: no buy/sell volumes storici (solo live via websocket); ok per ora.
- Debug: log dettagliati (errori API, progress); usa tqdm per feedback.
- Toggle CCXT: in config.py, opzione per implementazioni manuali (Scavalca ccxt per learning python).