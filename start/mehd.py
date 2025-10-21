# start/mehd.py
import ccxt
import pandas as pd
from tqdm import tqdm
import time
import os
import sys
from colorama import init, Fore, Style

# Inizializza colorama
init(autoreset=True)

# Import da utils e config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.date_utils import parse_date, get_current_timestamp_ms, timestamp_to_datetime
from utils.file_utils import get_parquet_filename, check_file_exists, load_parquet, save_parquet, ensure_directory_exists, inspect_parquet
from utils.logger import setup_logger
from start.config import SUPPORTED_EXCHANGES, DEFAULT_EXCHANGE, DEFAULT_PAIR, TIMEFRAME, DATA_PATH, LOGS_PATH, USE_CCXT

def validate_exchange(exchange_name):
    """Valida se l'exchange è supportato e raggiungibile."""
    if exchange_name not in SUPPORTED_EXCHANGES:
        raise ValueError(f"Exchange '{exchange_name}' non supportato. Supportati: {SUPPORTED_EXCHANGES}")
    try:
        exchange_class = getattr(ccxt, exchange_name, None)
        if exchange_class is None:
            raise ValueError(f"Exchange '{exchange_name}' non trovato in CCXT")
        exchange = exchange_class()
        logger.info(f"Connessione a {exchange_name} in corso...")
        markets = exchange.load_markets()
        if markets is None:
            raise ValueError(f"Impossibile caricare i mercati per {exchange_name}: risposta None")
        logger.info(f"Mercati caricati per {exchange_name}, trovati {len(markets)} mercati")
        return exchange
    except Exception as e:
        logger.error(f"Errore dettagliato nella connessione a {exchange_name}: {str(e)}")
        raise ValueError(f"Errore nella connessione all'exchange {exchange_name}: {str(e)}")

def validate_pair(exchange, pair):
    """Valida se il pair esiste sull'exchange."""
    if pair not in exchange.markets:
        raise ValueError(f"Pair '{pair}' non disponibile su {exchange.id}")

def get_oldest_timestamp(exchange, pair):
    """Ottiene il timestamp più vecchio disponibile per il pair."""
    return 0  # Default: dal inizio possibile

def fetch_ohlcv(exchange, pair, tf, since, limit=1000):
    """Fetch OHLCV con gestione rate limits.
    
    Args:
        exchange: Oggetto exchange di CCXT.
        pair (str): Coppia di trading (es. 'BTC/USDT').
        tf (str): Timeframe delle candele (es. '1m').
        since (int): Timestamp di inizio in millisecondi.
        limit (int): Numero massimo di candele per richiesta (default: 1000).
    
    Returns:
        list: Lista di candele OHLCV o lista vuota in caso di errore.
    """
    try:
        return exchange.fetch_ohlcv(pair, tf, since=since, limit=limit)
    except ccxt.NetworkError as e:
        logger.error(f"Errore di rete durante il fetch: {e}")
        time.sleep(5)  # Attendi 5 secondi prima di ritentare
        return fetch_ohlcv(exchange, pair, tf, since, limit)  # Ritenta
    except ccxt.ExchangeError as e:
        logger.error(f"Errore exchange durante il fetch: {e}")
        return []  # Ritorna lista vuota se l'errore persiste
    except Exception as e:
        logger.error(f"Errore imprevisto durante il fetch: {e}")
        return []

import config  # Importa il file di configurazione

def download_historical_data(exchange, pair, start_timestamp=None, append=False):
    """Scarica tutti i dati storici dal start_timestamp o dal più vecchio, salvando in batch per robustezza e restituisce il DataFrame."""
    # Definisci filepath in base a exchange, pair e timeframe
    filepath = os.path.join(config.DATA_PATH, get_parquet_filename(exchange.id, pair, config.TIMEFRAME))
    
    data = []
    limit = 1000
    batch_save_size = config.BATCH_SAVE_SIZE
    batch_count = 0
    since = start_timestamp if start_timestamp else get_oldest_timestamp(exchange, pair)
    latest_timestamp = get_current_timestamp_ms()
    
    # Gestisci append in base all'esistenza del file
    if append and os.path.exists(filepath):
        logger.info(f"Aggiungendo dati a {filepath}...")
    else:
        logger.info(f"Creazione o sovrascrittura di {filepath}...")
        append = False
    
    try:
        with tqdm(desc=f"Download {pair} su {exchange.id}", unit=" batch") as pbar:
            while since < latest_timestamp:
                ohlcv = fetch_ohlcv(exchange, pair, config.TIMEFRAME, since, limit)
                if not ohlcv:
                    break
                data.extend(ohlcv)
                since = ohlcv[-1][0] + 1
                pbar.update(1)
                batch_count += 1
                time.sleep(1)
                
                # Salva in batch per robustezza
                if batch_count >= batch_save_size:
                    logger.info(f"Salvataggio intermedio di {len(data)} candele...")
                    df_batch = pd.DataFrame(data, columns=['timestamp_ms', 'open', 'high', 'low', 'close', 'volume'])
                    if len(data[0]) > 6:
                        df_batch['trades_count'] = [row[6] if len(row) > 6 else None for row in data]
                    save_parquet(df_batch, filepath, append=append)
                    data = []  # Svuota la memoria
                    batch_count = 0  # Reset counter
                    append = True  # Dopo il primo salvataggio, usa append
        
        # Salva l'ultimo batch (inclusa l'ultima candela)
        if data:
            logger.info(f"Salvataggio finale di {len(data)} candele...")
            df_final = pd.DataFrame(data, columns=['timestamp_ms', 'open', 'high', 'low', 'close', 'volume'])
            if len(data[0]) > 6:
                df_final['trades_count'] = [row[6] if len(row) > 6 else None for row in data]
            save_parquet(df_final, filepath, append=append)
        
        # Carica e restituisci il DataFrame completo
        if os.path.exists(filepath):
            df = load_parquet(filepath)
            logger.info(f"Dati caricati da {filepath} per restituzione.")
            return df
        else:
            logger.warning("File non trovato dopo salvataggio.")
            return None
    
    except Exception as e:
        logger.error(f"Errore durante il download: {e}")
        return None

def main():
    global logger
    ensure_directory_exists(DATA_PATH)
    ensure_directory_exists(LOGS_PATH)
    logger = setup_logger(LOGS_PATH)
    
    logger.info("🚀 Avvio MEHD - Multi-Exchanges Historical Data Downloader")
    
    # Input CLI con colori
    exchange_name = input(f"{Fore.CYAN}Exchange ({', '.join(SUPPORTED_EXCHANGES)}; default: {DEFAULT_EXCHANGE}): {Style.RESET_ALL}") or DEFAULT_EXCHANGE
    try:
        exchange = validate_exchange(exchange_name)
    except ValueError as e:
        logger.error(str(e))
        return
    
    pair = input(f"{Fore.CYAN}Pair (es. BTC/USDT; default: {DEFAULT_PAIR}): {Style.RESET_ALL}") or DEFAULT_PAIR
    try:
        validate_pair(exchange, pair)
    except ValueError as e:
        logger.error(str(e))
        return
    
    filename = get_parquet_filename(exchange.id, pair, TIMEFRAME)
    filepath = os.path.join(DATA_PATH, filename)
    
    append = False
    start_timestamp = None
    default_date_str = "più vecchio disponibile"
    
    if check_file_exists(filepath):
        action = input(f"{Fore.CYAN}File {filename} esiste. (o)verwrite, (a)ppend, (i)gnore? [o/a/i]: {Style.RESET_ALL}").lower()
        if action == 'i':
            logger.info("Download ignorato.")
            return
        append = action == 'a'
        if append:
            existing_df = load_parquet(filepath)
            if not existing_df.empty:
                first_timestamp = existing_df['timestamp_ms'].min()
                print(f"Min timestamp: {first_timestamp}")
                first_date = timestamp_to_datetime(first_timestamp).strftime('%Y-%m-%d')
                default_date_str = f"{first_date} (prima data nel file)"
                last_timestamp = existing_df['timestamp_ms'].max()
                start_timestamp = last_timestamp + 1
            else:
                default_date_str = "più vecchio disponibile (file vuoto)"
        else:
            start_timestamp = None
    else:
        append = False
        start_timestamp = None
    
    start_date_str = input(f"{Fore.CYAN}📅 Start date (YYYY-MM-DD; default: {default_date_str}): {Style.RESET_ALL}")
    if start_date_str:
        try:
            start_timestamp = parse_date(start_date_str)
        except ValueError as e:
            logger.error(str(e))
            return
    
    # Download
    df = download_historical_data(exchange, pair, start_timestamp, append)  # Passa append esplicitamente
    if df is not None and not df.empty:
        logger.info(f"🎉 Dati salvati in {filepath}")
        inspect_parquet(filepath, logger=logger)
    else:
        logger.warning("Nessun dato scaricato o file vuoto.")

if __name__ == "__main__":
    main()