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
from utils.file_utils import get_parquet_filename, get_data_directory, check_file_exists, load_parquet, save_parquet, ensure_directory_exists, inspect_parquet
from utils.market_utils import detect_market_type, get_perpetual_symbol
from utils.logger import setup_logger
from start.config import SUPPORTED_EXCHANGES, DEFAULT_EXCHANGE, DEFAULT_PAIR, DEFAULT_MARKET_TYPE, TIMEFRAME, DATA_PATH, LOGS_PATH, USE_CCXT, FETCH_FUNDING, FETCH_OPEN_INTEREST

# ⚠️ RIMUOVI QUESTA RIGA - NON SERVE!
# global FETCH_FUNDING, FETCH_OPEN_INTEREST

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
        # Prova a cercare varianti del simbolo
        possible_variants = [
            pair,
            pair.replace('/', ''),
            pair.replace('/', ':'),
            f"{pair}:USDT" if ':USDT' not in pair else pair,
            f"{pair}:PERP" if ':PERP' not in pair else pair,
        ]
        
        for variant in possible_variants:
            if variant in exchange.markets:
                logger.info(f"Trovato pair come: {variant}")
                return variant
        
        raise ValueError(f"Pair '{pair}' non disponibile su {exchange.id}. Mercati disponibili: {list(exchange.markets.keys())[:10]}...")
    return pair

def get_oldest_timestamp(exchange, pair, market_type='spot'):
    """Ottiene il timestamp più vecchio disponibile per il pair."""
    try:
        # Prova a ottenere le candele più vecchie disponibili
        ohlcv = exchange.fetch_ohlcv(pair, TIMEFRAME, since=0, limit=1)
        if ohlcv:
            return ohlcv[0][0]
    except Exception as e:
        logger.warning(f"Impossibile determinare timestamp più vecchio: {e}")
    
    # Default: dal 2020 per crypto
    return 1577836800000  # 2020-01-01

def fetch_ohlcv(exchange, pair, tf, since, limit=1000):
    """Fetch OHLCV con gestione rate limits."""
    try:
        return exchange.fetch_ohlcv(pair, tf, since=since, limit=limit)
    except ccxt.NetworkError as e:
        logger.error(f"Errore di rete durante il fetch: {e}")
        time.sleep(5)
        return []
    except ccxt.ExchangeError as e:
        logger.error(f"Errore exchange durante il fetch: {e}")
        return []
    except Exception as e:
        logger.error(f"Errore imprevisto durante il fetch: {e}")
        return []

def download_ohlcv_data(exchange, pair, filepath, start_timestamp=None, append=False):
    """Scarica dati OHLCV e salva in Parquet."""
    data = []
    limit = 1000
    batch_save_size = 10
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
        with tqdm(desc=f"Download {pair}", unit="candles", bar_format='{l_bar}{bar:20}{r_bar}{bar:-20b}') as pbar:
            while since < latest_timestamp:
                ohlcv = fetch_ohlcv(exchange, pair, TIMEFRAME, since, limit)
                if not ohlcv:
                    logger.info("Nessun dato aggiuntivo disponibile.")
                    break
                
                data.extend(ohlcv)
                since = ohlcv[-1][0] + 1
                pbar.update(len(ohlcv))
                batch_count += 1
                
                time.sleep(exchange.rateLimit / 1000)
                
                # Salva in batch per robustezza
                if batch_count >= batch_save_size:
                    logger.info(f"Salvataggio intermedio di {len(data)} candele...")
                    df_batch = create_ohlcv_dataframe(data)
                    save_parquet(df_batch, filepath, append=append)
                    data = []
                    batch_count = 0
                    append = True
        
        # Salva l'ultimo batch
        if data:
            logger.info(f"Salvataggio finale di {len(data)} candele...")
            df_final = create_ohlcv_dataframe(data)
            save_parquet(df_final, filepath, append=append)
        
        # Carica e restituisci il DataFrame completo
        if os.path.exists(filepath):
            return load_parquet(filepath)
        else:
            logger.warning("File non trovato dopo salvataggio.")
            return None
    
    except Exception as e:
        logger.error(f"Errore durante il download OHLCV: {e}")
        return None

def create_ohlcv_dataframe(data):
    """Crea DataFrame dalle candele OHLCV."""
    if not data:
        return pd.DataFrame()
    
    columns = ['timestamp_ms', 'open', 'high', 'low', 'close', 'volume']
    df = pd.DataFrame(data, columns=columns[:len(data[0])])
    
    if len(data[0]) > 6:
        df['trades_count'] = [row[6] for row in data]
    
    return df

def download_funding_data(exchange, pair, filepath, start_timestamp=None, append=False):
    """Scarica dati di funding rate per perpetual."""
    logger.info(f"Scaricando funding rate per {pair}...")
    
    data = []
    since = start_timestamp if start_timestamp else get_oldest_timestamp(exchange, pair)
    latest_timestamp = get_current_timestamp_ms()
    
    try:
        with tqdm(desc=f"Funding {pair}", unit="records") as pbar:
            while since < latest_timestamp:
                try:
                    funding_data = exchange.fetch_funding_rate_history(pair, since=since, limit=100)
                    if not funding_data:
                        break
                    
                    for entry in funding_data:
                        standardized_entry = {
                            'timestamp_ms': entry['timestamp'],
                            'funding_rate': entry['fundingRate'],
                            'datetime': entry['datetime']
                        }
                        data.append(standardized_entry)
                    
                    since = funding_data[-1]['timestamp'] + 1
                    pbar.update(len(funding_data))
                    time.sleep(exchange.rateLimit / 1000)
                    
                except Exception as e:
                    if 'end of data' in str(e).lower() or 'no data' in str(e).lower():
                        break
                    else:
                        logger.warning(f"Errore funding API: {e}")
                        time.sleep(5)
        
        if data:
            df = pd.DataFrame(data)
            save_parquet(df, filepath, append=append)
            logger.info(f"Funding rate salvati: {len(df)} record")
            return df
            
    except Exception as e:
        logger.error(f"Errore scaricamento funding: {e}")
    
    return None

def download_oi_data(exchange, pair, filepath, start_timestamp=None, append=False):
    """Scarica dati di open interest per perpetual."""
    logger.info(f"Scaricando open interest per {pair}...")
    
    data = []
    since = start_timestamp if start_timestamp else get_oldest_timestamp(exchange, pair)
    latest_timestamp = get_current_timestamp_ms()
    
    try:
        with tqdm(desc=f"Open Interest {pair}", unit="records") as pbar:
            while since < latest_timestamp:
                try:
                    oi_data = exchange.fetch_open_interest_history(pair, '1m', since=since, limit=100)
                    if not oi_data:
                        break
                    
                    for entry in oi_data:
                        standardized_entry = {
                            'timestamp_ms': entry['timestamp'],
                            'open_interest': entry['openInterest'],
                            'datetime': entry['datetime']
                        }
                        data.append(standardized_entry)
                    
                    since = oi_data[-1]['timestamp'] + 1
                    pbar.update(len(oi_data))
                    time.sleep(exchange.rateLimit / 1000)
                    
                except Exception as e:
                    if 'not supported' in str(e).lower():
                        logger.warning(f"Open Interest non supportato per {exchange.id}")
                        break
                    elif 'end of data' in str(e).lower() or 'no data' in str(e).lower():
                        break
                    else:
                        logger.warning(f"Errore OI API: {e}")
                        time.sleep(5)
        
        if data:
            df = pd.DataFrame(data)
            save_parquet(df, filepath, append=append)
            logger.info(f"Open interest salvati: {len(df)} record")
            return df
            
    except Exception as e:
        logger.error(f"Errore scaricamento open interest: {e}")
    
    return None

def download_historical_data(exchange, pair, market_type, start_timestamp=None, append=False):
    """Scarica dati storici organizzati per tipo di mercato."""
    
    # Crea directory organizzate usando path manuali
    candles_dir = f"{DATA_PATH}/{market_type}/candles"
    funding_dir = f"{DATA_PATH}/{market_type}/funding"
    oi_dir = f"{DATA_PATH}/{market_type}/oi"
    
    ensure_directory_exists(candles_dir)
    
    # Filepath per candles
    candles_filename = get_parquet_filename(exchange.id, pair, TIMEFRAME, market_type, 'candles')
    candles_path = f"{candles_dir}/{candles_filename}"
    
    logger.info(f"📊 Scaricando dati {market_type} per {pair}")
    logger.info(f"📁 File candele: {candles_filename}")
    
    # Scarica candele OHLCV
    df_candles = download_ohlcv_data(exchange, pair, candles_path, start_timestamp, append)
    
    # Se perpetual, scarica dati aggiuntivi
    if market_type == 'perpetual':
        if FETCH_FUNDING:
            funding_filename = get_parquet_filename(exchange.id, pair, TIMEFRAME, market_type, 'funding')
            funding_path = f"{funding_dir}/{funding_filename}"
            ensure_directory_exists(funding_dir)
            download_funding_data(exchange, pair, funding_path, start_timestamp, append)
        
        if FETCH_OPEN_INTEREST:
            oi_filename = get_parquet_filename(exchange.id, pair, TIMEFRAME, market_type, 'oi')
            oi_path = f"{oi_dir}/{oi_filename}"
            ensure_directory_exists(oi_dir)
            download_oi_data(exchange, pair, oi_path, start_timestamp, append)
    
    return df_candles

def main():
    global logger
    ensure_directory_exists(DATA_PATH)
    ensure_directory_exists(LOGS_PATH)
    logger = setup_logger(LOGS_PATH)
    
    logger.info("🚀 Avvio MEHD - Multi-Exchanges Historical Data Downloader")
    
    # Input CLI
    exchange_name = input(f"{Fore.CYAN}Exchange ({', '.join(SUPPORTED_EXCHANGES)}; default: {DEFAULT_EXCHANGE}): {Style.RESET_ALL}") or DEFAULT_EXCHANGE
    try:
        exchange = validate_exchange(exchange_name)
    except ValueError as e:
        logger.error(str(e))
        return
    
    pair = input(f"{Fore.CYAN}Pair (es. BTC/USDT; default: {DEFAULT_PAIR}): {Style.RESET_ALL}") or DEFAULT_PAIR
    try:
        pair = validate_pair(exchange, pair)
        logger.info(f"✅ Pair validato: {pair}")
    except ValueError as e:
        logger.error(str(e))
        return
    
    # Rileva automaticamente il tipo di mercato
    market_type = detect_market_type(exchange, pair)
    logger.info(f"📊 Tipo di mercato rilevato: {Fore.YELLOW}{market_type.upper()}{Style.RESET_ALL}")
    
    # Configura download dati aggiuntivi per perpetual
    fetch_funding = FETCH_FUNDING  # ✅ Queste sono variabili importate, non globali
    fetch_oi = FETCH_OPEN_INTEREST  # ✅ Non serve dichiararle global
    
    if market_type == 'perpetual':
        confirm_funding = input(f"{Fore.YELLOW}Scaricare funding rate? (y/n, default: y): {Style.RESET_ALL}").lower()
        if confirm_funding == 'n':
            fetch_funding = False
        
        confirm_oi = input(f"{Fore.YELLOW}Scaricare open interest? (y/n, default: y): {Style.RESET_ALL}").lower()
        if confirm_oi == 'n':
            fetch_oi = False
    
    # Preparazione paths
    candles_dir = f"{DATA_PATH}/{market_type}/candles"
    candles_filename = get_parquet_filename(exchange.id, pair, TIMEFRAME, market_type, 'candles')
    filepath = f"{candles_dir}/{candles_filename}"
    
    append = False
    start_timestamp = None
    default_date_str = "più vecchio disponibile"
    
    if check_file_exists(filepath):
        existing_df = load_parquet(filepath)
        if not existing_df.empty:
            first_date = timestamp_to_datetime(existing_df['timestamp_ms'].min()).strftime('%Y-%m-%d')
            last_date = timestamp_to_datetime(existing_df['timestamp_ms'].max()).strftime('%Y-%m-%d')
            logger.info(f"📁 File esistente: {len(existing_df)} candele da {first_date} a {last_date}")
            
            action = input(f"{Fore.CYAN}File esistente. (o)verwrite, (a)ppend, (i)gnore? [o/a/i]: {Style.RESET_ALL}").lower()
            if action == 'i':
                logger.info("Download ignorato.")
                return
            append = action == 'a'
            if append:
                start_timestamp = existing_df['timestamp_ms'].max() + 1
                default_date_str = f"{last_date} (continua da ultima candela)"
    
    start_date_str = input(f"{Fore.CYAN}📅 Start date (YYYY-MM-DD; default: {default_date_str}): {Style.RESET_ALL}")
    if start_date_str:
        try:
            start_timestamp = parse_date(start_date_str)
            logger.info(f"Data iniziale impostata: {timestamp_to_datetime(start_timestamp)}")
        except ValueError as e:
            logger.error(str(e))
            return
    
    # ⚠️ RIMUOVI QUESTE RIGHE - NON PUOI MODIFICARE VARIABILI IMPORTATE
    # FETCH_FUNDING = fetch_funding
    # FETCH_OPEN_INTEREST = fetch_oi
    
    # Invece, passa le variabili come parametri o usa variabili locali
    df = download_historical_data(exchange, pair, market_type, start_timestamp, append)
    
    if df is not None and not df.empty:
        logger.info(f"🎉 Download completato!")
        logger.info(f"📊 Candele scaricate: {len(df)}")
        logger.info(f"📁 File principale: {candles_filename}")
        
        # Ispezione del file principale candele
        logger.info("\n" + "="*50)
        logger.info("ISPEZIONE FILE CANDLE:")
        logger.info("="*50)
        inspect_parquet(filepath, logger=logger)
        
        # Se perpetual, ispeziona anche file funding e OI se scaricati
        if market_type == 'perpetual':
            if fetch_funding:  # ✅ Usa la variabile locale
                funding_path = f"{DATA_PATH}/{market_type}/funding/{get_parquet_filename(exchange.id, pair, TIMEFRAME, market_type, 'funding')}"
                if check_file_exists(funding_path):
                    logger.info("\n" + "="*50)
                    logger.info("ISPEZIONE FILE FUNDING:")
                    logger.info("="*50)
                    inspect_parquet(funding_path, logger=logger)
            
            if fetch_oi:  # ✅ Usa la variabile locale
                oi_path = f"{DATA_PATH}/{market_type}/oi/{get_parquet_filename(exchange.id, pair, TIMEFRAME, market_type, 'oi')}"
                if check_file_exists(oi_path):
                    logger.info("\n" + "="*50)
                    logger.info("ISPEZIONE FILE OPEN INTEREST:")
                    logger.info("="*50)
                    inspect_parquet(oi_path, logger=logger)
    else:
        logger.warning("Nessun dato scaricato o file vuoto.")

if __name__ == "__main__":
    main()